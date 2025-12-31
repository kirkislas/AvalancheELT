# Secrets
import os

# Time
import time
import math
from datetime import datetime, timezone

# Typing
from typing import Any, Dict, List, Optional, Tuple

# HTML parsing helpers
from html import unescape

# Regex
import re

# Logging
import logging

# Geocoding
import googlemaps

# United States
import us

# HTTP Requests
import requests

# Database Interaction
from sqlalchemy import MetaData, Table, create_engine, select, func
from sqlalchemy.dialects.postgresql import insert as pg_insert


# Config
API_URL = "https://api.avalanche.state.co.us/api/v2/accident_reports.json"
MAX_RETRIES = 5
REQUEST_TIMEOUT = 30
CACHE_INVALIDATION_ENDPOINT = "https://avalanchebackend.onrender.com/api/invalidate-cache/"
ELT_JOB_ID = str(int(datetime.now(timezone.utc).timestamp()))


# Environment Variables
DATABASE_URL = os.environ.get("PROD_DATABASE_URL")
FAST_API_KEY = os.environ.get("PROD_FAST_API_KEY")
GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY")
REBUILD_SILVER_FROM_ALL_BRONZE = (
    os.environ.get("REBUILD_SILVER_FROM_ALL_BRONZE", "false").strip().lower() in {"1", "true", "yes", "y"}
)
MAX_GEOCODE_PER_RUN = int(os.environ.get("MAX_GEOCODE_PER_RUN", "50"))
DRY_RUN = os.environ.get("DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "y"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

if not DATABASE_URL:
    raise RuntimeError("PROD_DATABASE_URL environment variable is required")


# Database Setup
engine = create_engine(DATABASE_URL)
metadata = MetaData()
accidents_bronze = Table("accidents_bronze", metadata, autoload_with=engine)
accidents_silver = Table("accidents_silver", metadata, autoload_with=engine)
log_table = Table("log", metadata, autoload_with=engine)


# Google Maps Setup
gmaps_client = None
if GOOGLE_MAPS_API_KEY:
    try:
        gmaps_client = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    except Exception as gmaps_err:
        logging.warning("Google Maps client initialization failed: %s", gmaps_err)
        gmaps_client = None

GEOCODE_CACHE: Dict[str, Tuple[Optional[float], Optional[float]]] = {}
MOUNTAIN_STATES = {
    "Alaska",
    "California",
    "Colorado",
    "Idaho",
    "Montana",
    "Nevada",
    "New Hampshire",
    "New Mexico",
    "Oregon",
    "Utah",
    "Vermont",
    "Washington",
    "Wyoming",
}
SILVER_REQUIRED_STRING_FIELDS = [
    "source_id",
    "display_date",
    "state",
    "location",
    "summary_description",
]
SILVER_REQUIRED_NUMERIC_FIELDS = ["latitude", "longitude"]
PLACEHOLDER_TOKENS = {"unknown", "n/a", "na", "null", "none", "-"}
MIN_EXTENDED_CHARS = 140
EXTENDED_DESCRIPTION_BLOCKLIST = {"none additional"}
EXTENDED_DESCRIPTION_BLOCKLIST_LOWER = {token.lower() for token in EXTENDED_DESCRIPTION_BLOCKLIST}
SILVER_COLUMNS = [
    "source_id",
    "season",
    "date",
    "display_date",
    "state",
    "location",
    "summary_description",
    "full_description",
    "fatalities",
    "latitude",
    "longitude",
    "refined_location",
]


def parse_page_out_of_range(response: requests.Response) -> Tuple[bool, Optional[str]]:
    try:
        payload = response.json()
    except ValueError:
        return False, None
    message = payload.get("message") or payload.get("error")
    if isinstance(message, str):
        lowered = message.lower()
        if "expected" in lowered and ":page" in lowered and "got" in lowered:
            return True, message
    return False, None


# Fetch API JSON helper
def fetch_api_json(
    url: str,
    max_retries: int = MAX_RETRIES,
    timeout: int = REQUEST_TIMEOUT
) -> Tuple[Optional[List[Dict[str, Any]]], bool, Optional[str]]:
    last_err = None
    backoff = 2

    with requests.Session() as session:
        session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        })

        for attempt in range(1, max_retries + 1):
            try:
                response = session.get(url, timeout=timeout)

                if response.status_code == 200:
                    try:
                        return response.json(), False, None
                    except Exception as json_err:
                        return None, False, f"JSON decode failed: {json_err}. Snippet: {response.text[:200]}"

                if response.status_code == 400:
                    stop, message = parse_page_out_of_range(response)
                    if stop:
                        return None, True, message
                    last_err = f"HTTP 400. Body: {response.text[:200]}"
                    break

                if response.status_code in (429, 500, 502, 503, 504):
                    last_err = f"HTTP {response.status_code}. Snippet: {response.text[:200]}"
                else:
                    return None, False, f"HTTP {response.status_code}. Snippet: {response.text[:200]}"

            except Exception as req_err:
                last_err = f"Request error: {req_err}"

            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    return None, False, f"Fetch failed after retries. Last error: {last_err}"


# Logging helper
def insert_log(
    elt_job_id: str,
    start_time: datetime,
    end_time: datetime,
    duration,
    status: str,
    data_count: int,
    error_message: Optional[str] = None,
) -> None:
    log_entry = {
        "elt_job_id": elt_job_id,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "status": status,
        "data_count": data_count,
        "error_message": error_message,
    }

    with engine.begin() as conn:
        conn.execute(log_table.insert().values(**log_entry))


# Cache invalidation helper
def invalidate_cache(api_endpoint: str, api_key: Optional[str]) -> None:
    if not api_endpoint or not api_key:
        logging.info("Cache invalidation skipped: missing endpoint or API key.")
        return

    try:
        headers = {"access_token": api_key}
        response = requests.post(api_endpoint, headers=headers, json={"keys": []}, timeout=15)
        if response.status_code == 200:
            logging.info("Cache invalidated successfully.")
        else:
            logging.warning("Failed to invalidate cache: HTTP %s - %s", response.status_code, response.text)
    except Exception as cache_err:
        logging.error("Cache invalidation error: %s", cache_err)


# State conversion helper
def convert_state_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return value
    lookup = us.states.lookup(value)
    return lookup.name if lookup else value


# Location refinement helper
def refine_location(state, location):
    try:
        if location is None:
            return None

        loc_str = str(location).strip()
        if not loc_str:
            return None

        if loc_str.lower().strip(" ,") in PLACEHOLDER_TOKENS:
            return None

        if "," in loc_str:
            last_comma_part = loc_str.split(",")[-1].strip()
            if " of " in last_comma_part:
                refined_location = last_comma_part.split(" of ", 1)[1]
            else:
                refined_location = last_comma_part
        elif " of " in loc_str:
            refined_location = loc_str.split(" of ", 1)[1]
        else:
            refined_location = loc_str

        refined_location = refined_location.strip()
        if not refined_location:
            return None

        refined_lower = refined_location.lower()
        refined_leading_token = refined_lower.split(",")[0].strip()
        if refined_leading_token in PLACEHOLDER_TOKENS:
            return None

        if state:
            return f"{refined_location}, {state}"
        return refined_location
    except Exception:
        logging.exception("Refine location failed for state=%s location=%s", state, location)
        return None


def strip_html(text: Optional[str]) -> Optional[str]:
    if not isinstance(text, str):
        return None
    working = re.sub(r"(?i)<br\s*/?>", "\n", text)
    working = re.sub(r"(?i)</p>\s*<p>", "\n\n", working)
    working = re.sub(r"(?i)</?p>", "\n\n", working)
    working = re.sub(r"(?i)</?span[^>]*>", "", working)
    working = re.sub(
        r"(?is)<a[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        r"\2 (\1)",
        working,
    )
    working = re.sub(r"<[^>]+>", " ", working)
    working = unescape(working)
    working = working.replace("\r", "")
    working = re.sub(r"[ \t]+", " ", working)
    working = re.sub(r"\n{3,}", "\n\n", working)
    lines = [line.strip() for line in working.split("\n")]
    working = "\n".join(lines)
    working = re.sub(r"\n{3,}", "\n\n", working).strip()
    return working or None


def clean_string(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    return cleaned if cleaned else None


def is_placeholder_string(value: Any) -> bool:
    cleaned = clean_string(value)
    if not cleaned:
        return False
    return cleaned.lower() in PLACEHOLDER_TOKENS


# Fatality parsing helper
def parse_fatalities(record: Dict[str, Any]) -> Optional[int]:
    def to_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None

    involvement = record.get("involvement_summary")
    if isinstance(involvement, dict) and "killed" in involvement:
        killed_value = involvement.get("killed")
        if killed_value is not None:
            parsed = to_int(killed_value)
            if parsed is not None:
                return parsed

    public_detail = record.get("public_report_detail") or {}
    detail_summary = public_detail.get("involvement_summary")
    if isinstance(detail_summary, str):
        match = re.search(r"(?<!\d)(\d+)\s*killed", detail_summary, re.IGNORECASE)
        if match:
            parsed = to_int(match.group(1))
            if parsed is not None:
                return parsed

    for field in ("fatalities", "killed", "deaths", "number_of_fatalities"):
        parsed = to_int(record.get(field))
        if parsed is not None:
            return parsed

    return None




# ISO date extraction helper
def safe_iso_date(record: Dict[str, Any]) -> Optional[str]:
    observed_at = record.get("observed_at") or record.get("observation_date")
    if isinstance(observed_at, str):
        cleaned = observed_at.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(cleaned).date().isoformat()
        except ValueError:
            pass

    date_text = record.get("date")
    season = record.get("season")
    if isinstance(date_text, str) and "/" in date_text and season:
        try:
            month, day = map(int, date_text.replace("†", "").split("/"))
            start_year, end_year = season.split("-")
            start_year = int(start_year)
            end_year = int("20" + end_year[-2:]) if len(end_year) == 2 else int(end_year)
            year = start_year if month >= 7 else end_year
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except Exception:
            return None

    if isinstance(date_text, str) and len(date_text) == 10 and date_text[4] == "-":
        return date_text

    return None


# Season derivation helper
def derive_season(record: Dict[str, Any], iso_date: Optional[str]) -> Optional[str]:
    season = record.get("season")
    if season:
        return season

    if not iso_date:
        return None

    try:
        date_obj = datetime.strptime(iso_date, "%Y-%m-%d")
        if date_obj.month >= 7:
            return f"{date_obj.year}-{str(date_obj.year + 1)[-2:]}"
        return f"{date_obj.year - 1}-{str(date_obj.year)[-2:]}"
    except ValueError:
        return None


def format_display_date(iso_date: Optional[str]) -> Optional[str]:
    if not iso_date:
        return None
    try:
        date_obj = datetime.strptime(iso_date, "%Y-%m-%d")
        return date_obj.strftime("%m-%d-%Y")
    except ValueError:
        return None


# Coordinate parsing helper
def extract_coordinates(record: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    def pull_from_obj(obj: Any) -> Tuple[Optional[float], Optional[float]]:
        if not isinstance(obj, dict):
            return None, None
        lat = obj.get("latitude") or obj.get("lat")
        lon = obj.get("longitude") or obj.get("lon") or obj.get("lng")
        lat_val = None
        lon_val = None
        try:
            if lat is not None:
                lat_val = float(lat)
            if lon is not None:
                lon_val = float(lon)
        except (ValueError, TypeError):
            return None, None
        if lat_val is not None and lon_val is not None:
            return lat_val, lon_val
        return None, None

    latitude, longitude = pull_from_obj(record)
    if latitude is not None and longitude is not None:
        return latitude, longitude

    detail = record.get("public_report_detail")
    latitude, longitude = pull_from_obj(detail)
    if latitude is not None and longitude is not None:
        return latitude, longitude

    observation_locations = record.get("observations") or record.get("observation")
    if isinstance(observation_locations, list):
        for entry in observation_locations:
            latitude, longitude = pull_from_obj(entry)
            if latitude is not None and longitude is not None:
                return latitude, longitude

    location_obj = record.get("location")
    if isinstance(location_obj, dict):
        latitude, longitude = pull_from_obj(location_obj)
        if latitude is not None and longitude is not None:
            return latitude, longitude

    return None, None


# Source identifier helper
def extract_source_id(record: Dict[str, Any]) -> Optional[str]:
    raw_source = record.get("id") or record.get("uuid")
    if raw_source is None:
        return None
    source_id = f"caic:{str(raw_source).strip()}"
    if not source_id or source_id == "caic:":
        return None
    return source_id


# Record normalization helper
# Silver v2 represents display-ready rows that meet a strict contract (no defensive filtering needed downstream).
# summary_description is mandatory because the frontend requires a human-readable summary without synthesizing counts.
# Invalid rows are excluded rather than patched to preserve source truth and avoid masking upstream data issues.
# Frontend should not re-filter because the contract is enforced here to keep the UI thin and predictable.
def normalize_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source_id = extract_source_id(record)
    if not source_id:
        return None

    iso_date = safe_iso_date(record)
    season = derive_season(record, iso_date)
    display_date = format_display_date(iso_date)

    public_detail = record.get("public_report_detail") or {}
    location_value = public_detail.get("location") or record.get("location")
    if isinstance(location_value, dict):
        location_value = location_value.get("name") or location_value.get("description")
    summary_description = None
    detail_summary = public_detail.get("involvement_summary")
    if isinstance(detail_summary, str):
        summary_description = clean_string(strip_html(detail_summary))
    if not summary_description:
        involvement_summary = record.get("involvement_summary")
        if isinstance(involvement_summary, str):
            summary_description = clean_string(strip_html(involvement_summary))
    if is_placeholder_string(summary_description):
        summary_description = None

    full_text_source = (
        public_detail.get("accident_summary")
        or public_detail.get("accident_summary_markdown")
        or public_detail.get("event_description")
    )
    full_description = strip_html(full_text_source)
    if not full_description:
        full_description = strip_html(record.get("description"))
    if isinstance(full_description, str):
        normalized_full = full_description.strip()
        if normalized_full.lower() in EXTENDED_DESCRIPTION_BLOCKLIST_LOWER:
            full_description = None
        elif len(normalized_full) < MIN_EXTENDED_CHARS:
            full_description = None
        else:
            full_description = normalized_full

    state_value = convert_state_name(record.get("state") or record.get("state_abbr"))
    fatalities = parse_fatalities(record)
    latitude, longitude = extract_coordinates(record)
    refined = refine_location(state_value, location_value)
    display_location = refined if isinstance(refined, str) and refined.strip() else location_value
    display_location = clean_string(display_location)
    if is_placeholder_string(display_location):
        display_location = None

    return {
        "source_id": source_id,
        "season": season,
        "date": iso_date,
        "display_date": display_date,
        "state": state_value,
        "location": display_location,
        "summary_description": summary_description,
        "full_description": full_description,
        "fatalities": fatalities,
        "latitude": latitude,
        "longitude": longitude,
        "refined_location": refined,
    }


# Geocoding helper
def geocode_location(location_text: Optional[str]) -> Tuple[Optional[float], Optional[float]]:
    if not location_text or not gmaps_client:
        return None, None

    cache_key = location_text.strip().lower()
    if cache_key in GEOCODE_CACHE:
        return GEOCODE_CACHE[cache_key]

    try:
        results = gmaps_client.geocode(location_text)
        if results:
            geometry = results[0].get("geometry", {})
            loc = geometry.get("location", {})
            lat = loc.get("lat")
            lng = loc.get("lng")
            if lat is not None and lng is not None:
                GEOCODE_CACHE[cache_key] = (lat, lng)
                time.sleep(0.2)
                return lat, lng
    except Exception as geo_err:
        logging.warning("Geocoding failed for '%s': %s", location_text, geo_err)

    GEOCODE_CACHE[cache_key] = (None, None)
    return None, None


# Geocoding enrichment helper
def enrich_with_geocoding(records: List[Dict[str, Any]], max_attempts: int = MAX_GEOCODE_PER_RUN) -> Dict[str, int]:
    stats = {"attempted": 0, "succeeded": 0, "skipped_cap": 0}
    if not gmaps_client or max_attempts <= 0:
        return stats

    for record in records:
        lat = record.get("latitude")
        lon = record.get("longitude")
        refined = record.get("refined_location")
        needs_geo = (lat is None and lon is None) and isinstance(refined, str) and refined.strip()
        if needs_geo:
            if stats["attempted"] >= max_attempts:
                stats["skipped_cap"] += 1
                continue
            geo_lat, geo_lon = geocode_location(refined)
            stats["attempted"] += 1
            if geo_lat is not None and geo_lon is not None:
                record["latitude"] = geo_lat
                record["longitude"] = geo_lon
                stats["succeeded"] += 1
    return stats

def filter_silver_records(records: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    skip_stats: Dict[str, Dict[str, Any]] = {
        "missing_required_fields": {"count": 0, "samples": []},
        "placeholder_required_fields": {"count": 0, "samples": []},
        "missing_summary_description": {"count": 0, "samples": []},
        "placeholder_summary_description": {"count": 0, "samples": []},
        "invalid_coordinates": {"count": 0, "samples": []},
        "state_not_mountain": {"count": 0, "samples": []},
    }
    filtered: List[Dict[str, Any]] = []

    def record_skip(reason: str, record: Dict[str, Any]) -> None:
        info = skip_stats[reason]
        info["count"] += 1
        if len(info["samples"]) < 5:
            info["samples"].append(record.get("source_id"))

    for record in records:
        summary_value = record.get("summary_description")
        summary_clean = clean_string(summary_value)
        if summary_clean is None:
            record_skip("missing_summary_description", record)
            continue
        if is_placeholder_string(summary_clean):
            record_skip("placeholder_summary_description", record)
            continue

        missing_fields: List[str] = []
        placeholder_fields: List[str] = []
        for field in (field for field in SILVER_REQUIRED_STRING_FIELDS if field != "summary_description"):
            value = record.get(field)
            if clean_string(value) is None:
                missing_fields.append(field)
                continue
            if is_placeholder_string(value):
                placeholder_fields.append(field)
        if missing_fields:
            record_skip("missing_required_fields", record)
            continue
        if placeholder_fields:
            record_skip("placeholder_required_fields", record)
            continue

        numeric_values = {
            field: record.get(field) for field in SILVER_REQUIRED_NUMERIC_FIELDS
        }
        invalid_coordinates = False
        for field in SILVER_REQUIRED_NUMERIC_FIELDS:
            value = numeric_values.get(field)
            if isinstance(value, bool):
                invalid_coordinates = True
                break
            if value is None or not isinstance(value, (int, float)):
                invalid_coordinates = True
                break
            if not math.isfinite(value):
                invalid_coordinates = True
                break
            if field == "latitude" and not (-90 <= value <= 90):
                invalid_coordinates = True
                break
            if field == "longitude" and not (-180 <= value <= 180):
                invalid_coordinates = True
                break
        if invalid_coordinates:
            record_skip("invalid_coordinates", record)
            continue

        state = record.get("state")
        state_clean = state.strip() if isinstance(state, str) else None
        if not state_clean or state_clean not in MOUNTAIN_STATES:
            record_skip("state_not_mountain", record)
            continue

        filtered.append(record)

    return filtered, skip_stats


# Upsert helper
def upsert_records(
    connection,
    table: Table,
    rows: List[Dict[str, Any]],
    returning_columns: Optional[List[Any]] = None,
) -> Tuple[int, List[Dict[str, Any]]]:
    if not rows:
        return 0, []
    stmt = pg_insert(table).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=["source_id"])
    columns = returning_columns or [table.c.source_id]
    stmt = stmt.returning(*columns)
    result = connection.execute(stmt)
    inserted = [dict(row._mapping) for row in result.fetchall()]
    return len(inserted), inserted


def upsert_silver_records(connection, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    insert_stmt = pg_insert(accidents_silver).values(rows)
    excluded = insert_stmt.excluded
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[accidents_silver.c.source_id],
        set_={
            "season": func.coalesce(excluded.season, accidents_silver.c.season),
            "date": func.coalesce(excluded.date, accidents_silver.c.date),
            "display_date": func.coalesce(excluded.display_date, accidents_silver.c.display_date),
            "state": func.coalesce(excluded.state, accidents_silver.c.state),
            "location": func.coalesce(excluded.location, accidents_silver.c.location),
            "summary_description": func.coalesce(
                excluded.summary_description,
                accidents_silver.c.summary_description,
            ),
            "full_description": func.coalesce(
                excluded.full_description,
                accidents_silver.c.full_description,
            ),
            "fatalities": func.coalesce(excluded.fatalities, accidents_silver.c.fatalities),
            "latitude": func.coalesce(excluded.latitude, accidents_silver.c.latitude),
            "longitude": func.coalesce(excluded.longitude, accidents_silver.c.longitude),
            "refined_location": func.coalesce(excluded.refined_location, accidents_silver.c.refined_location),
        },
    ).returning(accidents_silver.c.source_id)

    result = connection.execute(update_stmt)
    affected_rows = result.fetchall()
    return len(affected_rows)


# Bronze record builder
def build_bronze_payload_raw(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    payload: List[Dict[str, Any]] = []
    fetched_at = datetime.now(timezone.utc)
    for record in records:
        source_id = extract_source_id(record)
        if not source_id:
            continue
        payload.append(
            {
                "source_id": source_id,
                "raw_json": record,
                "fetched_at": fetched_at,
                "elt_job_id": ELT_JOB_ID,
            }
        )
    return payload


def fetch_bronze_raw_records(
    connection,
    elt_job_id: str,
    rebuild_from_all: bool,
) -> List[Dict[str, Any]]:
    query = select(accidents_bronze.c.raw_json).order_by(accidents_bronze.c.id)
    if not rebuild_from_all:
        query = query.where(accidents_bronze.c.elt_job_id == elt_job_id)
    result = connection.execute(query)
    return [row._mapping["raw_json"] for row in result.fetchall()]


# Silver record selector
def build_silver_payload(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{col: record.get(col) for col in SILVER_COLUMNS} for record in records]


def assert_silver_contract(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int, List[Optional[str]]]:
    violations = 0
    samples: List[Optional[str]] = []
    for row in rows:
        invalid_fields: List[str] = []
        for field in SILVER_REQUIRED_STRING_FIELDS:
            value = row.get(field)
            if clean_string(value) is None:
                invalid_fields.append(field)
                continue
            if is_placeholder_string(value):
                invalid_fields.append(field)
        for field in SILVER_REQUIRED_NUMERIC_FIELDS:
            value = row.get(field)
            if isinstance(value, bool):
                invalid_fields.append(field)
                continue
            if value is None or not isinstance(value, (int, float)):
                invalid_fields.append(field)
                continue
            if not math.isfinite(value):
                invalid_fields.append(field)
                continue
            if field == "latitude" and not (-90 <= value <= 90):
                invalid_fields.append(field)
                continue
            if field == "longitude" and not (-180 <= value <= 180):
                invalid_fields.append(field)
        if invalid_fields:
            violations += 1
            if len(samples) < 5:
                samples.append(row.get("source_id"))
            logging.warning(
                "Silver contract assertion for source_id=%s; invalid fields: %s",
                row.get("source_id"),
                invalid_fields,
            )
    return rows, violations, samples


# Main workflow
def main() -> None:
    start_time = datetime.now(timezone.utc)
    status = "Failure"
    error_message = None
    data_count = 0

    logging.info(
        "Run config: elt_job_id=%s api_url=%s dry_run=%s rebuild_all_bronze=%s max_geocode_per_run=%s "
        "request_timeout=%s max_retries=%s",
        ELT_JOB_ID,
        API_URL,
        DRY_RUN,
        REBUILD_SILVER_FROM_ALL_BRONZE,
        MAX_GEOCODE_PER_RUN,
        REQUEST_TIMEOUT,
        MAX_RETRIES,
    )
    logging.info(
        "Silver rebuild mode: %s",
        "all bronze rows" if REBUILD_SILVER_FROM_ALL_BRONZE else "current elt_job_id only",
    )
    # TODO: If any v2 columns are missing from accidents_silver, apply a migration outside this ELT job.
    silver_table_columns = set(accidents_silver.c.keys())
    missing_silver_columns = [col for col in SILVER_COLUMNS if col not in silver_table_columns]
    if missing_silver_columns:
        logging.warning(
            "accidents_silver missing v2 columns: %s",
            ", ".join(missing_silver_columns),
        )

    try:
        # -------------------------
        # PAGINATED FETCH
        # -------------------------
        page = 1
        all_records: List[Dict[str, Any]] = []

        while True:
            page_url = f"{API_URL}?page={page}"
            raw_data, stop_pagination, fetch_error = fetch_api_json(page_url, MAX_RETRIES, REQUEST_TIMEOUT)

            if stop_pagination:
                logging.info("Pagination complete. API rejected page %s: %s", page, fetch_error or "page out of range")
                break

            if raw_data is None:
                raise RuntimeError(fetch_error or f"API returned no data on page {page}")

            if not isinstance(raw_data, list) or not raw_data:
                logging.info("No records returned on page %s. Pagination complete.", page)
                break

            all_records.extend(raw_data)
            logging.info("Fetched page %s: %s records (running total: %s)", page, len(raw_data), len(all_records))

            page += 1

        if not all_records:
            raise RuntimeError("No records fetched from API.")

        # -------------------------
        # BRONZE LOAD
        # -------------------------
        bronze_payload = build_bronze_payload_raw(all_records)

        if not bronze_payload:
            logging.info("No records with valid source IDs available for bronze load.")
            status = "Success"
            return

        if DRY_RUN:
            logging.info("DRY_RUN enabled: skipping bronze insert and DB read; using in-memory payload.")
            inserted_bronze = 0
            bronze_raw_records = [row["raw_json"] for row in bronze_payload]
        else:
            with engine.begin() as conn:
                inserted_bronze, _ = upsert_records(
                    conn,
                    accidents_bronze,
                    bronze_payload,
                    returning_columns=[accidents_bronze.c.source_id],
                )
            logging.info("Inserted %s bronze records.", inserted_bronze)
            if inserted_bronze == 0:
                if REBUILD_SILVER_FROM_ALL_BRONZE:
                    logging.info("No new bronze rows inserted; REBUILD_SILVER_FROM_ALL_BRONZE=True so using all bronze rows.")
                elif not DRY_RUN:
                    logging.info("No new bronze rows inserted; early exit because rebuild-from-all and DRY_RUN are disabled.")
                    status = "Success"
                    data_count = 0
                    return
                else:
                    logging.info("No new bronze rows inserted; limiting silver rebuild to this job's bronze rows.")

            with engine.begin() as conn:
                bronze_raw_records = fetch_bronze_raw_records(conn, ELT_JOB_ID, REBUILD_SILVER_FROM_ALL_BRONZE)

        if not bronze_raw_records:
            if DRY_RUN:
                logging.info("No bronze rows available in DRY_RUN payload; skipping silver load.")
            elif REBUILD_SILVER_FROM_ALL_BRONZE:
                logging.info("Bronze table is empty; skipping silver load.")
            else:
                logging.info(
                    "No bronze rows found for this job; skipping silver load. "
                    "Set REBUILD_SILVER_FROM_ALL_BRONZE=true to rebuild from all bronze."
                )
            status = "Success"
            return

        # -------------------------
        # NORMALIZATION FROM BRONZE
        # -------------------------
        normalized_records: List[Dict[str, Any]] = []

        for raw_record in bronze_raw_records:
            if not isinstance(raw_record, dict):
                continue
            normalized = normalize_record(raw_record)
            if normalized:
                normalized_records.append(normalized)

        normalized_samples = [record.get("source_id") for record in normalized_records[:5]]
        normalized_sample_text = ", ".join(filter(None, normalized_samples))
        logging.info(
            "Normalized rows: %s%s",
            len(normalized_records),
            f" [samples: {normalized_sample_text}]" if normalized_sample_text else "",
        )

        missing_summary_sources: List[Optional[str]] = []
        missing_summary_count = 0
        for record in normalized_records:
            summary_value = record.get("summary_description")
            summary_text = summary_value.strip() if isinstance(summary_value, str) else ""
            if not summary_text:
                missing_summary_count += 1
                if len(missing_summary_sources) < 5:
                    missing_summary_sources.append(record.get("source_id"))
        missing_summary_sample = ", ".join(filter(None, missing_summary_sources))
        logging.info(
            "Missing summary_description: %s%s",
            missing_summary_count,
            f" [samples: {missing_summary_sample}]" if missing_summary_sample else "",
        )

        non_null_fatalities = sum(1 for r in normalized_records if r.get("fatalities") is not None)
        logging.info("Fatalities non-null after normalize: %s/%s", non_null_fatalities, len(normalized_records))
        zero_fatalities = sum(1 for r in normalized_records if r.get("fatalities") == 0)
        positive_fatalities = sum(
            1 for r in normalized_records if isinstance(r.get("fatalities"), int) and r.get("fatalities") > 0
        )
        null_fatalities = len(normalized_records) - zero_fatalities - positive_fatalities
        logging.info(
            "Fatalities breakdown: zero=%s, gt0=%s, null=%s",
            zero_fatalities,
            positive_fatalities,
            null_fatalities,
        )
        summary_non_null = sum(1 for r in normalized_records if r.get("summary_description"))
        full_non_null = sum(1 for r in normalized_records if r.get("full_description"))
        logging.info("summary_description non-null: %s/%s", summary_non_null, len(normalized_records))
        logging.info("full_description non-null: %s/%s", full_non_null, len(normalized_records))

        if not normalized_records:
            logging.info("No valid records available after normalization.")
            status = "Success"
            return

        # -------------------------
        # GEOCODING ENRICHMENT
        # -------------------------
        missing_before = sum(
            1 for r in normalized_records
            if r.get("latitude") is None and r.get("longitude") is None
        )

        geocode_stats = enrich_with_geocoding(normalized_records, MAX_GEOCODE_PER_RUN)

        missing_after = sum(
            1 for r in normalized_records
            if r.get("latitude") is None and r.get("longitude") is None
        )

        logging.info("Geocode fill: %s filled, %s still missing.", missing_before - missing_after, missing_after)
        logging.info(
            "Geocode stats: attempted=%s, succeeded=%s, skipped_cap=%s (max %s).",
            geocode_stats["attempted"],
            geocode_stats["succeeded"],
            geocode_stats["skipped_cap"],
            MAX_GEOCODE_PER_RUN,
        )

        # -------------------------
        # UPSERT (APPEND-ONLY)
        # -------------------------
        silver_candidates, skip_stats = filter_silver_records(normalized_records)
        skipped_total = sum(stat["count"] for stat in skip_stats.values())
        candidate_samples = [record.get("source_id") for record in silver_candidates[:5]]
        candidate_sample_text = ", ".join(filter(None, candidate_samples))
        logging.info(
            "Silver candidates: %s%s",
            len(silver_candidates),
            f" [samples: {candidate_sample_text}]" if candidate_sample_text else "",
        )
        logging.info("Silver quality filter: total=%s, kept=%s, skipped=%s", len(normalized_records), len(silver_candidates), skipped_total)
        for reason, stat in skip_stats.items():
            samples = ", ".join(filter(None, stat["samples"]))
            sample_text = f" [samples: {samples}]" if samples else ""
            logging.info("Silver quality filter detail: %s=%s%s", reason, stat["count"], sample_text)
        invalid_samples = ", ".join(filter(None, skip_stats["invalid_coordinates"]["samples"]))
        logging.info(
            "Invalid coordinate rejections: %s%s",
            skip_stats["invalid_coordinates"]["count"],
            f" [samples: {invalid_samples}]" if invalid_samples else "",
        )

        raw_silver_payload = build_silver_payload(silver_candidates)
        silver_payload, contract_assertion_count, contract_samples = assert_silver_contract(raw_silver_payload)
        if contract_assertion_count:
            sample_text = ", ".join(filter(None, contract_samples))
            logging.warning(
                "Silver contract assertion flagged %s rows (log-only; filter enforces).%s",
                contract_assertion_count,
                f" [samples: {sample_text}]" if sample_text else "",
            )
        dropped_by_filters = len(normalized_records) - len(silver_candidates)
        logging.info(
            "Silver candidate metrics: total_normalized=%s candidates=%s dropped_by_state=%s dropped_by_filters=%s",
            len(normalized_records),
            len(silver_candidates),
            skip_stats["state_not_mountain"]["count"],
            dropped_by_filters,
        )
        logging.info("Silver contract assertion count (log-only)=%s", contract_assertion_count)
        silver_columns = [
            "source_id",
            "season",
            "date",
            "display_date",
            "state",
            "location",
            "summary_description",
            "full_description",
            "fatalities",
            "latitude",
            "longitude",
            "refined_location",
        ]
        null_counts = {
            column: sum(1 for row in silver_payload if row.get(column) is None)
            for column in silver_columns
        }
        logging.info(
            "Silver payload: total=%s nulls=%s",
            len(silver_payload),
            null_counts,
        )
        sample_records = silver_payload[:3]
        for record in sample_records:
            summary_value = record.get("summary_description")
            full_value = record.get("full_description")
            logging.info(
                "Silver sample: source_id=%s location=%s state=%s display_date=%s summary_len=%s full_len=%s",
                record.get("source_id"),
                record.get("location"),
                record.get("state"),
                record.get("display_date"),
                len(summary_value) if isinstance(summary_value, str) else 0,
                len(full_value) if isinstance(full_value, str) else 0,
            )

        if DRY_RUN:
            sample_sources = [record.get("source_id") for record in silver_payload[:5]]
            logging.info("DRY_RUN sample silver candidate source_ids (max 5): %s", sample_sources)

        if not silver_payload:
            silver_upserted = 0
            logging.info("No records passed silver quality filters; skipping silver upsert.")
        elif DRY_RUN:
            silver_upserted = 0
            logging.info("DRY_RUN enabled: skipping silver upsert for %s records.", len(silver_payload))
        else:
            with engine.begin() as conn:
                silver_upserted = upsert_silver_records(conn, silver_payload)
                logging.info("Silver rows touched: %s", silver_upserted)

        data_count = silver_upserted
        status = "Success"

    except Exception as run_err:
        error_message = str(run_err)
        logging.error("ELT run failed: %s", error_message)

    finally:
        end_time = datetime.now(timezone.utc)
        duration = end_time - start_time

        if DRY_RUN:
            logging.info(
                "DRY_RUN enabled: skipping log insert. status=%s, data_count=%s, error_message=%s",
                status,
                data_count,
                error_message,
            )
        else:
            try:
                insert_log(
                    elt_job_id=ELT_JOB_ID,
                    start_time=start_time,
                    end_time=end_time,
                    duration=duration,
                    status=status,
                    data_count=data_count,
                    error_message=error_message if status != "Success" else None,
                )
            except Exception as log_err:
                logging.error("Failed to record log entry: %s", log_err)

        if status == "Success":
            if DRY_RUN:
                logging.info("DRY_RUN enabled: skipping cache invalidation.")
            else:
                invalidate_cache(CACHE_INVALIDATION_ENDPOINT, FAST_API_KEY)




# Entrypoint
if __name__ == "__main__":
    main()
