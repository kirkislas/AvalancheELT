# Secrets
import os

# Time
import time
from datetime import datetime, timezone

# Typing
from typing import Any, Dict, List, Optional, Tuple

# Geocoding
import googlemaps

# United States
import us

# HTTP Requests
import requests

# Database Interaction
from sqlalchemy import MetaData, Table, create_engine
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
        print(f"Google Maps client initialization failed: {gmaps_err}")
        gmaps_client = None

GEOCODE_CACHE: Dict[str, Tuple[Optional[float], Optional[float]]] = {}


# Fetch API JSON helper
def fetch_api_json(url: str, max_retries: int = MAX_RETRIES, timeout: int = REQUEST_TIMEOUT) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
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
                        return response.json(), None
                    except Exception as json_err:
                        return None, f"JSON decode failed: {json_err}. Snippet: {response.text[:200]}"

                if response.status_code in (429, 500, 502, 503, 504):
                    last_err = f"HTTP {response.status_code}. Snippet: {response.text[:200]}"
                else:
                    return None, f"HTTP {response.status_code}. Snippet: {response.text[:200]}"

            except Exception as req_err:
                last_err = f"Request error: {req_err}"

            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    return None, f"Fetch failed after retries. Last error: {last_err}"


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
        print("Cache invalidation skipped: missing endpoint or API key.")
        return

    try:
        headers = {"access_token": api_key}
        response = requests.post(api_endpoint, headers=headers, json={"keys": []}, timeout=15)
        if response.status_code == 200:
            print("Cache invalidated successfully.")
        else:
            print(f"Failed to invalidate cache: HTTP {response.status_code} - {response.text}")
    except Exception as cache_err:
        print(f"Cache invalidation error: {cache_err}")


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

        unknown_tokens = {"unknown", "n/a", "na", "null", "none", "-"}
        if loc_str.lower().strip(" ,") in unknown_tokens:
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
        if refined_leading_token in unknown_tokens:
            return None

        if state:
            return f"{refined_location}, {state}"
        return refined_location
    except Exception:
        return None


# Fatality parsing helper
def parse_fatalities(record: Dict[str, Any]) -> Optional[int]:
    candidate_fields = [
        record.get("fatalities"),
        record.get("killed"),
        record.get("deaths"),
        record.get("number_of_fatalities"),
    ]
    for value in candidate_fields:
        if value is None:
            continue
        try:
            return int(value)
        except (ValueError, TypeError):
            continue

    involvement = record.get("involvement_summary")
    if isinstance(involvement, list):
        total = 0
        found = False
        for entry in involvement:
            if not isinstance(entry, dict):
                continue
            killed = entry.get("killed") or entry.get("fatalities")
            if killed is None:
                continue
            try:
                total += int(killed)
                found = True
            except (ValueError, TypeError):
                continue
        if found:
            return total

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


# Record normalization helper
def normalize_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    raw_source = record.get("id")
    if raw_source is None:
        raw_source = record.get("uuid")
    if raw_source is None:
        return None
    source_id = f"caic:{str(raw_source).strip()}"
    if not source_id or source_id == "caic:":
        return None

    iso_date = safe_iso_date(record)
    season = derive_season(record, iso_date)

    public_detail = record.get("public_report_detail") or {}
    location_value = public_detail.get("location") or record.get("location")
    if isinstance(location_value, dict):
        location_value = location_value.get("name") or location_value.get("description")
    description = (
        public_detail.get("accident_summary")
        or public_detail.get("accident_summary_markdown")
        or public_detail.get("event_description")
        or record.get("description")
    )

    state_value = convert_state_name(record.get("state") or record.get("state_abbr"))
    fatalities = parse_fatalities(record)
    latitude, longitude = extract_coordinates(record)
    refined = refine_location(state_value, location_value)

    return {
        "source_id": source_id,
        "season": season,
        "date": iso_date,
        "state": state_value,
        "location": location_value,
        "description": description,
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
        print(f"Geocoding failed for '{location_text}': {geo_err}")

    GEOCODE_CACHE[cache_key] = (None, None)
    return None, None


# Geocoding enrichment helper
def enrich_with_geocoding(records: List[Dict[str, Any]]) -> None:
    for record in records:
        lat = record.get("latitude")
        lon = record.get("longitude")
        refined = record.get("refined_location")
        needs_geo = (lat is None and lon is None) and isinstance(refined, str) and refined.strip()
        if needs_geo:
            geo_lat, geo_lon = geocode_location(refined)
            if geo_lat is not None and geo_lon is not None:
                record["latitude"] = geo_lat
                record["longitude"] = geo_lon


# Upsert helper
def upsert_records(connection, table: Table, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    stmt = pg_insert(table).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=["source_id"]).returning(table.c.source_id)
    result = connection.execute(stmt)
    inserted = result.fetchall()
    return len(inserted)


# Bronze record selector
def build_bronze_payload(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bronze_columns = ["source_id", "season", "date", "state", "location", "description", "fatalities"]
    return [{col: record.get(col) for col in bronze_columns} for record in records]


# Silver record selector
def build_silver_payload(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    silver_columns = [
        "source_id",
        "season",
        "date",
        "state",
        "location",
        "description",
        "fatalities",
        "latitude",
        "longitude",
        "refined_location",
    ]
    return [{col: record.get(col) for col in silver_columns} for record in records]


# Main workflow
def main() -> None:
    start_time = datetime.utcnow()
    status = "Failure"
    error_message = None
    data_count = 0

    try:
        raw_data, fetch_error = fetch_api_json(API_URL, MAX_RETRIES, REQUEST_TIMEOUT)
        if raw_data is None:
            raise RuntimeError(fetch_error or "API returned no data")
        total_fetched = len(raw_data) if isinstance(raw_data, list) else 0
        print(f"Fetched {total_fetched} records from API.")

        normalized_records: List[Dict[str, Any]] = []
        for record in raw_data:
            if not isinstance(record, dict):
                continue
            normalized = normalize_record(record)
            if normalized:
                normalized_records.append(normalized)
        print(f"Normalized {len(normalized_records)} records.")

        if not normalized_records:
            print("No valid records available for processing.")
            status = "Success"
        else:
            missing_before = sum(
                1 for record in normalized_records if record.get("latitude") is None and record.get("longitude") is None
            )
            enrich_with_geocoding(normalized_records)
            missing_after = sum(
                1 for record in normalized_records if record.get("latitude") is None and record.get("longitude") is None
            )
            print(f"Geocode fill: {missing_before - missing_after} filled, {missing_after} still missing.")

            bronze_payload = build_bronze_payload(normalized_records)
            silver_payload = build_silver_payload(normalized_records)

            with engine.begin() as conn:
                inserted_bronze = upsert_records(conn, accidents_bronze, bronze_payload)
                print(f"Inserted {inserted_bronze} bronze records.")
                inserted_silver = upsert_records(conn, accidents_silver, silver_payload)
                print(f"Inserted {inserted_silver} silver records.")

            data_count = inserted_silver
            status = "Success"

    except Exception as run_err:
        error_message = str(run_err)
        print(f"ELT run failed: {error_message}")

    finally:
        end_time = datetime.utcnow()
        duration = end_time - start_time
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
            print(f"Failed to record log entry: {log_err}")

        if status == "Success":
            invalidate_cache(CACHE_INVALIDATION_ENDPOINT, FAST_API_KEY)


# Entrypoint
if __name__ == "__main__":
    main()
