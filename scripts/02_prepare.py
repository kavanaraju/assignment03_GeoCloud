"""
    Script to transform raw AirNow data files into BigQuery-compatible formats.

    This script reads the raw .dat files downloaded by 01_extract.py and converts
    them into CSV, JSON-L, and Parquet formats suitable for loading into
    BigQuery as external tables.

    Hourly observation data is converted to: CSV, JSON-L, Parquet
    Site location data is converted to: CSV, JSON-L, GeoParquet (with point geometry)

    Usage:
        python scripts/02_prepare.py
"""

import pathlib
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point


DATA_DIR = pathlib.Path(__file__).parent.parent / 'data'

HOURLY_COLUMNS = [
    'valid_date',
    'valid_time',
    'aqsid',
    'site_name',
    'gmt_offset',
    'parameter_name',
    'reporting_units',
    'value',
    'data_source',
]


def read_hourly_data(date_str):
    """Read all 24 hourly .dat files for a date and combine into a DataFrame."""
    date_compact = date_str.replace('-', '')
    raw_dir = DATA_DIR / 'raw' / date_str
    dfs = []
    for hour in range(24):
        hour_str = str(hour).zfill(2)
        filepath = raw_dir / f'HourlyData_{date_compact}{hour_str}.dat'
        if filepath.exists():
            df = pd.read_csv(
                filepath,
                sep='|',
                header=None,
                names=HOURLY_COLUMNS,
                encoding='latin-1',
            )
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True)


def read_site_locations():
    """Read the most recent Monitoring_Site_Locations_V2.dat file."""
    raw_dirs = sorted((DATA_DIR / 'raw').iterdir())
    latest_dir = raw_dirs[-1]
    filepath = latest_dir / 'Monitoring_Site_Locations_V2.dat'
    df = pd.read_csv(
        filepath,
        sep='|',
        encoding='latin-1',
    )
    # Lowercase all column names
    df.columns = [c.strip().lower() for c in df.columns]
    # Deduplicate by AQSID - one row per site
    df = df.drop_duplicates(subset=['aqsid'])
    return df


# --- Hourly observation data ---

def prepare_hourly_csv(date_str):
    output_dir = DATA_DIR / 'prepared' / 'hourly'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_hourly_data(date_str)
    df.to_csv(output_dir / f'{date_str}.csv', index=False)


def prepare_hourly_jsonl(date_str):
    output_dir = DATA_DIR / 'prepared' / 'hourly'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_hourly_data(date_str)
    df.to_json(output_dir / f'{date_str}.jsonl', orient='records', lines=True)


def prepare_hourly_parquet(date_str):
    output_dir = DATA_DIR / 'prepared' / 'hourly'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_hourly_data(date_str)
    df.to_parquet(output_dir / f'{date_str}.parquet', index=False)


# --- Site location data ---

def prepare_site_locations_csv():
    output_dir = DATA_DIR / 'prepared' / 'sites'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_site_locations()
    df.to_csv(output_dir / 'site_locations.csv', index=False)


def prepare_site_locations_jsonl():
    output_dir = DATA_DIR / 'prepared' / 'sites'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_site_locations()
    df.to_json(output_dir / 'site_locations.jsonl', orient='records', lines=True)


def prepare_site_locations_geoparquet():
    output_dir = DATA_DIR / 'prepared' / 'sites'
    output_dir.mkdir(parents=True, exist_ok=True)
    df = read_site_locations()
    # Create point geometry from latitude and longitude
    geometry = [
        Point(lon, lat)
        for lon, lat in zip(df['longitude'], df['latitude'])
    ]
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs='EPSG:4326')
    gdf.to_parquet(output_dir / 'site_locations.geoparquet', index=False)


if __name__ == '__main__':
    import datetime

    # Prepare site locations (only need to do this once)
    print('Preparing site locations...')
    prepare_site_locations_csv()
    prepare_site_locations_jsonl()
    prepare_site_locations_geoparquet()

    # Prepare hourly data for each day in July 2024 (backfill)
    start_date = datetime.date(2024, 7, 1)
    end_date = datetime.date(2024, 7, 31)

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat()
        print(f'Preparing hourly data for {date_str}...')
        prepare_hourly_csv(date_str)
        prepare_hourly_jsonl(date_str)
        prepare_hourly_parquet(date_str)
        current_date += datetime.timedelta(days=1)

    print('Done.')