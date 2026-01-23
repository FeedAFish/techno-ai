import requests
import os
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime
import argparse
import time
import numpy as np

def download_from_google_drive(drive_link, output_path):
    # Extract file ID from sharing link
    if '/d/' in drive_link:
        file_id = drive_link.split('/d/')[1].split('/')[0]
    else:
        file_id = drive_link
    
    # Construct direct download URL
    direct_url = f"https://drive.google.com/uc?id={file_id}&export=download"
    
    print(f"Downloading file from Google Drive...")
    print(f"File ID: {file_id}")
    
    try:
        if os.path.exists(output_path):
            print(f"✗ File already exists at: {output_path}")
            return False
        
        # Use session to handle redirects
        session = requests.Session()
        response = session.get(direct_url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Get total file size
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f:
            downloaded = 0
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total_size:
                        progress = (downloaded / total_size) * 100
                        print(f"Progress: {progress:.1f}%", end='\r')
        
        print(f"\n✓ File successfully downloaded to: {output_path}")
        return True
    except Exception as e:
        print(f"✗ Failed to download file: {e}")
        return False

def run_weather_download():
    """Run the full weather data download pipeline"""
    if not os.path.exists("./.data"):
        os.makedirs("./.data")

    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/quartier_paris/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
    response = requests.get(url)
    if not os.path.exists("./.data/quartier_paris.csv"):
        with open("./.data/quartier_paris.csv", "wb") as f:
            f.write(response.content)
    else: 
        print("quartier_paris.csv already exists. Skipping download.")

    df = pd.read_csv("./.data/quartier_paris.csv", sep=';')
    
    # Extract x, y coordinates from 'Geometry X Y' column
    df[['x', 'y']] = df['Geometry X Y'].str.split(',', expand=True).astype(float)

    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    def download_weather_data(name, x, y):
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": x,
            "longitude": y,
            "start_date": "2021-01-01",
            "end_date": "2024-12-31",
            "hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "rain", "snowfall", "precipitation", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "soil_temperature_0_to_7cm", "soil_moisture_0_to_7cm", "cloud_cover", "et0_fao_evapotranspiration", "vapour_pressure_deficit", "apparent_temperature", "is_day"],
        }
        responses = openmeteo.weather_api(url, params=params)

        response = responses[0]
        print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
        print(f"Elevation: {response.Elevation()} m asl")
        print(f"Timezone difference to GMT+0: {response.UtcOffsetSeconds()}s")

        hourly = response.Hourly()
        hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
        hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
        hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
        hourly_rain = hourly.Variables(3).ValuesAsNumpy()
        hourly_snowfall = hourly.Variables(4).ValuesAsNumpy()
        hourly_precipitation = hourly.Variables(5).ValuesAsNumpy()
        hourly_wind_speed_10m = hourly.Variables(6).ValuesAsNumpy()
        hourly_wind_direction_10m = hourly.Variables(7).ValuesAsNumpy()
        hourly_wind_gusts_10m = hourly.Variables(8).ValuesAsNumpy()
        hourly_soil_temperature_0_to_7cm = hourly.Variables(9).ValuesAsNumpy()
        hourly_soil_moisture_0_to_7cm = hourly.Variables(10).ValuesAsNumpy()
        hourly_cloud_cover = hourly.Variables(11).ValuesAsNumpy()
        hourly_et0_fao_evapotranspiration = hourly.Variables(12).ValuesAsNumpy()
        hourly_vapour_pressure_deficit = hourly.Variables(13).ValuesAsNumpy()
        hourly_apparent_temperature = hourly.Variables(14).ValuesAsNumpy()
        hourly_is_day = hourly.Variables(15).ValuesAsNumpy()

        start_time = datetime.fromtimestamp(hourly.Time(), tz=None)
        interval_seconds = hourly.Interval()
        num_steps = len(hourly_temperature_2m)
        dates = np.arange(
            np.datetime64(start_time),
            np.datetime64(start_time) + np.timedelta64(interval_seconds * num_steps, 's'),
            np.timedelta64(interval_seconds, 's')
        )

        hourly_dataframe = pd.DataFrame({
            "date": dates,
            "temperature_2m": hourly_temperature_2m,
            "relative_humidity_2m": hourly_relative_humidity_2m,
            "dew_point_2m": hourly_dew_point_2m,
            "rain": hourly_rain,
            "snowfall": hourly_snowfall,
            "precipitation": hourly_precipitation,
            "wind_speed_10m": hourly_wind_speed_10m,
            "wind_direction_10m": hourly_wind_direction_10m,
            "wind_gusts_10m": hourly_wind_gusts_10m,
            "soil_temperature_0_to_7cm": hourly_soil_temperature_0_to_7cm,
            "soil_moisture_0_to_7cm": hourly_soil_moisture_0_to_7cm,
            "cloud_cover": hourly_cloud_cover,
            "et0_fao_evapotranspiration": hourly_et0_fao_evapotranspiration,
            "vapour_pressure_deficit": hourly_vapour_pressure_deficit,
            "apparent_temperature": hourly_apparent_temperature,
            "is_day": hourly_is_day,
        })

        hourly_dataframe.to_csv(f"./.data/weather_hourly_quartier_{name}.csv", index=False)

    for _, row in df[['L_QU', 'x', 'y']].iterrows():
        quartier_name = row['L_QU']
        x = row['x']
        y = row['y']
        
        if os.path.exists(f"./.data/weather_hourly_quartier_{quartier_name}.csv"):
            print(f"weather_hourly_quartier_{quartier_name}.csv already exists. Skipping download.")
            continue
        try:
            print(f"\nDownloading weather data for quartier {quartier_name} at coordinates ({x}, {y})")
            download_weather_data(quartier_name, x, y)
            print("\nWaiting for 60 seconds to avoid rate limiting...")
            time.sleep(60)
        except Exception as e:
            print(f"\nFailed to download weather data for quartier {quartier_name}: {e}")
            quit()
        
    print("Download complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Weather data downloader - fetch from OpenMeteo API or download pre-processed file from Google Drive"
    )
    parser.add_argument(
        '-f', '--file',
        action='store_true',
        help='Download a pre-processed CSV file from Google Drive instead of running the full pipeline'
    )
    parser.add_argument(
        '-o', '--output',
        type=str,
        default="./.data/data_final.csv",
        metavar='OUTPUT_PATH',
        help='Output path for the downloaded file (default: ./.data/data_final.csv)'
    )
    
    args = parser.parse_args()
    
    GOOGLE_DRIVE_URL = "https://drive.google.com/file/d/1PTciLGZnN7KodSdsKNkeeNEEdo8TzO9Y/view?usp=share_link"
    
    if args.file:
        if not os.path.exists("./.data"):
            os.makedirs("./.data")
        download_from_google_drive(GOOGLE_DRIVE_URL, args.output)
    else:
        run_weather_download()