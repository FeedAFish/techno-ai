import requests
import os
import polars
import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime, timedelta

if not os.path.exists("./.data"):
    os.makedirs("./.data")

url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/quartier_paris/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
response = requests.get(url)
if not os.path.exists("./.data/quartier_paris.csv"):
    with open("./.data/quartier_paris.csv", "wb") as f:
        f.write(response.content)
else: 
    print("quartier_paris.csv already exists. Skipping download.")

df = polars.read_csv("./.data/quartier_paris.csv", separator = ';')
df = df.with_columns(
    polars.col('Geometry X Y').map_elements(lambda s: float(s.split(',')[0]), return_dtype=polars.Float64).alias('x'),
    polars.col('Geometry X Y').map_elements(lambda s: float(s.split(',')[1]), return_dtype=polars.Float64).alias('y')
)

def download_weather_data(name,x,y):

    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
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
    dates = [start_time + timedelta(seconds=interval_seconds * i) for i in range(num_steps)]

    hourly_dataframe = polars.DataFrame({
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

    hourly_dataframe.write_csv(f"./.data/weather_hourly_quartier_{name}.csv")

import time
for i in df[['L_QU','x','y']].iter_rows():
    
    if os.path.exists(f"./.data/weather_hourly_quartier_{i[0]}.csv"):
        print(f"weather_hourly_quartier_{i[0]}.csv already exists. Skipping download.")
        continue
    try : 
        print(f"\nDownloading weather data for quartier {i[0]} at coordinates ({i[1]}, {i[2]})")
        download_weather_data(i[0],i[1],i[2])
    except Exception as e:
        print(f"\nFailed to download weather data for quartier {i[0]}: {e}")
        quit()
    print("\nWaiting for 60 seconds to avoid rate limiting...")
    time.sleep(60)
    

print("Download complete.")


