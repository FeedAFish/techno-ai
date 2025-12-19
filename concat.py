import os
import polars

for i in os.listdir('.data/'):
    if i.startswith('weather_hourly_quartier_') and i.endswith('.csv'):
        df_temp = polars.read_csv(f'.data/{i}')
        quartier = i.split('.')[-2].split('_')[-1]
        df_temp = df_temp.with_columns(
            polars.lit(quartier).alias('quartier')
        )
        df_temp = df_temp[['date',
         'quartier',
         'temperature_2m',
         'relative_humidity_2m',
         'dew_point_2m',
         'rain',
         'snowfall',
         'precipitation',
         'wind_speed_10m',
         'wind_direction_10m',
         'wind_gusts_10m',
         'soil_temperature_0_to_7cm',
         'soil_moisture_0_to_7cm',
         'cloud_cover',
         'et0_fao_evapotranspiration',
         'vapour_pressure_deficit',
         'apparent_temperature',
         'is_day']]
        if 'df_weather' in locals():
            df_weather = polars.concat([df_weather, df_temp])
        else:
            df_weather = df_temp

df_weather.write_csv('./.data/weather_data.csv')