import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from openmeteo_sdk.Unit import Unit as UnitType
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse

from constants import HourlyParams
import open_meteo_data_transform as open_meteo_data_transform
from utils import farhenheits_to_celcius, inches_to_millimeter, knots_to_kmh, feet_to_meter

# Setup the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)

def make_request(params: object):
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    return response

def transform_units(unit_type, values):
    match unit_type:
        case UnitType.fahrenheit:
            return farhenheits_to_celcius(values)
        case UnitType.knots:
            return knots_to_kmh(values)
        case UnitType.feet:
            return feet_to_meter(values)
        case UnitType.inch:
            return inches_to_millimeter(values)
        case UnitType.wmo_code:
            return [int(v) for v in values]
        case _:
            return values

def combine_dataframes(response: WeatherApiResponse, params: object):
    hourly = response.Hourly()

    if hourly != None:
        hourly_data = {"date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )}

        # read hourly data
        for i in range(len(params['hourly'])):
            key = params['hourly'][i]
            variable = hourly.Variables(i)
            values = transform_units(
                variable.Unit(), 
                variable.ValuesAsNumpy()
            )
            hourly_data[key] = values

    hourly_dataframe = pd.DataFrame(data=hourly_data)

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    values_lists = (
        daily.Variables(0).ValuesInt64AsNumpy(),
        daily.Variables(1).ValuesInt64AsNumpy(),
        daily.Variables(2).ValuesAsNumpy()
    )

    daily_data = {"date": pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )}

    for i in range(len(params['daily'])):
        key = params['daily'][i]
        variable = daily.Variables(i)
        values = transform_units(
            variable.Unit(), 
            values_lists[i],
        )
        daily_data[key] = values

    daily_dataframe = pd.DataFrame(data=daily_data)
    
    return daily_dataframe, hourly_dataframe

if __name__ == '__main__':
    params = {
        "latitude": 55,
        "longitude": 83,
        "start_date": "2025-06-18",
        "end_date": "2025-06-19",
        "daily": ["sunrise", "sunset", "daylight_duration"],
        "hourly": HourlyParams.to_list(),
        "timezone": "auto",
        "timeformat": "unixtime",
        "wind_speed_unit": "kn",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch"
    }
    response = make_request(params)
    daily_dataframe, hourly_dataframe = combine_dataframes(response, params)
    with open("hourly_data.json", "w") as json_file:
        json_file.write(hourly_dataframe.to_json())
    with open("hourly_data.txt", "w") as txt_file:
        txt_file.write(hourly_dataframe.to_string())
    with open("daily_data.json", "w") as json_file:
        json_file.write(daily_dataframe.to_json())
    with open("daily_data.txt", "w") as txt_file:
        txt_file.write(daily_dataframe.to_string())
    transformed_data = open_meteo_data_transform.transform_dataframes(daily_dataframe, hourly_dataframe)

