from datetime import datetime, timedelta, UTC
from collections import namedtuple

from pandas import DataFrame, read_json

ResultRow = namedtuple(
    'ResultRow',
    [
        # Средняя температура на высоте 2 метров за 24 часа (в градусах Цельсия).
        'avg_temperature_2m_24h',
        # Средняя относительная влажность на высоте 2 метров за 24 часа (в процентах).
        'avg_relative_humidity_2m_24h',
        # Средняя точка росы на высоте 2 метров за 24 часа (в градусах Цельсия).
        'avg_dew_point_2m_24h',
        # Средняя кажущаяся температура (ощущаемая температура) за 24 часа (в градусах Цельсия).
        'avg_apparent_temperature_24h',
        # Средняя температура на высоте 80 метров за 24 часа (в градусах Цельсия).
        'avg_temperature_80m_24h',
        # Средняя температура на высоте 120 метров за 24 часа (в градусах Цельсия).
        'avg_temperature_120m_24h',
        # Средняя скорость ветра на высоте 10 метров за 24 часа (в метрах в секунду).
        'avg_wind_speed_10m_24h',
        # Средняя скорость ветра на высоте 80 метров за 24 часа (в метрах в секунду).
        'avg_wind_speed_80m_24h',
        'avg_visibility_24h',  # Средняя видимость за 24 часа (в метрах).
        # Общее количество дождя за 24 часа (в миллиметрах).
        'total_rain_24h',
        # Общее количество ливней за 24 часа (в миллиметрах).
        'total_showers_24h',
        # Общее количество снегопада за 24 часа (в миллиметрах).
        'total_snowfall_24h',
        # Средняя температура на высоте 2 метров за период светового дня (в градусах Цельсия).
        'avg_temperature_2m_daylight',
        # Средняя относительная влажность на высоте 2 метров за период светового дня (в процентах).
        'avg_relative_humidity_2m_daylight',
        # Средняя точка росы на высоте 2 метров за период светового дня (в градусах Цельсия).
        'avg_dew_point_2m_daylight',
        # Средняя кажущаяся температура (ощущаемая емпература) за период светового дня (в градусах Цельсия).
        'avg_apparent_temperature_daylight',
        # Средняя температура на высоте 80 метров за период светового дня (в градусах Цельсия).
        'avg_temperature_80m_daylight',
        # Средняя температура на высоте 120 метров за период светового дня (в градусах Цельсия).
        'avg_temperature_120m_daylight',
        # Средняя скорость ветра на высоте 10 метров за период светового дня (в метрах в секунду).
        'avg_wind_speed_10m_daylight',
        # Средняя скорость ветра на высоте 80 метров за период светового дня (в метрах в секунду).
        'avg_wind_speed_80m_daylight',
        # Средняя видимость за период светового дня (в метрах).
        'avg_visibility_daylight',
        # Общее количество дождя за период светового дня (в миллиметрах).
        'total_rain_daylight',
        # Общее количество ливней за период светового дня (в миллиметрах).
        'total_showers_daylight',
        # Общее количество снегопада за период светового дня (в миллиметрах).
        'total_snowfall_daylight',
        # Скорость ветра на высоте 10 метров, преобразованная в метры в секунду.
        'wind_speed_10m_m_per_s',
        # Скорость ветра на высоте 80 метров, преобразованная в метры в секунду.
        'wind_speed_80m_m_per_s',
        # Температура на высоте 2 метров, преобразованная в градусы Цельсия.
        'temperature_2m_celsius',
        # Кажущаяся температура (ощущаемая температура), преобразованная в градусы Цельсия.
        'apparent_temperature_celsius',
        # Температура на высоте 80 метров, преобразованная в градусы Цельсия.
        'temperature_80m_celsius',
        # Температура на высоте 120 метров, преобразованная в градусы Цельсия.
        'temperature_120m_celsius',
        # Температура почвы на глубине 0 см, преобразованная в градусы Цельсия.
        'soil_temperature_0cm_celsius',
        # Температура почвы на глубине 6 см, преобразованная в градусы Цельсия.
        'soil_temperature_6cm_celsius',
        # Количество дождя, преобразованное в миллиметры.
        'rain_mm',
        # Количество ливней, преобразованное в миллиметры.
        'showers_mm',
        # Количество снегопада, преобразованное в миллиметры.
        'snowfall_mm',
        # Продолжительность светового дня в часах для каждой записи. Вычисляется как разница между временем
        # заката (sunset) и времени восхода (sunrise), переведенная в часы.
        'daylight_hours',
        # Время заката в формате ISO 8601 (например, 2025-05- 16T21:06:29Z).
        'sunset_iso',
        # Время восхода в формате ISO 8601 (например, 2025- 16T21:06:29Z).
        'sunrise_iso'
    ]
)


def day_data_to_record(day_df: DataFrame, day_record: dict):
    if len(day_df) != 24:
        raise Exception('Wrong number of rows')

    day_df['timestamp'] = [int(pd_timestamp.timestamp())
                           for pd_timestamp in day_df.index]
    sunrise, sunset = day_record.sunrise, day_record.sunset

    daylight_df = day_df[lambda df: (
        df.timestamp >= sunrise) & (df.timestamp < sunset)]

    round_list = lambda df_series, decimal_points: [round(v, decimal_points) for v in df_series]
    get_mean = lambda df, valuesKey: round(df[valuesKey].mean(), 2)
    kmh_to_mps = lambda df, valuesKey: [round(v / 1000 / 60, 2) for v in df[valuesKey]]

    row = ResultRow(
        avg_temperature_2m_24h=get_mean(day_df, 'temperature_2m'),
        avg_relative_humidity_2m_24h=get_mean(day_df, 'relative_humidity_2m'),
        avg_dew_point_2m_24h=get_mean(day_df, 'dew_point_2m'),
        avg_apparent_temperature_24h=get_mean(day_df, 'apparent_temperature'),
        avg_temperature_80m_24h=get_mean(day_df, 'temperature_80m'),
        avg_temperature_120m_24h=get_mean(day_df, 'temperature_120m'),
        avg_wind_speed_10m_24h=get_mean(day_df, 'wind_speed_10m'),
        avg_wind_speed_80m_24h=get_mean(day_df, 'wind_speed_80m'),
        avg_visibility_24h=get_mean(day_df, 'visibility'),
        total_rain_24h=day_df.rain.sum(),
        total_showers_24h=day_df.showers.sum(),
        total_snowfall_24h=day_df.snowfall.sum(),
        avg_temperature_2m_daylight=get_mean(daylight_df, 'temperature_2m'),
        avg_relative_humidity_2m_daylight=get_mean(
            daylight_df, 'relative_humidity_2m'),
        avg_dew_point_2m_daylight=get_mean(daylight_df, 'dew_point_2m'),
        avg_apparent_temperature_daylight=get_mean(daylight_df, 'apparent_temperature'),
        avg_temperature_80m_daylight=get_mean(daylight_df, 'temperature_80m'),
        avg_temperature_120m_daylight=get_mean(daylight_df, 'temperature_120m'),
        avg_wind_speed_10m_daylight=get_mean(daylight_df, 'wind_speed_10m'),
        avg_wind_speed_80m_daylight=get_mean(daylight_df, 'wind_speed_80m'),
        avg_visibility_daylight=get_mean(daylight_df, 'visibility'),
        total_rain_daylight=daylight_df.rain.sum(),
        total_showers_daylight=daylight_df.showers.sum(),
        total_snowfall_daylight=daylight_df.snowfall.sum(),
        wind_speed_10m_m_per_s=kmh_to_mps(day_df, 'wind_speed_10m'),
        wind_speed_80m_m_per_s=kmh_to_mps(day_df, 'wind_speed_10m'),
        temperature_2m_celsius=round_list(day_df.temperature_2m, 2),
        apparent_temperature_celsius=round_list(day_df.apparent_temperature, 2),
        temperature_80m_celsius=round_list(day_df.temperature_80m, 2),
        temperature_120m_celsius=round_list(day_df.temperature_120m, 2),
        soil_temperature_0cm_celsius=round_list(day_df.soil_temperature_0cm, 2),
        soil_temperature_6cm_celsius=round_list(day_df.soil_temperature_6cm, 2),
        rain_mm=round_list(day_df.rain, 2),
        showers_mm=round_list(day_df.showers, 2),
        snowfall_mm=round_list(day_df.snowfall, 2),
        daylight_hours=round(day_record.daylight_duration / 3600, 2),
        sunrise_iso=datetime.fromtimestamp(
            sunrise, UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
        sunset_iso=datetime.fromtimestamp(
            sunset, UTC).strftime('%Y-%m-%dT%H:%M:%SZ')
    )

    return row


def transform_dataframes(daily_dataframe: DataFrame, hourly_dataframe: DataFrame) -> DataFrame:
    daily_dataframe.set_index('date', inplace=True)
    hourly_dataframe.set_index('date', inplace=True)
    day_seconds = timedelta(days=1)
    data = {
        'date': []
    }

    for day in daily_dataframe.itertuples():
        day_start, day_end = day.Index, day.Index + day_seconds
        day_df = hourly_dataframe[(hourly_dataframe.index >= day_start) & (
            hourly_dataframe.index < day_end)]
        new_row = day_data_to_record(day_df, day)
        data['date'].append(day.Index.strftime('%Y-%m-%d'))
        for index, field_key in enumerate(new_row._fields):
            if not field_key in data:
                data[field_key] = []
            data[field_key].append(new_row[index])
    result_df = DataFrame(data)
    
    return result_df


if __name__ == '__main__':
    try:
        daily_dataframe = read_json('daily_data.json')
        hourly_dataframe = read_json('hourly_data.json')
    except Exception as e:
        print('Couldnt read data files')
        raise e

    transform_dataframes(daily_dataframe, hourly_dataframe)
