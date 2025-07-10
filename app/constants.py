from enum import StrEnum, auto


class HourlyParams(StrEnum):
  temperature_2m = auto()
  relative_humidity_2m = auto()
  dew_point_2m = auto()

  apparent_temperature = auto()
  temperature_80m = auto()
  temperature_120m = auto()

  wind_speed_10m = auto()
  wind_speed_80m = auto()
  wind_direction_10m = auto()
  wind_direction_80m = auto()

  visibility = auto()
  evapotranspiration = auto()
  weather_code = auto()

  soil_temperature_0cm = auto()
  soil_temperature_6cm = auto()

  rain = auto()
  showers = auto()
  snowfall = auto()

  @classmethod
  def to_list(cls):
      return [m.value for m in cls]
