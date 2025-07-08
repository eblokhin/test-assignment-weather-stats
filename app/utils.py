KMH_TO_KNOT = 1.852
MILLIMETER_TO_INCH = 25.4
METER_TO_FEET = 0.3048

def knots_to_kmh(knots: list[float]) -> list[float]:
  return [round(v * KMH_TO_KNOT, 3) for v in knots]

def farhenheits_to_celcius(farhs: list[float]) -> list[float]:
  return [round((v - 32) * 5 / 9, 2) for v in farhs]

def inches_to_millimeter(inches: list[float]) -> list[float]:
  return [round(v * MILLIMETER_TO_INCH) for v in inches]

def feet_to_meter(feets: list[float]) -> list[float]:
  return [round(v * METER_TO_FEET, 2) for v in feets]
