import unittest

import utils

class TestUtils(unittest.TestCase):
  def test_knots_to_kmh(self):
    expected = [3.148, 9.816, 0.229]
    res = utils.knots_to_kmh([1.7, 5.3, 0.1234])
    self.assertEqual(res, expected)

  def test_farhenheits_to_celcius(self):
    expected = [37.91, -20.83, 10]
    res = utils.farhenheits_to_celcius([100.234, -5.5, 50])
    self.assertEqual(res, expected)

  def test_inches_to_centimeter(self):
    expected = [1283, -94, 268]
    res = utils.inches_to_millimeter([50.5, -3.7, 10.5432])
    self.assertEqual(res, expected)

  def test_feet_to_meter(self):
    expected = [15.39, -1.13, 3.21]
    res = utils.feet_to_meter([50.5, -3.7, 10.5432])
    self.assertEqual(res, expected)

# Executing the tests in the above test case class
if __name__ == "__main__":
  unittest.main()