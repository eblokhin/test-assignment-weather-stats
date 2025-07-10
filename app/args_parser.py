import argparse
from datetime import datetime, timedelta
from decimal import Decimal

def _validate_decimal(value):
    """Validate that the value is a decimal string with precision 4."""
    try:
        d = round(Decimal(value), 5)
        decimal_value = d.quantize(Decimal(1)) if d == d.to_integral() else d.normalize()
    except ValueError:
        raise argparse.ArgumentTypeError(f"'{value}' is not a valid decimal number")
    return decimal_value

def _validate_date(value):
    """Validate that the value is a date string in format %Y-%m-%d."""
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"'{value}' is not a valid date in format YYYY-MM-DD"
        )
    
def parse_args():
    today_date = datetime.now().date() - timedelta(days=1)
    parser = argparse.ArgumentParser(
        description="Process location and date range parameters"
    )
    
    # Add arguments with validation
    parser.add_argument(
        "-lon",
        "--longitude",
        type=_validate_decimal,
        required=True,
        help="Longitude as decimal string with precision 4 (e.g., '12.3456')"
    )
    
    parser.add_argument(
        "-lat",
        "--latitude",
        type=_validate_decimal,
        required=True,
        help="Latitude as decimal string with precision 4 (e.g., '-78.9012')"
    )
    
    parser.add_argument(
        "-df",
        "--date_from",
        type=_validate_date,
        help="Start date in YYYY-MM-DD format (defaults to yesterday date)",
        default=today_date
    )
    
    parser.add_argument(
        "-dt",
        "--date_to",
        type=_validate_date,
        help="End date in YYYY-MM-DD format (defaults to yesterday date)",
        default=today_date
    )

    parser.add_argument(
        "--csv",
        action="store_true",
        help="Output result as csv",
        default=True,
    )

    parser.add_argument(
        "--json",
        action="store_true",        
        help="Output result as json"
    )

    args = parser.parse_args()

    if (args.date_to < args.date_from):
        raise argparse.ArgumentTypeError(
            f"'{args.date_from}' must be less or equal than '{args.date_to}'"
        )
    
    return args