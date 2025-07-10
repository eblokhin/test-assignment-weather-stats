from pandas import DataFrame

from app.args_parser import parse_args
from app.constants import HourlyParams
from app.db_client import save_records_data
from app.open_meteo_data_transform import WeatherStatsNamedTuple, transform_dataframes
from app.request import make_request, combine_dataframes

def main():
    args = parse_args()
    
    # Display the parsed arguments
    print(f"Longitude: {args.longitude}")
    print(f"Latitude: {args.latitude}")
    print(f"Date from: {args.date_from}")
    print(f"Date to: {args.date_to}")
    print(f"Output as CSV: {args.csv}")
    print(f"Output as JSON: {args.json}")

    params = {
        "latitude": args.latitude,
        "longitude": args.longitude,
        "start_date": args.date_from,
        "end_date": args.date_to,
        "daily": ["sunrise", "sunset", "daylight_duration"],
        "hourly": HourlyParams.to_list(),
        "timezone": "auto",
        "timeformat": "unixtime",
        "wind_speed_unit": "kn",
        "temperature_unit": "fahrenheit",
        "precipitation_unit": "inch"
    }
    response = make_request(params)
    (
        daily_dataframe, hourly_dataframe, timezone_name, longitude, latitude
    ) = combine_dataframes(response, params)

    result_list: list[dict[str, WeatherStatsNamedTuple]] = transform_dataframes(
        daily_dataframe, hourly_dataframe)
    
    df_data = {
        'date': []
    }
    result_records = []
    
    for row in result_list:
        [date, data] = list(row.values())
        
        # composing DataFrame data
        df_data['date'].append(date)
        for index, field_key in enumerate(data._fields):
            if not field_key in df_data:
                df_data[field_key] = []
            df_data[field_key].append(data[index])

        # composing model objects for DB
        record = {
            'longitude':longitude,
            'latitude':latitude,
            'date':date,
            'timezone':timezone_name,
            'data':data._asdict()
        }
        result_records.append(record)
    
    save_records_data(result_records)

    result_df = DataFrame(data=df_data)
    for idx in range(len(result_df)):
        day_df = result_df.iloc[idx]
        
        if args.json == True:
            with open(f"output/{longitude}_{latitude}_{day_df.date}.json", "w") as json_file:
                json_file.write(day_df.to_json())
        if args.csv == True:
            with open(f"output/{longitude}_{latitude}_{day_df.date}.csv", "w") as csv_file:
                csv_file.write(day_df.to_csv())

if __name__ == "__main__":
    main()