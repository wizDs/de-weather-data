import pipelines
import argparse
import logging
from datetime import datetime
from dmi_open_data import Parameter


def get_apikey(path: str = "apikey") -> str:
    try:
        with open(file=path, mode="r") as file:
            secret = file.read()
        return secret
    except FileNotFoundError as e:
        raise FileNotFoundError(f"apikey path '{path}' was not found") from e


def get_args() -> pipelines.Config:

    parser = argparse.ArgumentParser(
        description='run datapipeline for 1 month')
    parser.add_argument('--year', type=int,
                        help='year for which the data pipeline should be run')
    parser.add_argument('--month', type=int,
                        help='month for which the data pipeline should be run')
    parser.add_argument('--path', default='./data', type=str,
                        help='the destination where data will be persisted')
    parser.add_argument('--datatype', default='temp_dry', type=str,
                        help='type of datapipeline to be run', choices=["spotprice", "powersystem", "weather"])
    parser.add_argument('--apikeypath', default="apikey", type=str,
                        help='destination where the apikey to the weather client is stored')
    parser.add_argument('--station', default="Københavns Lufthavn", type=str,
                        help='the station name where the observations are collected')

    args = parser.parse_args()

    return pipelines.Config(
        year=args.year,
        month=args.month,
        path=args.path,
        apikey=get_apikey(args.apikeypath),
        station_name=args.station,
        datatype=Parameter(args.datatype)
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    config = get_args()
    if config.start_date() <= datetime.today():
        try:
            pipelines.WeatherPipeline(config).run()
        except Warning as w:
            logging.warning(w)
