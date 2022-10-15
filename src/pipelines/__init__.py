import logging
import pandas as pd
import pathlib
import uuid
from datetime import datetime,  timedelta
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel
from loaders.weather import DMIClientWrapper
from dmi_open_data import Parameter


class Config(BaseModel):
    year: int
    month: int
    path: str
    apikey: str
    station_name: str
    datatype: Parameter

    def start_date(self):
        return datetime(self.year, self.month, 1)

    def end_date(self):
        return self.start_date() + relativedelta(months=+1) - timedelta(days=1)

    def get_full_path(self) -> pathlib.Path:
        full_path = f"{self.path}/{self.datatype.value}/year={self.year}/month={self.month}/"
        return pathlib.Path(full_path)


class DataPipeline:

    def __init__(self, config: Config) -> None:
        self.config = config

    def persist_to_parquet(self, data: pd.DataFrame) -> None:
        path = self.config.get_full_path()
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)

        data.to_parquet(path=path / f"{uuid.uuid4()}.parquet")

    def run(self):
        raise NotImplementedError()


class WeatherPipeline(DataPipeline):

    def __init__(self, config: Config) -> None:
        super().__init__(config)

    def run(self):
        start_date = self.config.start_date()
        end_date = self.config.end_date()

        # Extract data from energidataservice
        dmi_client = DMIClientWrapper(self.config.apikey)
        simplerecords = dmi_client.get(
            station_name=self.config.station_name,
            parameter=self.config.datatype,
            from_time=start_date,
            to_time=end_date
        )
        if not simplerecords:
            raise Warning(
                f"no data was found for {start_date.year}-{start_date.month}")

        # Create pandas data frame
        data = pd.DataFrame([row.dict() for row in simplerecords])
        data["station"] = self.config.station_name

        # persist data
        logging.info(
            "%s-data for %s-%s stored to %s",
            self.config.datatype.value,
            start_date.year,
            start_date.month,
            self.config.path
        )
        self.persist_to_parquet(data=data)
