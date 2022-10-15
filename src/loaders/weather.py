import pandas as pd
from dmi_open_data import DMIOpenDataClient, Parameter
from models.weather.weatherstation import WeatherStation
from models.weather.record import Record, SimpleRecord


class DMIClientWrapper:

    def __init__(self, api_key: str):
        self.client = DMIOpenDataClient(api_key=api_key)
        self.stations = self.get_stations()

    def get(self, station_name: str, parameter: Parameter, **kwargs) -> list[SimpleRecord]:

        # Identify station from station_name
        try:
            station = next(
                station for station in self.stations
                if station.properties.name == station_name
            )
        except StopIteration as exc:
            raise Exception(f"The station '{station_name}' is not valid") from exc

        if parameter.value not in set(station.properties.parameterId):
            return pd.DataFrame()

        # Get temperature observations from DMI station in given time period
        observations = self.client.get_observations(
            parameter=parameter,
            station_id=station.properties.stationId,
            **kwargs
        )
        records = map(Record.parse_obj, observations)

        # Select only SimpleRecord
        simplerecords = map(lambda r: r.simple, records)

        return list(simplerecords)

    
    def get_stations(self) -> list[WeatherStation]:
        stations = map(WeatherStation.parse_obj, self.client.get_stations())
        active_stations = filter(
            lambda s: s.properties.status == 'Active', 
            stations
        )
        return list(active_stations)

    