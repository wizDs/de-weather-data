from datetime import datetime
from pydantic import BaseModel
from models.weather.geometry import Geometry

class Property(BaseModel):
    created: datetime
    observed: datetime
    parameterId: str
    value: float

class SimpleRecord(BaseModel):
    time: datetime
    value: float

class Record(BaseModel):
    geometry: Geometry
    id: str
    properties: Property

    @property
    def simple(self) -> SimpleRecord:
        return SimpleRecord(
            time=self.properties.observed, 
            value=self.properties.value
        )