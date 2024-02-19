from pydantic import BaseModel


class Car(BaseModel):
    model: str
    price: float
    year: str
    milage: int
