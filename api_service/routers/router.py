from fastapi import APIRouter
from elastic_crud.connect_to_elastic import connect
from elastic_crud.crud import ElasticSearchCRUD
from services_model.car_model import Car

from .apis.api import main_url
import requests

router = APIRouter()


@router.get('/elastic_list')
async def elastic_list(page: int, size: int):
    es = ElasticSearchCRUD('car_crud', connect()).list(page=page, size=size)
    return es


@router.get('/elastic_search')
async def elastic_search(field: str, value, page: int, size: int, query=None):
    es = ElasticSearchCRUD('car_crud', connect()).search(field=field, value=value, page=page, size=size, query=query)
    return es


@router.get('/get/{id}')
async def get_item(id: int):
    url = main_url.MAIN_URL + f'/{id}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print(data)
        return data
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")


@router.post('/create/')
async def create_item(car: Car):
    model = car.model
    year = car.year
    price = car.price
    milage = car.milage
    r = requests.post('https://httpbin.org / post', data={
        'model': model,
        'year': year,
        'price': price,
        'milage': milage
    })

    return r.json()
