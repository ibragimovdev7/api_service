from fastapi import APIRouter
from elastic_crud.connect_to_elastic import connect
from elastic_crud.crud import ElasticSearchCRUD

router = APIRouter()


@router.get('/get/{id}')
async def elastic_list(id: int):
    es = ElasticSearchCRUD('car_crud', connect()).retrive(id=id)
    return es


@router.get('/list_cars')
async def elastic_list(page: int, size: int):
    es = ElasticSearchCRUD('car_crud', connect()).list(page=page, size=size)
    return es


@router.get('/search')
async def elastic_list(field: str, value, page: int, size: int, query=None):
    es = ElasticSearchCRUD('car_crud', connect()).search(field=field, value=value, page=page, size=size, query=query)
    return es
