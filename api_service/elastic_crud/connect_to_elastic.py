from elasticsearch import Elasticsearch
from .conf import settings


def connect() -> Elasticsearch:
    client = Elasticsearch(
        settings.ELASTIC_PATH,
        verify_certs=False,
        basic_auth=(settings.ELASTIC_USER, settings.ELASTIC_PASSWORD)
    )

    return client
