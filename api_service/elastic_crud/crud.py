from fastapi import HTTPException


class ElasticSearchCRUD:
    def __init__(self, name, client):
        self.name = name
        self.client = client
        if not self.client.indices.create(index=self.name):
            self.client.indices.create(index=self.name)

    def create(self, **kwargs):
        try:
            if self.client.indices.exists(index=self.name):
                if not self.client.indices.exists(index=self.name, id=kwargs['data']['id']):
                    self.client.index(index=self.name, document=kwargs['data'], id=kwargs['data']['id'])
                    return self.client.get(index=self.name, id=kwargs['data']['id'])
                else:
                    raise HTTPException(status_code=400, detail={
                        'error': f'Such an object with id = {kwargs["data"]["id"]} already exists'})
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exists'})
        except Exception as e:
            raise HTTPException(status_code=400, detail={'error': e})

    def retrive(self, id):
        try:
            if self.client.indices.exists(index=self.name):
                if self.client.exists(index=self.name, id=id):
                    return self.client.get(index=self.name, id=id)['_source']
                else:
                    raise HTTPException(status_code=400, detail={'error': f'Object with id={id} does not exist'})
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exist'})
        except Exception as e:
            raise HTTPException(status_code=400, detail={'error': e})

    def list(self, page=0, size=10, sort=('id')):
        resp = {'results': [], 'total': 0}
        try:
            if self.client.indices.exists(index=self.name):
                results = []
                res = self.client.search(index=self.name, size=size, from_=page * size, sort=sort)
                for i in res['hits']['hits']:
                    results.append(i['_source'])
                resp['results'] = results
                resp['total'] = res['hits']['total']['value']
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exist'})
        except Exception as e:
            raise HTTPException(status_code=400, detail={'error': e})
        return resp

    def delete(self, id):
        try:
            if self.client.indices.exists(index=self.name):
                if self.client.exists(index=self.name, id=id):
                    self.client.delete(index=self.name, id=id)
                    return {'message': 'Successful deleted!'}
                else:
                    raise HTTPException(status_code=400, detail={'error': f'Object with id={id} does not exist'})
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exist'})
        except Exception as e:
            raise HTTPException(status_code=400, detail={'error': e})

    def update(self, id, **kwargs):
        try:
            if self.client.indices.exists(index=self.name):
                if self.client.exists(index=self.name, id=id):
                    self.client.update(index=self.name, id=id, doc=kwargs['data'])
                    return self.client.get(index=self.name, id=id)['_source']
                else:
                    raise HTTPException(status_code=400, detail={'error': f'Object with id={id} does not exist'})
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exist'})
        except Exception as e:
            raise HTTPException(status_code=400, detail={'error': e})

    def search(self, field, value, page=0, size=10, query=None, sort=('id')):
        resp = {'results': [], 'total': 0}
        try:
            if self.client.indices.exists(index=self.name):
                if self.client.search(index=self.name)['hits']['hits']:
                    if field in self.client.search(index=self.name)['hits']['hits'][0]['_source']:
                        if query:
                            res = self.client.search(index=self.name, query=query, size=size, page=page * size,
                                                     sort=sort)
                            resp['results'] = [i['_source'] for i in res['hits']['hits']]
                            resp['total'] = res['hits']['total']['value']
                            return resp
                        query = {
                            'match': {
                                field: value
                            }
                        }
                        res = self.client.search(index=self.name, query=query, size=size, from_=page * size)
                        resp['results'] = [i['_source'] for i in res['hits']['hits']]
                        resp['total'] = res['hits']['total']['value']
                        return resp
                    else:
                        raise HTTPException(status_code=400, detail={'error': f'Not found field={field}'})
                else:
                    return resp
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exist'})
        except Exception as e:
            print('---', e)
            raise HTTPException(status_code=400, detail={'error': e})

    def filter(self, page=0, size=10, query=None, sort=('id'), **kwargs):
        resp = {'results': [], 'total': 0}
        try:
            if self.client.indices.exists(index=self.name):
                if self.client.indices.search(index=self.name)['hits']['hits']:
                    if query:
                        res = self.client.search(index=self.name, query=query, size=size, from_=page * size, sort=sort)
                        resp['results'] = [i['_source'] for i in res['hits']['hits']]
                        resp['total'] = res['hits']['total']['value']
                        return resp
                    must = []
                    for key, value in kwargs.items():
                        if key in self.client.search(index=self.name)['hits']['hits'][0]['_source']:
                            must.append({'match': {key: value}})
                    if not must:
                        return []
                    query = {
                        'bool': {
                            'must': must
                        }
                    }
                    res = self.client.search(index=self.name, query=query, size=size, from_=page * size, sort=sort)
                    resp['results'] = [i['_source'] for i in res['hits']['hits']]
                    resp['total'] = res['hits']['total']['value']
                    return resp
                else:
                    return resp
            else:
                raise HTTPException(status_code=400, detail={'error': 'Index with this name does not exist'})
        except Exception as e:
            raise HTTPException(status_code=400, detail={'error': e})
