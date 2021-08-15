from elasticsearch import Elasticsearch, RequestError, NotFoundError


class CounterService:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es_client = Elasticsearch(hosts=[host])
        self.index = 'counter'
        self.dtype = '_doc'

    def create_index(self):
        result = self.es_client.indices.create(self.index)
        print(f"debug: response:{result}")
        return result['acknowledged']

    def create_index_if_not_exist(self):
        result = ""
        try:
            result = self.create_index()
        except RequestError as ex:
            print(f"debug: Error in Create Index {ex}")
            print(f"debug: Status Code {ex.status_code}")
            print(f"debug: Status info {ex.info}")
            print(f"debug: Status error {ex.error}")
            print(f"debug: Status reason {ex.info['error']['root_cause'][0]['reason']}")
            if ex.error == 'resource_already_exists_exception':
                result = True
        return result

    def drop_index(self):
        result = self.es_client.indices.delete(index=self.index)
        print(f"debug: drop result: {result}")
        return result['acknowledged']

    def next(self, name):
        try:
            result = self.es_client.get(index=self.index, doc_type=self.dtype, id=name)
            print(f"debug: result:{result}")
            if result['found']:
                result['_source']['count'] = result['_source']['count'] + 1
                update_response = self.es_client.index(index=self.index, doc_type=self.dtype,
                                                       id=name, body=result['_source'],
                                                       refresh='wait_for')
                print(f"debug: update counter response: {update_response}")
                return result['_source']['count']
        except NotFoundError as ex:
            # if not (ex.info['found']):
            print(f"info:{ex.info}")
            print(f"error:{ex.error}")
            print(f"status_code:{ex.status_code}")
            result = self.es_client.index(index=self.index, doc_type=self.dtype,
                                          id=name, body={'count': 1},
                                          refresh='wait_for')
            print(f"debug: result:{result}")
            return 1

    def current(self, name):
        try:
            result = self.es_client.get(index=self.index, doc_type=self.dtype, id=name)
            print(f"debug: result:{result}")
            if result['found']:
                return result['_source']['count']
        except NotFoundError as ex:
            print(f"info:{ex.info}")
            print(f"error:{ex.error}")
            print(f"status_code:{ex.status_code}")
            result = self.es_client.index(index=self.index, doc_type=self.dtype,
                                          id=name, body={'count': 1},
                                          refresh='wait_for')
            print(f"debug: result:{result}")
            return 1
