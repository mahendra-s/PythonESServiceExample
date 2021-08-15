from datetime import date

from elasticsearch import Elasticsearch
from elasticsearch import RequestError


class ESAirflowService:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es_client = Elasticsearch(hosts=[host])
        self.index = 'counter'
        self.dtype = '_doc'
        self.register_template()

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

