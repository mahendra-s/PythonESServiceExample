from datetime import date

from elasticsearch import Elasticsearch, RequestError


class ESAirflowService:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es_client = Elasticsearch(hosts=[host])
        self.index = f'airflow_{str(date.today())}'
        self.readIndex = 'airflow'
        self.dtype = '_doc'
        self.register_template()

    def register_template(self):
        mapping = {
            "index_patterns": ["airflow*"],
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 0
            },
            "aliases": {"airflow": {}},
            "mappings": {
                "_doc": {
                    "_source": {"enabled": True}
                }
            }
        }
        print(f"debug: mapping definition: {mapping}")
        response = self.es_client.indices.put_template(name=f'{self.readIndex}_template', body=mapping, )
        print(f"debug: response: {response}")
        return response['acknowledged']

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

    def put_document(self, dct):
        id_query = {'query': {'term': {"_id": {"value": dct['id']}}}}
        result = self.es_client.search(index=self.readIndex, body=id_query)
        print(f'debug: result: {result}')
        if result['hits']['total'] >= 1:
            existing_index = result['hits']['hits'][0]['_index']
            result = self.es_client.index(index=existing_index, id=dct['id'], body=dct, refresh='wait_for')
        else:
            result = self.es_client.index(index=self.index, id=dct['id'], body=dct, refresh='wait_for')
        print(f"debug: result:{result}")
        return result['result']

    def drop_index(self):
        response = self.es_client.indices.delete(index=self.index)
        print(f"debug: response:{response}")
        return response['acknowledged']

    def get_doc_by_id(self, id):
        id_query = {'query': {'term': {"_id": {"value": id}}}}
        result = self.es_client.search(index=self.readIndex, body=id_query)
        print(f'debug: result: {result}')
        if result['hits']['total'] >= 1:
            return result['hits']['hits'][0]['_source']
        return None
