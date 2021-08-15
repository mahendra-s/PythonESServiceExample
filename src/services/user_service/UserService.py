from datetime import date

from elasticsearch import Elasticsearch, RequestError

from src.services.User import User


def as_user(dct):
    print(f"debug dict value: {dct}")
    return User(dct['id'], dct['name'], dct['address'], dct['phone'])


class UserService:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es_client = Elasticsearch(hosts=[host])
        self.headers = {"Content-type": "application/json"}
        self.index = f"user_{str(date.today())}"
        self.readIndex = "user"
        self.dtype = "user"
        self.register_template()

    def register_template(self):
        mapping = {
            "index_patterns": ["user*"],
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 0
            },
            "aliases": {"user": {}},
            "mappings": {
                "user": {
                    "_source": {"enabled": True},
                    "properties": {
                        "id": {"type": "keyword"},
                        "name": {"type": "keyword"},
                        "phone": {"type": "keyword"},
                        "address": {"type": "keyword"}
                    }
                }
            }
        }
        print(f"debug: mapping definition: {mapping}")
        response = self.es_client.indices.put_template(name=f'{self.dtype}_template', body=mapping, )
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

    def get_user(self, id):
        id_query = {'query': {'term': {'_id': {'value': id}}}}
        response = self.es_client.search(index=self.readIndex, doc_type=self.dtype, body=id_query)
        print(f'debug: response: {response}')
        if response['hits']['total'] == 1:
            return as_user(response['hits']['hits'][0]['_source'])
        return None

    def put_user(self, user):
        resp = self.es_client.index(self.index, doc_type=self.dtype, id=user.id, body=user.__dict__, refresh='wait_for')
        print(f"info: response:{resp}")
        return resp['result']

    def matching_name(self, name):
        query1 = {"query": {"match": {"name": name}}}
        print(f"debug: query:{query1}")
        result = self.es_client.search(index=self.readIndex, doc_type=self.dtype
                                       , body=query1
                                       , filter_path=['hits.hits._source'])
        print(f"debug: result:{result}")
        return result
