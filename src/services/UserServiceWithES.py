import http.client
import json
from datetime import date

from elasticsearch import Elasticsearch
from elasticsearch import RequestError

from User import User


def as_user(dct):
    print(f"debug dct type: {type(dct)}")
    print(f"debug dct value: {dct}")
    return User(dct['id'], dct['name'], dct['address'], dct['phone'])


class UserService:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.es_client = Elasticsearch(hosts=[host])
        self.conn = http.client.HTTPConnection(host, port)
        self.headers = {"Content-type": "application/json"}
        self.index = f"user_{str(date.today())}"
        self.readIndex = "user"
        self.dtype = "user"
        self.register_template()

    def register_template(self):
        mapping = """{
            "index_patterns": [
                "user*"
            ],
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 0
            },
            "aliases": {
                "user": {}
            }
            ,
            "mappings": {
                "user": {
                    "_source": {
                        "enabled": true
                    },
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "keyword"
                        },
                        "phone": {
                            "type": "keyword"
                        },
                        "address": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }"""
        print(f"debug: mapping definition: {mapping}")
        # headers = {"Content-type": "application/json"}
        # self.conn.request("PUT", f"_template/{self.dtype}_template", headers=headers, body=mapping)
        # resp = self.conn.getresponse()
        # print(f"debug: status:{resp.status}, reason: {resp.reason}")
        # print(f"debug: response:{resp.read()}")
        response = self.es_client.indices.put_template(name=f'{self.dtype}_template', body=mapping,)
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
        print(f"debug: host:{self.host}, port:{self.port}")
        print(f"debug: conn:{self.conn}")
        self.conn.request("GET", f"{self.readIndex}/{self.dtype}/{id}")
        resp = self.conn.getresponse()
        print(f"info: status:{resp.status}, reason: {resp.reason}")
        resp_dct = json.loads(resp.read())
        if resp_dct['found']:
            return as_user(resp_dct['_source'])
        return None

    def put_user(self, user):
        # self.conn.request("PUT", f"{self.index}/{self.dtype}/{user.id}"
        #                   , headers=self.headers
        #                   , body=json.dumps(user.__dict__))
        # resp = self.conn.getresponse()
        # print(f"info: status:{resp.status}, reason: {resp.reason}")
        # print(f"info: response:{resp.read()}")
        resp = self.es_client.index(self.index, doc_type=self.dtype, id=user.id, body=user.__dict__)
        print(f"info: response:{resp}")
        return resp['result']

    def matching_name(self, name):
        # query = {
        #     "query": {
        #         "match": {
        #             "name": name
        #         }
        #     }
        # }

        query1 = {"query": {"match": {"name": name}}}
        print(f"debug: query:{query1}")
        result = self.es_client.search(index=self.readIndex, doc_type=self.dtype
                                       , body=query1
                                       , filter_path=['hits.hits._source'])
        print(f"debug: result:{result}")
        # print("query hits:", result["hits"]["hits"])
        return result
