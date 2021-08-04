import http.client
import json
from datetime import date

from User import User


def as_user(dct):
    print(f"debug dct type: {type(dct)}")
    print(f"debug dct value: {dct}")
    return User(dct['id'], dct['name'], dct['address'], dct['phone'])


class UserService:
    def __init__(self, host, port):
        self.host = host
        self.port = port
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
                            "type": "long"
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
        headers = {"Content-type": "application/json"}
        print(f"debug: mapping definition: {mapping}")
        self.conn.request("PUT", f"_template/{self.dtype}_template", headers=headers, body=mapping)
        resp = self.conn.getresponse()
        print(f"info: status:{resp.status}, reason: {resp.reason}")
        print(f"info: response:{resp.read()}")

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
        self.conn.request("GET", f"{self.index}/{self.dtype}/{user.id}"
                          , headers=self.headers
                          , body=json.dumps(user.__dict__))
        resp = self.conn.getresponse()
        print(f"info: status:{resp.status}, reason: {resp.reason}")
        print(f"info: response:{resp.read()}")

    def matching_name(self, name):
        query = {
            "query": {
                "match": {
                    "name": {name}
                }
            }
        }

        query1 = {"query": {"match": {"name": {name}}}}
        print(f"debug: query:{query1.__str__()}")
        self.conn.request("GET", f"{self.readIndex}/{self.dtype}/_search"
                          , headers=self.headers
                          , body=query1)
        # json.dumps(query1.__dict__))
        resp = self.conn.getresponse()
        print(f"info: status:{resp.status}, reason: {resp.reason}")
        print(f"info: response:{resp.read()}")
