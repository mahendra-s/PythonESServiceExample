import unittest
import uuid
from typing import Optional

from src.services.ConfigService import app_config
from src.services.User import User
from src.services.user_service.UserService import UserService


class UserServiceTestCase(unittest.TestCase):
    host = app_config['elasticsearch']['host']
    port = app_config['elasticsearch']['port']
    service = UserService(host, port)

    def test_create_index(self):
        print(f"Creating Index")
        result = self.service.create_index_if_not_exist()
        print(f"Creating Index Done")
        self.assertEqual(True, result, "create index fail")

    def test_put_user(self):
        user = User(uuid.uuid4().hex, "Mahi", "Pune", "879797977")
        result = self.service.put_user(user)
        self.assertEqual("created", result, "User Updated")

    def test_get_kiran(self):
        result = self.service.matching_name("kiran")
        user = {'id': 123, 'name': 'kiran', 'phone': '1231231231', 'address': 'Pune,MH'}
        self.assertEqual(user, result['hits']['hits'][0]['_source'], "First retrieved user does not match")

    def test_get_user_by_id(self):
        result: Optional[User] = self.service.get_user(123)
        user = {'id': 123, 'name': 'kiran', 'phone': '1231231231', 'address': 'Pune,MH'}
        none_resp: Optional[User] = self.service.get_user('NoUserId')
        self.assertEqual(None, none_resp, "User Not Found")
        self.assertEqual(user, result.__dict__, "User Found")

    def test_z_last(self):
        try:
            self.close_connection()
        except:
            self.assertFalse("Error Closing Connection")
        self.assertTrue('Connection Closed')

    def close_connection(self):
        self.service.es_client.close()


if __name__ == '__main__':
    unittest.main()
