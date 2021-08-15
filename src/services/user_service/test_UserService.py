import unittest
import uuid
from typing import Optional

from UserService import UserService
from src.services.User import User


class UserServiceTestCase(unittest.TestCase):
    s1 = UserService('172.22.0.1', 9200)

    def test_create_index(self):
        print(f"Creating Index")
        result = self.s1.create_index_if_not_exist()
        print(f"Creating Index Done")
        self.assertEqual(True, result, "create index fail")

    def test_put_user(self):
        user = User(uuid.uuid4().hex, "Mahi", "Pune", "879797977")
        result = self.s1.put_user(user)
        self.assertEqual("created", result, "User Updated")

    def test_get_kiran(self):
        result = self.s1.matching_name("kiran")
        user = {'id': 123, 'name': 'kiran', 'phone': '1231231231', 'address': 'Pune,MH'}
        self.assertEqual(user, result['hits']['hits'][0]['_source'], "First retrieved user does not match")

    def test_get_user_by_id(self):
        result: Optional[User] = self.s1.get_user(123)
        user = {'id': 123, 'name': 'kiran', 'phone': '1231231231', 'address': 'Pune,MH'}
        none_resp: Optional[User] = self.s1.get_user('NoUserId')
        self.assertEqual(None, none_resp, "User Not Found")
        self.assertEqual(user, result.__dict__, "User Found")


if __name__ == '__main__':
    unittest.main()
