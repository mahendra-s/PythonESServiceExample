import unittest
import uuid

from User import User
from UserServiceWithES import UserService


class UserServiceTestCase(unittest.TestCase):
    s1 = UserService('172.22.0.1', 9200)

    def test_something(self):
        self.assertEqual(True, True)

    def test_create_index(self):
        # s1 = UserService('172.22.0.1', 9200)
        print(f"Creating Index")
        result = self.s1.create_index_if_not_exist()
        print(f"Creating Index Done")
        self.assertEqual(True, result, "create index fail")

    def test_put_user(self):
        user = User(uuid.uuid4().hex, "Mahi", "Pune", "879797977")
        result = self.s1.put_user(user)
        # self.assertEqual("updated", result, "User Updated")
        self.assertEqual("created", result, "User Updated")

    def test_get_kiran(self):
        # s1 = UserService('172.22.0.1', 9200)
        result = self.s1.matching_name("kiran")
        user = {'id': 123, 'name': 'kiran', 'phone': '1231231231', 'address': 'Pune,MH'}
        expected = {'hits': {'hits': [{'_source': user}]}}
        self.assertEqual(expected, result, "Retrieved user does not match")
        self.assertEqual(user, result['hits']['hits'][0]['_source'], "First retrieved user does not match")


if __name__ == '__main__':
    unittest.main()
