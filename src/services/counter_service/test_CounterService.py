import unittest

from src.services.counter_service.CounterService import CounterService


class CounterServiceTestCase(unittest.TestCase):
    service = CounterService('172.22.0.1', 9200)

    def test_create_index(self):
        response = self.service.create_index_if_not_exist()
        print("Service Index Created")
        self.assertEqual(True, response, 'Index Created')

    def test_zz_drop_index(self):
        response = self.service.drop_index()
        print("Service Index Dropped")
        self.service.es_client.close()
        self.assertEqual(True, response, "Index Dropped")

    def test_get_first_time(self):
        resp = self.service.next("FirstCounterName")
        self.assertEqual(1, resp, "First Time Call with New name match")

    def test_get_first_three_numbers(self):
        resp1 = self.service.next("NewName")
        resp2 = self.service.next("NewName")
        resp3 = self.service.next("NewName")
        self.assertEqual(1, resp1, "First Call with New name match")
        self.assertEqual(2, resp2, "Second Call with New name match")
        self.assertEqual(3, resp3, "Third Call with New name match")

    def test_get_current_with_first_three(self):
        resp1 = 1  # self.service.next("AnotherNewName")
        curr1 = self.service.current("AnotherNewName")
        curr1_twice = self.service.current("AnotherNewName")
        curr1_thrice = self.service.current("AnotherNewName")
        resp2 = self.service.next("AnotherNewName")
        curr2 = self.service.current("AnotherNewName")
        resp3 = self.service.next("AnotherNewName")
        curr3 = self.service.current("AnotherNewName")
        self.assertEqual(1, resp1, "First Call with New name match")
        self.assertEqual(1, curr1, "Second Call with New name match")
        self.assertEqual(1, curr1_twice, "Second Call with New name match")
        self.assertEqual(1, curr1_thrice, "Second Call with New name match")
        self.assertEqual(2, resp2, "Third Call with New name match")
        self.assertEqual(2, curr2, "Third Call with New name match")
        self.assertEqual(3, resp3, "Third Call with New name match")
        self.assertEqual(3, curr3, "Third Call with New name match")

    def close_connection(self):
        self.service.es_client.close()


if __name__ == '__main__':
    unittest.main()
