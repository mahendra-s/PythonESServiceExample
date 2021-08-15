import unittest

from src.services.es_airflow_service.ESAirflowService import ESAirflowService


class ESAirflowServiceTestCase(unittest.TestCase):
    service = ESAirflowService('172.22.0.1', 9200)
    doc_id = 1001

    def test_1_create_index(self):
        print(f"Creating Index")
        result = self.service.create_index_if_not_exist()
        print(f"Creating Index Done")
        self.assertEqual(True, result, "Index Created")

    def test_2_put_document(self):
        sample_doc = {'id': self.doc_id, 'user': 'Mahendra', 'job': 987098}
        response = self.service.put_document(sample_doc)
        self.assertEqual("created", response, "Document Stored")
        # self.assertEqual("updated", response['result'], "Document Stored")

    def test_3_update_document(self):
        sample_doc = {'id': self.doc_id, 'user': 'Mahendra', 'job': 987098, 'config': {'nodes': 10, 'heap': '10g'}}
        response = self.service.put_document(sample_doc)
        self.assertEqual("updated", response, "Document Updated")

    def test_4_get_doc_by_id(self):
        response = self.service.get_doc_by_id(self.doc_id)
        expected = {'id': self.doc_id, 'job': 987098, 'user': 'Mahendra', 'config': {'heap': '10g', 'nodes': 10}}
        self.assertEqual(expected, response, "Document populated by id")

    def test_zz_drop_index(self):
        response = self.service.drop_index()
        self.assertEqual(True, response, "Document populated by id")


if __name__ == '__main__':
    unittest.main()
