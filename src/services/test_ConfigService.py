import unittest

from src.services.ConfigService import app_config


class ConfigServiceTestCase(unittest.TestCase):
    def test_load_config(self):
        print(f"debug: Config file test")
        print(app_config)
        self.assertTrue(app_config, "App Key Config is not available")

    def test_es_host(self):
        es = app_config['elasticsearch']
        print(f"debug: ElasticSearch config:{es}")
        self.assertTrue(es['host'], 'elasticsearch host is not defined')
        self.assertTrue(es['port'], 'elasticsearch port is not defined')


if __name__ == '__main__':
    unittest.main()
