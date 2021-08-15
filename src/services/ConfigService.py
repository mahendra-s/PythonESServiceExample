import yaml


class Configuration(object):
    @staticmethod
    def laod_config():
        file = 'conf/config.yaml'
        print(f'debug: loading config file:{file}')
        with open(file, "r") as f:
            return yaml.safe_load(f)


app_config = Configuration.laod_config()['app']
