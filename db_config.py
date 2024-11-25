import configparser
import os


class DbConfig:
    def __init__(self):
        self.config = self.load_config()
        # Cấu hình từ file `config.example.ini`
        self.data_dir = self.config['data']['data_dir']
        self.products_csv = self.config['data']['products']
        self.images_csv = self.config['data']['images']
        self.specifications_csv = self.config['data']['specifications']
        self.category_url = self.config['urls']['product_url']
        self.db_connection = f"mysql+pymysql://{self.config['database']['user']}:" \
                             f"{self.config['database']['password']}@{self.config['database']['host']}/" \
                             f"{self.config['database']['database']}?charset={self.config['database']['charset']}"
        self.chromedriver_path = self.config['path']['chromedriver']
        self.db_user = self.config['database']['user']

    def load_config(self):
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(__file__), 'config.example.ini')

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found at {config_path}")
        config.read(config_path)
        return config
