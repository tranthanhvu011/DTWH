import configparser
import time
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select
from sqlalchemy import create_engine, text

class DbConfig:
    def __init__(self):
        try:
            self.config = self.load_config()
            self.data_dir = self.config['data']['data_dir']
            self.products_csv = self.config['data']['products']
            self.images_csv = self.config['data']['images']
            self.specifications_csv = self.config['data']['specifications']
            self.category_url = self.config['urls']['product_url']
            self.chromedriver_path = self.config['path']['chromedriver']
            self.db_user = self.config['database']['user']

            # Danh sách database
            self.databases = [
                self.config['database']['database_control'],
                self.config['database']['database_staging'],
                self.config['database']['database_warehouse'],
                self.config['database']['database_mart']
            ]

            self.base_db_connection = (
                f"mysql+pymysql://{self.config['database']['user']}:" 
                f"{self.config['database']['password']}@{self.config['database']['host']}/"
            )

            self.create_databases_if_not_exists()

            self.db_connection_control = self.create_db_connection('database_control')
            self.db_connection_staging = self.create_db_connection('database_staging')
            self.db_connection_warehouse = self.create_db_connection('database_warehouse')
            self.db_connection_mart = self.create_db_connection('database_mart')

        except Exception as e:
            print(f"Error initializing DbConfig: {e}")
            raise

    def load_config(self):
        config = configparser.ConfigParser()
        try:
            config.read('config.ini')
            if not config.sections():
                raise FileNotFoundError("Config file not found or is empty.")
            return config
        except Exception as e:
            print(f"Error reading config file: {e}")
            raise

    def create_databases_if_not_exists(self):
        try:
            engine = create_engine(self.base_db_connection, echo=True)
            with engine.connect() as conn:
                for db_name in self.databases:
                    # Sử dụng text() để thực thi câu lệnh SQL
                    sql = text(f"CREATE DATABASE IF NOT EXISTS `{db_name}` CHARACTER SET {self.config['database']['charset']}")
                    conn.execute(sql)
                    print(f"Database `{db_name}` đã được kiểm tra và tạo nếu cần.")
        except SQLAlchemyError as e:
            print(f"Error creating databases: {e}")
            raise

    def create_db_connection(self, db_name_key):
        try:
            return (
                f"mysql+pymysql://{self.config['database']['user']}:" 
                f"{self.config['database']['password']}@{self.config['database']['host']}/" 
                f"{self.config['database'][db_name_key]}?charset={self.config['database']['charset']}"
            )
        except KeyError as e:
            print(f"Missing configuration for {db_name_key}: {e}")
            raise

class Control:
    def __init__(self):
        self.db_config = DbConfig()
        self.engine = create_engine(self.db_config.db_connection_control, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.config_table, self.logs_table = self._define_tables()
        self.metadata.create_all(self.engine)
        self.process = None
        self.config_id = self.sync_config_with_db()

    def set_process(self, process): 
        self.process = process

    def _define_tables(self):
        config_table = Table('config', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('data_dir', String(255)),
            Column('products_csv', String(255)),
            Column('specifications_csv', String(255)),
            Column('images_csv', String(255)),
            Column('category_url', Text),
            Column('db_connection', Text),
            Column('chromedriver_path', String(255)),
            Column('user', String(50))
        )

        logs_table = Table('logs', self.metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('config_id', Integer, ForeignKey('config.id')),
            Column('timestamp', String(50)),
            Column('action', String(50)),
            Column('details', String(500)),
            Column('process', String(50)),
            Column('status', String(50))
        )

        return config_table, logs_table

    def sync_config_with_db(self):
        return 1  # Simplified to return 1 directly

    def write_log(self, action, details, status, process=None):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        if process is None:
            process = self.process

        with self.engine.begin() as conn:
            conn.execute(self.logs_table.insert().values(
                config_id=self.config_id,
                timestamp=timestamp,
                action=action,
                details=details,
                process=process,
                status=status
            ))

    def test(self):
        print("Running tests for Control class...")

        # Test sync_config_with_db
        config_id = self.sync_config_with_db()
        if config_id:
            print(f"Config ID retrieved: {config_id}")
        else:
            print("Failed to retrieve Config ID")

        # Set the process to staging
        self.set_process("staging")

        # Test write_log
        try:
            self.write_log(action="TestAction", details="This is a test log entry.", status="Success")
            print("Log entry created successfully.")
        except Exception as e:
            print(f"Failed to create log entry: {e}")

        print("Tests completed.")

# Example usage
if __name__ == "__main__":
    control = Control()
    control.test()
