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
            self.db_connection = (
                    f"mysql+pymysql://{self.config['database']['user']}:" 
                    f"{self.config['database']['password']}@{self.config['database']['host']}/"
                    f"{self.config['database']['database_control']}?charset={self.config['database']['charset']}"
)


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
        self.db_config = DbConfig()  # Kết nối đến DbConfig, nơi quản lý cấu hình kết nối
        self.engine = create_engine(self.db_config.db_connection_control, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()

        # Định nghĩa bảng config và logs
        self.config_table, self.logs_table = self._define_tables()

        # Tạo bảng nếu chưa tồn tại
        self.metadata.create_all(self.engine)
        self.insert_config()
        
        # Chèn cấu hình vào bảng config nếu chưa có
        self.config_id = self.insert_config()  # Trả về ID của config đã chèn hoặc tồn tại
        self.process = None  # Giá trị mặc định cho process

    def set_process(self, process): 
        self.process = process

    def _define_tables(self):
        # Định nghĩa bảng config
        config_table = Table('config', self.metadata,
            Column('id', Integer, primary_key=True),
            Column('data_dir', String(255)),
            Column('products_csv', String(255)),
            Column('specifications_csv', String(255)),
            Column('images_csv', String(255)),
            Column('category_url', Text),
            Column('db_connection', Text),
            Column('chromedriver_path', String(255)),
            Column('user', String(50))
        )

        # Định nghĩa bảng logs
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
    def get_config_data(self):
        # Retrieve config data from the config table
        with self.engine.connect() as conn:
            result = conn.execute(select(self.config_table)).mappings().fetchone()
            if result:
                return dict(result)
            else:
                raise ValueError("Configuration not found in the config table.")
    def insert_config(self):
        try:
            with self.engine.connect() as conn:
                # Kiểm tra xem cấu hình đã tồn tại chưa
                result = conn.execute(
                    select(self.config_table.c.id).where(
                        self.config_table.c.user == self.db_config.db_user
                    )
                ).mappings().fetchone()

                if result:  # Nếu tồn tại, trả về ID của cấu hình
                    print(f"Cấu hình đã tồn tại. ID: {result['id']}")
                    return result['id']

                # Nếu chưa tồn tại, chèn cấu hình mới
                print(f"Cấu hình chưa tồn tại, tiến hành chèn mới cho user {self.db_config.db_user}")
                insert_stmt = self.config_table.insert().values(
                    data_dir=self.db_config.data_dir,
                    products_csv=self.db_config.products_csv,
                    specifications_csv=self.db_config.specifications_csv,
                    images_csv=self.db_config.images_csv,
                    category_url=self.db_config.category_url,
                    db_connection=self.db_config.db_connection,
                    chromedriver_path=self.db_config.chromedriver_path,
                    user=self.db_config.db_user
                )
                conn.execute(insert_stmt)
                conn.commit()  # Ghi thay đổi vào cơ sở dữ liệu

                # Lấy ID của cấu hình vừa chèn
                new_config_id = conn.execute(
                    select(self.config_table.c.id).where(
                        self.config_table.c.user == self.db_config.db_user
                    )
                ).mappings().fetchone()

                if new_config_id:
                    print(f"Đã chèn cấu hình mới. ID: {new_config_id['id']}")
                    return new_config_id['id']
                else:
                    raise RuntimeError("Không thể lấy ID sau khi chèn cấu hình mới.")
        except Exception as e:
            print(f"Lỗi khi chèn cấu hình: {e}")
            raise

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

        # Test insert_config
        config_id = self.insert_config()
        if config_id:
            print(f"Config ID retrieved: {config_id}")
        else:
            print("Failed to retrieve Config ID")

        # Set the process to staging
        self.set_process("staging")

        print("Tests completed.")

# Example usage
if __name__ == "__main__":
    control = Control()
    control.test()
