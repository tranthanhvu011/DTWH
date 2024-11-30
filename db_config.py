import configparser
import time
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select


class DbConfig:
    def __init__(self):
        # Load configuration from config.ini
        self.config = self.load_config()

        # Configuration for data directory and CSV files
        self.data_dir = self.config['data']['data_dir']
        self.products_csv = self.config['data']['products']
        self.images_csv = self.config['data']['images']
        self.specifications_csv = self.config['data']['specifications']

        # Configuration for URLs and paths
        self.category_url = self.config['urls']['product_url']
        self.chromedriver_path = self.config['path']['chromedriver']

        # Database connection settings
        self.db_user = self.config['database']['user']

        # Create database connections for control and staging databases
        self.db_connection_control = self.create_db_connection('database_control')
        self.db_connection_staging = self.create_db_connection('database_staging')

    def load_config(self):
        """
        Load configuration from the config.ini file.
        :return: Config object
        """
        config = configparser.ConfigParser()
        config.read('config.ini')  # Read the config file
        return config

    def create_db_connection(self, db_name_key):
        """
        Create the database connection string based on the db_name_key from the config file.
        :param db_name_key: Key for the database name in the config.ini (e.g., 'database_control').
        :return: Database connection string.
        """
        return (
            f"mysql+pymysql://{self.config['database']['user']}:" 
            f"{self.config['database']['password']}@{self.config['database']['host']}/" 
            f"{self.config['database'][db_name_key]}?charset={self.config['database']['charset']}"
        )


class Control:
    def __init__(self):
        self.db_config = DbConfig()
        self.engine = create_engine(self.db_config.db_connection_control, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.config_table, self.logs_table = self._define_tables()
        self.metadata.create_all(self.engine)

    def _define_tables(self):
        """
        Define the tables for config and logs.
        """
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

    def load_config(self):
        """
        Load configuration from the database.
        :return: Config record
        """
        with self.engine.begin() as conn:
            result = conn.execute(select(self.config_table).limit(1)).fetchone()
            return result

    def insert_config(self, config_data):
        """
        Insert configuration into the config table.
        :param config_data: Dictionary containing configuration values.
        :return: Inserted ID.
        """
        try:
            with self.engine.begin() as conn:
                result = conn.execute(self.config_table.insert().values(config_data))
                return result.inserted_primary_key[0]
        except SQLAlchemyError as e:
            print(f"Error inserting config: {e}")
            return None

    def write_log(self, action, details, status, config_id=None, process='control'):
        """
        Write a log entry into the logs table.
        :param action: Action performed (e.g., 'Insert', 'Update').
        :param details: Description of the action.
        :param status: Status of the action (e.g., 'Completed', 'Failed').
        :param config_id: Config ID (if applicable).
        :param process: Process name (e.g., 'control').
        """
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        config_id = config_id if config_id else 1  # Default to 1 if no config_id is provided

        with self.engine.begin() as conn:
            conn.execute(self.logs_table.insert().values(
                config_id=config_id,
                timestamp=timestamp,
                action=action,
                details=details,
                process=process,
                status=status
            ))

    def get_config_id(self, user):
        """
        Get the config ID for a specific user.
        :param user: User for whom the config is being retrieved.
        :return: Config ID.
        """
        with self.engine.begin() as conn:
            result = conn.execute(
                select(self.config_table.c.id)
                .where(self.config_table.c.user == user)
            ).fetchone()
            return result['id'] if result else None

    def check_process_in_log(self, process_name, status):
        """
        Check if a specific process has a log entry with the given status.
        :param process_name: The process name to check (e.g., 'control').
        :param status: The status to check (e.g., 'Completed').
        :return: List of logs matching the criteria.
        """
        with self.engine.begin() as conn:
            result = conn.execute(
                select(self.logs_table)
                .where(self.logs_table.c.process == process_name)
                .where(self.logs_table.c.status == status)
            ).fetchall()
            return result
