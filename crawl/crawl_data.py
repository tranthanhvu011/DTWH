import csv
import os
import time
import signal
import sys
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from sqlalchemy import select

# Adjust the path for db_config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import DbConfig

class CrawlData:
    def __init__(self):
        # Initialize database and configurations
        self.db_config = DbConfig()
        self.engine = create_engine('mysql+pymysql://root:@localhost/group11?charset=utf8mb4', echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.config_table, self.logs_table = self._define_tables()
        
        # Create tables and insert configuration
        self.metadata.create_all(self.engine)
        self.insert_config()
        
        # Load configuration data
        self.config = self.get_config_data()
        
        # Initialize Selenium WebDriver
        self.driver = self._init_driver()
        
        # Register signals for graceful termination
        self._register_signals()

    def _define_tables(self):
        # Define config and logs tables
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
            Column('config_id', Integer),
            Column('timestamp', String(50)),
            Column('action', String(50)),
            Column('details', String(500)),
            Column('process', String(50)),
            Column('status', String(50))
        )
        
        return config_table, logs_table

    def _init_driver(self):
        # Initialize Chrome WebDriver with options
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        return webdriver.Chrome(service=Service(self.db_config.chromedriver_path), options=chrome_options)

    def _register_signals(self):
        # Register signal handlers for graceful exit
        signal.signal(signal.SIGINT, self._handle_termination_signal)
        signal.signal(signal.SIGTERM, self._handle_termination_signal)

    def get_config_data(self):
        # Retrieve config data from the config table
        with self.engine.connect() as conn:
            result = conn.execute(select(self.config_table)).mappings().fetchone()
            if result:
                return dict(result)
            else:
                raise ValueError("Configuration not found in the config table.")

    def insert_config(self): 
        # Insert config data only if it doesn't already exist
        with self.engine.connect() as conn:
            result = conn.execute(select(self.config_table.c.id)).fetchone()
            if not result:
                conn.execute(self.config_table.insert().values(
                    data_dir=self.db_config.data_dir,
                    products_csv=self.db_config.products_csv,
                    specifications_csv=self.db_config.specifications_csv,
                    images_csv=self.db_config.images_csv,
                    category_url=self.db_config.category_url,
                    db_connection=self.db_config.db_connection,
                    chromedriver_path=self.db_config.chromedriver_path,
                    user=self.db_config.db_user
                ))
                conn.commit()  # Commit after insertion

    def _handle_termination_signal(self, signal_number, frame):
        # Handle process termination gracefully
        self.write_log("End Crawl", "Unexpected crawl termination", "Failed")
        self.driver.quit()
        print("Crawl was stopped abruptly.")
        sys.exit(1)

    def get_config_id(self, user):
        # Get config ID for the specified user
        with self.engine.connect() as conn:
            result = conn.execute(
                select(self.config_table.c.id).where(self.config_table.c.user == user)
            ).mappings().fetchone()
            return result['id'] if result else None

    def write_log(self, action, details, status="Success"):
        # Write a log entry into the logs table
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        new_log = self.logs_table.insert().values(
            config_id=self.get_config_id(self.db_config.db_user),
            timestamp=timestamp,
            action=action,
            details=details,
            process='crawl',
            status=status
        )
        with self.engine.begin() as conn:
            conn.execute(new_log)

    def start_crawling(self):
        # Start the crawling process
        try:
            self.write_log("Start Crawl", "Starting crawl")
            self.driver.get(self.config['category_url'])
            time.sleep(5)
            product_links = [elem.get_attribute('href') for elem in self.driver.find_elements(By.CSS_SELECTOR, '.proloop .proloop-name a')]
            products, specs, images = self._crawl_products(product_links)
            self._save_to_csv(products, specs, images)
        except Exception as e:
            self.write_log("Crawl Failed", str(e), "Failed")
        finally:
            self.write_log("End Crawl", "Crawl completed")
            self.driver.quit()

    def _crawl_products(self, product_links):
        # Crawl product details
        products, specs, images = [], [], []
        new_products = 0
        for link in product_links:
            self.driver.get(link)
            time.sleep(3)
            product_name = self.driver.find_element(By.CSS_SELECTOR, '.product-name h1').text
            if self.product_exists(product_name):
                continue
            price, discounted_price, discount_percent, thumb_image = self._get_product_info()
            product_id = new_products + 1
            products.append([product_id, product_name, price, discounted_price, discount_percent, thumb_image])
            new_products += 1
            specs += self._get_specifications(product_id)
            images += self._get_images(product_id)
        return products, specs, images

    def _get_product_info(self):
        # Extract product information
        try:
            price = self._clean_price(self.driver.find_element(By.CSS_SELECTOR, '.product-price .pro-price').text)
            discounted_price = self._clean_price(self.driver.find_element(By.CSS_SELECTOR, '.product-price del').text)
            discount_percent = self._clean_discount(self.driver.find_element(By.CSS_SELECTOR, '.product-price .pro-percent').text)
            thumb_image = self.driver.find_element(By.CSS_SELECTOR, '.img-default').get_attribute('src')
        except:
            price, discounted_price, discount_percent, thumb_image = None, None, None, None
        return price, discounted_price, discount_percent, thumb_image

    def _get_specifications(self, product_id):
        # Get specifications from product page
        specs = []
        try:
            specs_table = self.driver.find_element(By.ID, 'tblGeneralAttribute')
            for row in specs_table.find_elements(By.TAG_NAME, 'tr'):
                columns = row.find_elements(By.TAG_NAME, 'td')
                if len(columns) == 2:
                    specs.append([product_id, columns[0].text, columns[1].text])
        except:
            pass
        return specs

    def _get_images(self, product_id):
        # Get additional images for the product
        images = []
        try:
            for image in self.driver.find_elements(By.CSS_SELECTOR, '.product-gallery--photo a'):
                images.append([product_id, image.get_attribute('href')])
        except:
            pass
        return images

    def _save_to_csv(self, products, specs, images):
        # Save data to CSV files
        data_dir = self.config['data_dir']
        os.makedirs(data_dir, exist_ok=True)
        self._write_csv(os.path.join(data_dir, self.config['products_csv']), products, ['id', 'product_name', 'price', 'discounted_price', 'discount_percent', 'thumb_image'])
        self._write_csv(os.path.join(data_dir, self.config['specifications_csv']), specs, ['product_id', 'spec_name', 'spec_value'])
        self._write_csv(os.path.join(data_dir, self.config['images_csv']), images, ['product_id', 'image_url'])
        self.write_log("Crawl Completed", f"Added {len(products)} new products")

    def _write_csv(self, file_path, data, headers):
        # Write data to CSV with headers
        with open(file_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            if os.stat(file_path).st_size == 0:
                writer.writerow(headers)
            writer.writerows(data)

    def product_exists(self, product_name):
        # Check if product already exists in products CSV
        products_csv = os.path.join(self.config['data_dir'], self.config['products_csv'])
        if not os.path.exists(products_csv):
            return False
        with open(products_csv, mode='r', encoding='utf-8') as file:
            return any(row['product_name'] == product_name for row in csv.DictReader(file))

    def _clean_price(self, price_str):
        return self._clean_numeric_string(price_str, '₫')

    def _clean_discount(self, discount_str):
        return self._clean_numeric_string(discount_str, '%')

    @staticmethod
    def _clean_numeric_string(string, symbol):
        if string:
            string = string.replace(symbol, '').replace('.', '').replace(',', '.')
            try:
                return float(string)
            except ValueError:
                pass
        return None
