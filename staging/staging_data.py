import csv
import os
import sys
import time
import shutil
import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Text, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import DbConfig

class StagingData:
    def __init__(self):
        self.db_config = DbConfig()
        self.engine = create_engine(self.db_config.db_connection, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.products_table, self.logs_table, self.config_table, self.images_table, self.specifications_table = self._define_tables()
        self.metadata.create_all(self.engine)

    def _define_tables(self):
        products_table = Table('products', self.metadata,
                               Column('id', Integer, primary_key=True, autoincrement=True),
                               Column('product_name', String(255), unique=True),
                               Column('price', Float),
                               Column('discounted_price', Float),
                               Column('discount_percent', Float),
                               Column('thumb_image', String(255))
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

        images_table = Table('images', self.metadata,
                             Column('id', Integer, primary_key=True, autoincrement=True),
                             Column('product_id', Integer, ForeignKey('products.id')),
                             Column('image_url', String(255))
                            )

        specifications_table = Table('specifications', self.metadata,
                                     Column('id', Integer, primary_key=True, autoincrement=True),
                                     Column('product_id', Integer, ForeignKey('products.id')),
                                     Column('spec_name', String(255)),
                                     Column('spec_value', String(255))
                                    )
        
        return products_table, logs_table, config_table, images_table, specifications_table

    def get_config_id(self, user):
        with self.engine.begin() as conn:
            result = conn.execute(
                select(self.config_table.c.id)
                .where(self.config_table.c.user == user)
            ).mappings().fetchone()
        return result['id'] if result else 1

    def staging_data(self):
        data_dir = self.db_config.data_dir
        products_csv = os.path.join(data_dir, self.db_config.products_csv)
        images_csv = os.path.join(data_dir, self.db_config.images_csv)
        specifications_csv = os.path.join(data_dir, self.db_config.specifications_csv)

        # Thư mục history (tạo thư mục nếu chưa có)
        history_dir = os.path.join(data_dir, 'history')
        if not os.path.exists(history_dir):
            os.makedirs(history_dir)

        with self.engine.begin() as conn:
            # Start staging log
            self.write_log(conn, "Start Staging", "Starting the staging process", "In Progress")

            # Process all data types (products, images, specifications) in one unified function
            records = self.read_all_csv(products_csv, images_csv, specifications_csv)
            inserted, updated, errors = self.process_staging_data(conn, records)

            # Log staging summary
            self.write_log(conn, "Staging Summary",
                        f"Inserted: {inserted}, Updated: {updated}, Errors: {errors}",
                        "Completed" if errors == 0 else "Partial Success")

            # Di chuyển các file CSV đã staging thành công vào thư mục history
            self.move_file_to_history(products_csv)
            self.move_file_to_history(images_csv)
            self.move_file_to_history(specifications_csv)

            # End staging log
            self.write_log(conn, "End Staging", "Staging process completed", "Completed")

    def move_file_to_history(self, file_path):
        if os.path.exists(file_path):
            # Lấy ngày hiện tại để thêm vào tên file
            current_date = datetime.datetime.now().strftime('%Y%m%d')
            filename = os.path.basename(file_path)
            new_filename = f"{filename.split('.')[0]}{current_date}.csv"
            new_file_path = os.path.join(os.path.dirname(file_path), 'history', new_filename)

            # Di chuyển và đổi tên file
            shutil.move(file_path, new_file_path)
            print(f"File {filename} moved to history with new name {new_filename}")


    def read_all_csv(self, products_csv, images_csv, specifications_csv):
        records = {
            'products': [],
            'images': [],
            'specifications': []
        }
        
        with open(products_csv, mode='r', encoding='utf-8') as file:
            records['products'] = list(csv.DictReader(file))
        
        with open(images_csv, mode='r', encoding='utf-8') as file:
            records['images'] = list(csv.DictReader(file))
        
        with open(specifications_csv, mode='r', encoding='utf-8') as file:
            records['specifications'] = list(csv.DictReader(file))
        
        return records

    def process_staging_data(self, conn, records):
        inserted, updated, errors = 0, 0, 0 

        for category, rows in records.items():
            for row in rows:
                try:
                    if category == 'products':
                        result = self.process_product(conn, row)
                    elif category == 'images':
                        result = self.process_image(conn, row)
                    elif category == 'specifications':
                        result = self.process_specification(conn, row)

                    # Handle result to check if there's a new insert, update, or no action
                    if result == 'inserted':
                        inserted += 1  # Tăng số lượng bản ghi được insert
                    elif result == 'updated':
                        updated += 1  # Tăng số lượng bản ghi được update

                except SQLAlchemyError as e:
                    errors += 1  # Tăng số lượng bản ghi bị lỗi
                    self.write_log(conn, f"Insert/Update {category}", f"Failed to insert/update {category} due to {str(e)}", "Failed")
                    print(f"Failed to insert/update {category} due to {str(e)}")

        # Ghi log chỉ số lượng bản ghi đã insert, update và errors
        self.write_log(conn, "Staging Summary",
                    f"Inserted: {inserted}, Updated: {updated}, Errors: {errors}",
                    "Completed" if errors == 0 else "Partial Success")

        return inserted, updated, errors 


    def process_product(self, conn, product):
        price = self._convert_to_float(product['price'])
        discounted_price = self._convert_to_float(product['discounted_price'])
        discount_percent = self._convert_to_float(product['discount_percent'])
        thumb_image = product['thumb_image']

        existing_product = conn.execute(
            self.products_table.select().where(self.products_table.c.product_name == product['product_name'])
        ).mappings().fetchone()

        if existing_product:
            # Check if there are any changes to update
            if (existing_product['price'] != price or
                existing_product['discounted_price'] != discounted_price or
                existing_product['discount_percent'] != discount_percent or
                existing_product['thumb_image'] != thumb_image):
                conn.execute(
                    self.products_table.update()
                    .where(self.products_table.c.id == existing_product['id'])  # Using id for update
                    .values(
                        price=price,
                        discounted_price=discounted_price,
                        discount_percent=discount_percent,
                        thumb_image=thumb_image
                    )
                )
                return 'updated'
        else:
            result = conn.execute(self.products_table.insert().values(
                product_name=product['product_name'],
                price=price,
                discounted_price=discounted_price,
                discount_percent=discount_percent,
                thumb_image=thumb_image
            ))
            return 'inserted'

    def process_image(self, conn, image):
        product_id = self._convert_to_int(image['product_id'])
        image_url = image['image_url']

        existing_images = conn.execute(
            self.images_table.select().where(self.images_table.c.product_id == product_id)
        ).mappings().fetchall()

        for existing in existing_images:
            if existing['image_url'] != image_url:
                conn.execute(
                    self.images_table.update()
                    .where(self.images_table.c.product_id == product_id)
                    .values(image_url=image_url)
                )
                return 'updated'

        # If no existing image matched, insert a new image
        result = conn.execute(self.images_table.insert().values(
            product_id=product_id,
            image_url=image_url
        ))
        return 'inserted' if result.inserted_primary_key else None

    def process_specification(self, conn, spec):
        product_id = self._convert_to_int(spec['product_id'])
        spec_name = spec['spec_name']
        spec_value = spec['spec_value']

        existing_spec = conn.execute(
            self.specifications_table.select().where(
                self.specifications_table.c.product_id == product_id,
                self.specifications_table.c.spec_name == spec_name
            )
        ).mappings().fetchone()

        if existing_spec:
            if existing_spec['spec_value'] != spec_value:
                conn.execute(
                    self.specifications_table.update()
                    .where(self.specifications_table.c.product_id == product_id)
                    .where(self.specifications_table.c.spec_name == spec_name)
                    .values(spec_value=spec_value)
                )
                return 'updated'
        else:
            result = conn.execute(self.specifications_table.insert().values(
                product_id=product_id,
                spec_name=spec_name,
                spec_value=spec_value
            ))
            return 'inserted' if result.inserted_primary_key else None


    def write_log(self, conn, action, details, status):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        conn.execute(self.logs_table.insert().values(
            config_id=self.get_config_id(self.db_config.db_user),
            timestamp=timestamp,
            action=action,
            details=details,
            process='staging',
            status=status,
        ))

    @staticmethod
    def _convert_to_float(value):
        try:
            return float(value)
        except ValueError:
            return None

    @staticmethod
    def _convert_to_int(value):
        try:
            return int(value)
        except ValueError:
            return None

if __name__ == "__main__":
    staging_data = StagingData()
    staging_data.staging_data()
    
    print("Data successfully staged to the database.")
