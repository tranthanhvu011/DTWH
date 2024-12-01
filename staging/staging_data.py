import csv
import os
import sys
import time
import shutil
import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import Control

class StagingData:
    def __init__(self):
        self.control = Control()
        self.control.set_process("staging")
        self.db_config = self.control.db_config
        
        self.engine_staging = create_engine(self.db_config.db_connection_staging, echo=True)
        self.Session = sessionmaker(bind=self.engine_staging)
        self.metadata = MetaData()
        self.products_table, self.images_table, self.specifications_table = self._define_tables()
        self.metadata.create_all(self.engine_staging)

    def _define_tables(self):
        products_table = Table('products', self.metadata,
                               Column('id', Integer, primary_key=True, autoincrement=True),
                               Column('product_name', String(255), unique=True),
                               Column('price', Float),
                               Column('discounted_price', Float),
                               Column('discount_percent', Float),
                               Column('thumb_image', String(255))
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
        
        return products_table, images_table, specifications_table

    def check_files_exist(self, files):
        missing_files = [file for file in files if not os.path.exists(file)]
        return missing_files

    def staging_data(self):
        data_dir = self.db_config.data_dir
        products_csv = os.path.join(data_dir, self.db_config.products_csv)
        images_csv = os.path.join(data_dir, self.db_config.images_csv)
        specifications_csv = os.path.join(data_dir, self.db_config.specifications_csv)

        history_dir = os.path.join(data_dir, 'history')
        if not os.path.exists(history_dir):
            os.makedirs(history_dir)

        try:
            files = [products_csv, images_csv, specifications_csv]
            missing_files = self.check_files_exist(files)
            if missing_files:
                error_message = f"Missing files: {', '.join(missing_files)}"
                print(error_message)
                self.control.write_log("File Check", error_message, "Error")
                return

            self.control.write_log("Start Staging", "Starting the staging process", "In Progress")

            records = self.read_all_csv(products_csv, images_csv, specifications_csv)
            inserted, updated, errors = self.process_staging_data(records)

            self.control.write_log("Staging Summary",
                                   f"Inserted: {inserted}, Updated: {updated}, Errors: {errors}",
                                   "Completed" if errors == 0 else "Partial Success")

            self.move_file_to_history(products_csv)
            self.move_file_to_history(images_csv)
            self.move_file_to_history(specifications_csv)

        except Exception as e:
            error_message = f"Error during staging process: {str(e)}"
            print(error_message)
            self.control.write_log("Error", error_message, "Error")

        finally:
            self.control.write_log("End Staging", "Staging process completed", "Completed")

    def move_file_to_history(self, file_path):
        if os.path.exists(file_path):
            current_date = datetime.datetime.now().strftime('%Y%m%d')
            filename = os.path.basename(file_path)
            new_filename = f"{filename.split('.')[0]}_{current_date}.csv"
            new_file_path = os.path.join(os.path.dirname(file_path), 'history', new_filename)

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

    def process_staging_data(self, records):
        inserted, updated, errors = 0, 0, 0 

        with self.engine_staging.begin() as conn:
            for category, rows in records.items():
                for row in rows:
                    try:
                        if category == 'products':
                            result = self.process_product(conn, row)
                        elif category == 'images':
                            result = self.process_image(conn, row)
                        elif category == 'specifications':
                            result = self.process_specification(conn, row)

                        if result == 'inserted':
                            inserted += 1
                        elif result == 'updated':
                            updated += 1

                    except SQLAlchemyError as e:
                        errors += 1
                        self.write_log(f"Insert/Update {category}", f"Failed to insert/update {category} due to {str(e)}", "Failed")
                        print(f"Failed to insert/update {category} due to {str(e)}")

        self.write_log("Staging Summary",
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
            if (existing_product['price'] != price or
                existing_product['discounted_price'] != discounted_price or
                existing_product['discount_percent'] != discount_percent or
                existing_product['thumb_image'] != thumb_image):
                conn.execute(
                    self.products_table.update()
                    .where(self.products_table.c.id == existing_product['id'])
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

    def write_log(self, action, details, status):
        self.control.write_log(action, details, status)  

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
