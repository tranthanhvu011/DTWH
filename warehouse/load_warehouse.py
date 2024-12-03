import os
import sys
import time
from datetime import datetime
from sqlalchemy import PrimaryKeyConstraint, create_engine, Table, Column, Integer, String, Float, Date, ForeignKey, MetaData, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import Control

class LoadWarehouse:
    def __init__(self):
        self.control = Control()
        self.start_load_warehouse()

    def connect_control(self):
        self.engine_control = create_engine(self.control.db_config.db_connection_control, echo=True)
        self.SessionControl = sessionmaker(bind=self.engine_control)
        self.metadata_control = MetaData()
        self.metadata_control.create_all(self.engine_control)

    def connect_staging(self):
        self.engine_staging = create_engine(self.control.db_config.db_connection_staging, echo=True)
        self.SessionStaging = sessionmaker(bind=self.engine_staging)
        self.metadata_staging = MetaData()
        self.metadata_staging.create_all(self.engine_staging)

    def connect_warehouse(self):
        self.engine_warehouse = create_engine(self.control.db_config.db_connection_warehouse, echo=True)
        self.SessionWarehouse = sessionmaker(bind=self.engine_warehouse)
        self.metadata_warehouse = MetaData()
        self.metadata_warehouse.create_all(self.engine_warehouse)

    def _define_tables(self, engine):
        metadata = MetaData()
        
        # Define products table
        products_table = Table('products', metadata,
            Column('id', Integer, autoincrement=True),
            Column('product_name', String(255)),
            Column('price', Float),
            Column('discounted_price', Float),
            Column('discount_percent', Float),
            Column('thumb_image', String(255)),
            Column('date_update', Integer),
            Column('sk', Integer),
            PrimaryKeyConstraint('id', 'sk')
        )
        metadata.create_all(engine)

        # Define dim_dates table
        dim_date = Table('dim_dates', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('full_date', Date),
            Column('day', Integer),
            Column('month', Integer),
            Column('year', Integer),
            Column('month_year', String(255)),
            Column('week_of_year', Integer),
            Column('day_name', String(255)),
        )
        metadata.create_all(engine)

        # Define images table
        images_table = Table('images', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('product_id', Integer, ForeignKey('products.id')),
            Column('image_url', String(255)),
        )
        metadata.create_all(engine)

        # Define specifications table
        specifications_table = Table('specifications', metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('product_id', Integer, ForeignKey('products.id')),
            Column('spec_name', String(255)),
            Column('spec_value', String(255)),
        )
        metadata.create_all(engine)
        
        return products_table, images_table, specifications_table

    def check_connect(self, session):
        try:
            session.connection()
            return True
        except OperationalError:
            return False

    def write_log(self, action, details, status):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        self.control.write_log(action, details, status)  

    def get_connection_to_warehouse(self):
        engine_warehouse = create_engine(self.control.db_config.db_connection_warehouse)
        self._define_tables(engine_warehouse)
        SessionWarehouse = sessionmaker(bind=engine_warehouse)
        warehouse_session = SessionWarehouse() 
        return warehouse_session 
    
    def check_staging_data(self, conn):
        logs_table = Table('logs', self.metadata_control, autoload_with=conn)
        stmt = select(
            logs_table.c.id,
            logs_table.c.config_id,
            logs_table.c.timestamp,
            logs_table.c.action,
            logs_table.c.details,
            logs_table.c.process,
            logs_table.c.status
        ).where(
            logs_table.c.action == 'End Staging',
            logs_table.c.timestamp.like(time.strftime('%Y-%m-%d') + '%'),  # So sánh ngày hiện tại
            logs_table.c.status == 'Completed'
        )

        result = conn.execute(stmt).fetchall()
        return bool(result)

    def check_data_warehouse(self, conn):
        logs_table = Table('logs', self.metadata_control, autoload_with=conn)
        stmt = select(
            logs_table.c.id,
            logs_table.c.config_id,
            logs_table.c.timestamp,
            logs_table.c.action,
            logs_table.c.details,
            logs_table.c.process,
            logs_table.c.status
        ).where(
            logs_table.c.action == 'Load data to Warehouse',
            logs_table.c.timestamp.like(time.strftime('%Y-%m-%d') + '%'),  # So sánh ngày hiện tại
            logs_table.c.status == 'Success'
        )

        result = conn.execute(stmt).fetchall()
        return bool(result)

    def insert_current_date_into_dim_dates(self, conn_warehouse, dim_date_table):
        current_time = datetime.now()
    
        check_stmt = select(dim_date_table.c.id).where(dim_date_table.c.full_date == current_time.date())
        existing_date = conn_warehouse.execute(check_stmt).fetchone()

        if existing_date:
            return existing_date[0]
        else:
            insert_stmt = dim_date_table.insert().values(
                full_date=current_time,
                day=current_time.day,
                month=current_time.month,
                year=current_time.year,
                month_year=f"{current_time.month}-{current_time.year}",
                week_of_year=current_time.isocalendar()[1],  
                day_name=current_time.strftime('%A')        
            )

            result = conn_warehouse.execute(insert_stmt)
            conn_warehouse.commit()
        
            inserted_id = result.inserted_primary_key[0]
            return inserted_id

    def insert_data_to_warehouse(self, conn_control, conn_warehouse):
        dim_date = Table('dim_dates', self.metadata_control, autoload_with=conn_warehouse)
        warehouse_products_table = Table('products', self.metadata_control, autoload_with=conn_warehouse)
        warehouse_images_table = Table('images', self.metadata_control, autoload_with=conn_warehouse)
        warehouse_specifications_table = Table('specifications', self.metadata_control, autoload_with=conn_warehouse)
        id_dim_date = self.insert_current_date_into_dim_dates(conn_warehouse, dim_date)

        products_table = Table('products', self.metadata_control, autoload_with=conn_control)
        images_table = Table('images', self.metadata_control, autoload_with=conn_control)
        specifications_table = Table('specifications', self.metadata_control, autoload_with=conn_control)
        
        stmt_products = select(
            products_table.c.id, 
            products_table.c.product_name, 
            products_table.c.price, 
            products_table.c.discounted_price, 
            products_table.c.discount_percent, 
            products_table.c.thumb_image
        )
        products_data = conn_control.execute(stmt_products).fetchall()
        
        count_product = 0
        for product in products_data:
            check_stmt = select(warehouse_products_table.c.id, warehouse_products_table.c.price, func.coalesce(func.max(warehouse_products_table.c.sk), 0)
                                ).where(warehouse_products_table.c.product_name == product[1]
                                        ).group_by(warehouse_products_table.c.id)
            existing_products = conn_warehouse.execute(check_stmt).fetchall()

            stmt_images = select(
                images_table.c.id, 
                images_table.c.product_id, 
                images_table.c.image_url
            ).where(
                images_table.c.product_id == product[0] 
            )

            images_data = conn_control.execute(stmt_images).fetchall()

            stmt_specifications = select(
                specifications_table.c.id, 
                specifications_table.c.product_id, 
                specifications_table.c.spec_name, 
                specifications_table.c.spec_value
            ).where(
                specifications_table.c.product_id == product[0]
            )
            specifications_data = conn_control.execute(stmt_specifications).fetchall()
            # Bước 9.1. Check the data exists.
            if existing_products:
                # Bước 9.3. Check product has changed price
                if product[2] != existing_products[0][2]:
                    product_id = existing_products[0][0]
                    # Bước 9.4 Insert data to Warehouse with sk increase 1
                    sk = existing_products[0][2] + 1
                    insert_stmt = warehouse_products_table.insert().values(
                        id=product_id,
                        product_name=product[1],
                        price=product[2],
                        discounted_price=product[3],
                        discount_percent=product[4],
                        thumb_image=product[5],
                        sk=sk,
                        date_update=id_dim_date,
                    )
                    conn_warehouse.execute(insert_stmt)
            else:
                # Bước 9.2 Insert data to Warehouse
                insert_stmt = warehouse_products_table.insert().values(
                    product_name= product[1],
                    price=product[2],
                    discounted_price=product[3],
                    discount_percent=product[4],
                    thumb_image=product[5],
                    sk=1,
                    date_update=id_dim_date,
                )
                result = conn_warehouse.execute(insert_stmt)
                product_id = result.inserted_primary_key[0]
                
                for images in images_data:
                    insert_stmt = warehouse_images_table.insert().values(
                        product_id=product_id,
                        image_url=images[2],
                    )
                    conn_warehouse.execute(insert_stmt)
                for specifications in specifications_data:
                    insert_stmt = warehouse_specifications_table.insert().values(
                        product_id=product_id,
                        spec_name=specifications[2],
                        spec_value=specifications[3],
                    )
                    conn_warehouse.execute(insert_stmt)
            count_product += 1
        conn_warehouse.commit()
        return count_product

    def start_load_warehouse(self):
        # Bước 1. Connect Database Control
        self.connect_control()
        control_session = self.SessionControl()

        # Bước 2. Check the connection to database Control
        if not self.check_connect(control_session):
            # Bước 2.1. Insert new row into logs table.
            self.write_log(action="Load data to Warehouse", details="Failed to connect to Control", status="Error")
            return

        # Bước 3. Check data is available in Staging 
        if not self.check_staging_data(control_session.connection()):
            print("Data is not available in Staging.")
            self.write_log(action="Load data to Warehouse", details="No data available in Staging", status="Warning")
            return

        # Bước 4. Check if data has been loaded from Staging to Warehouse
        if self.check_data_warehouse(control_session.connection()):
            print("Data has already been loaded from Staging to Warehouse.")
            self.write_log(action="Load data to Warehouse", details="Data has already been loaded from Staging to Warehouse", status="Info")
            return

        # Bước 5. Connect to the Staging database
        self.connect_staging()
        staging_session = self.SessionStaging()

        # Bước 6. Check the connection to database Staging
        if staging_session is None:
            # Bước 6.1. Insert new row into logs table.
            self.write_log(action="Load data to Warehouse", details="Failed to get Staging connection", status="Error")
            return
        
        # Bước 7. Connect database Warehouse
        data_warehouse_session = self.get_connection_to_warehouse()

        # Bước 8. Check the connection to the Warehouse database
        if not self.check_connect(data_warehouse_session):
            # Bước 7.1. Insert new row into logs table.
            self.write_log(action="Load data to Warehouse", details="Failed to connect to Warehouse", status="Error")
            return

        # Bước 9. Load data from Staging to Warehouse
        total_product = self.insert_data_to_warehouse(conn_control=staging_session.connection(), conn_warehouse=data_warehouse_session.connection())

        # Bước 10. Insert new row into logs table  
        self.write_log(action="Load data to Warehouse", details=f"Load data from Staging to Warehouse: {total_product} products have been added.", status="Success")

        # Bước 11. Close all connections to the database
        control_session.close()
        data_warehouse_session.close()
        staging_session.close()

        print(f"Successfully loaded {total_product} products from Staging to Warehouse.")
if __name__ == "__main__":
    warehouse = LoadWarehouse()
    