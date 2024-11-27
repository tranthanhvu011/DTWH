import csv
import os
import sys
import time
import shutil
import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Text, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import OperationalError
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import DbConfig

class LoadWarehouse:
    def __init__(self):
        self.start_load_warehouse()
    
        
    def connect_control(self):
        self.db_config = DbConfig()
        self.engine = create_engine(self.db_config.db_connection, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.metadata.create_all(self.engine)
    
    def _define_tables(self, engine):
        self.Session = sessionmaker(bind=self.engine)
        metadata = MetaData()
        try:
            
            products_table = Table('products', metadata, autoload_with=engine, autoload=True)
        except Exception as e:
            
            products_table = Table('products', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('product_name', String(255), unique=True),
                Column('price', Float),
                Column('discounted_price', Float),
                Column('discount_percent', Float),
                Column('thumb_image', String(255))
            )
            metadata.create_all(engine)

        try:
            
            images_table = Table('images', metadata, autoload_with=engine, autoload=True)
        except Exception as e:
            
            images_table = Table('images', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('product_id', Integer, ForeignKey('products.id')),
                Column('image_url', String(255))
            )
            metadata.create_all(engine)

        try:
            
            specifications_table = Table('specifications', metadata, autoload_with=engine, autoload=True)
        except Exception as e:

            specifications_table = Table('specifications', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('product_id', Integer, ForeignKey('products.id')),
                Column('spec_name', String(255)),
                Column('spec_value', String(255))
            )
            metadata.create_all(engine)
        
        return products_table, images_table, specifications_table


    def check_connect(self, connect):
        try:
            connect.connection()
            return True
        except OperationalError:
            return False
        
    def get_connection_to_warehouse(self, config_id, control_session):
        config_table = Table('config', self.metadata, autoload_with=self.engine)
        result = control_session.query(config_table).filter(config_table.c.id == config_id).first()
        
        if result:
            db_connection_string = result[6]  
            engine_warehouse = create_engine(db_connection_string)
            self._define_tables(engine_warehouse)
            SessionWarehouse = sessionmaker(bind=engine_warehouse)
            warehouse_session = SessionWarehouse() 
            return warehouse_session 
        else:
            return None
    
    def write_log(self, conn, action, details, status, config_id):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_table = Table('logs', self.metadata, autoload_with=conn)
        conn.execute(log_table.insert().values(
            config_id=config_id,
            timestamp=timestamp,
            action=action,
            details=details,
            process='warehouse',
            status=status,
        ))
        conn.commit()

    def check_staging_data(self, conn):
        logs_table = Table('logs', self.metadata, autoload_with=conn)
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
        if result:
            return True
        else:
            return False

    def check_data_warehouse(self, conn):
        logs_table = Table('logs', self.metadata, autoload_with=conn)
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
        if result:
            return True
        else:
            return False
        
    def insert_data_to_warehouse(self, conn_control, conn_warehouse):
        products_table = Table('products', self.metadata, autoload_with=conn_control)
        images_table = Table('images', self.metadata, autoload_with=conn_control)
        specifications_table = Table('specifications', self.metadata, autoload_with=conn_control)
        

        stmt_products = select(
            products_table.c.id, 
            products_table.c.product_name, 
            products_table.c.price, 
            products_table.c.discounted_price, 
            products_table.c.discount_percent, 
            products_table.c.thumb_image
        )
        products_data = conn_control.execute(stmt_products).fetchall()

        stmt_images = select(
            images_table.c.id, 
            images_table.c.product_id, 
            images_table.c.image_url
        )
        images_data = conn_control.execute(stmt_images).fetchall()

        stmt_specifications = select(
            specifications_table.c.id, 
            specifications_table.c.product_id, 
            specifications_table.c.spec_name, 
            specifications_table.c.spec_value
        )
        specifications_data = conn_control.execute(stmt_specifications).fetchall()

        
        warehouse_products_table = Table('products', self.metadata, autoload_with=conn_warehouse)
        warehouse_images_table = Table('images', self.metadata, autoload_with=conn_warehouse)
        warehouse_specifications_table = Table('specifications', self.metadata, autoload_with=conn_warehouse)

        
        for product in products_data:
            insert_stmt = warehouse_products_table.insert().values(
                product_name=product[1],
                price=product[2],
                discounted_price=product[3],
                discount_percent=product[4],
                thumb_image=product[5]
            )
            conn_warehouse.execute(insert_stmt)
        
    
        for image in images_data:
            insert_stmt = warehouse_images_table.insert().values(
                product_id=image[1],
                image_url=image[2]
            )
            conn_warehouse.execute(insert_stmt)

        for specification in specifications_data:
            insert_stmt = warehouse_specifications_table.insert().values(
                product_id=specification[1],
                spec_name=specification[2],
                spec_value=specification[3]
            )
            conn_warehouse.execute(insert_stmt)
        conn_warehouse.commit()

    def start_load_warehouse(self):
        # Bước 1. Connect Database Control
        self.connect_control()
        control_session = self.Session()
        # Bước 2. Check the connection to database Control
        if(self.check_connect(connect=control_session) == False):
            # Bước 2.1. Insert new row table logs.
            self.write_log(conn=control_session.connection(), action="Load data to Warehouse", details="Failed to connect to Control", status="Error", config_id=2)
            return
        
        # Bước 3. Check data is avaible in Staging 
        if (self.check_staging_data(conn=control_session.connection()) == False):
            print("Check data is avaible in Staging ")
            return
        
        # Bước 4. Check data has been loaded from Staging to Warehouse
        if (self.check_data_warehouse(conn=control_session.connection())):
            print(" Check data has been loaded from Staging to Warehouse")
            return
        
        # Bước 5. 

        # Bước 6. 


        # Bước 7. Connect database Warehouse
        data_warehouse_session = self.get_connection_to_warehouse(config_id=2, control_session= control_session)
        
        # Bước 8. Check the connection to database Warehouse
        if(self.check_connect(connect=data_warehouse_session) == False):
            # Bước 8.1. Insert new row table logs.
            self.write_log(conn=control_session.connection(), action="Load data to Warehouse", details="Failed to connect to Warehouse", status="Error", config_id=2)
            return
        # Bước 9. Load data from Staging to Warehouse
        self.insert_data_to_warehouse(conn_control=control_session.connection(), conn_warehouse= data_warehouse_session.connection())
        self.write_log(conn=control_session.connection(), action="Load data to Warehouse", details="Load data from Staging to Warehouse", status="Success", config_id=2)

if __name__ == "__main__":
    warehouse = LoadWarehouse()