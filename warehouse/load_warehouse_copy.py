import csv
import os
import sys
import time
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Text, MetaData, ForeignKey, Date, func, PrimaryKeyConstraint,DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.exc import OperationalError
from sqlalchemy import select
from db_config import DbConfig

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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

        try:
            
            dim_date = Table('dim_dates', metadata, autoload_with=engine, autoload=True)
        except Exception as e:
            
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

        try:
            
            images_table = Table('images', metadata, autoload_with=engine, autoload=True)
        except Exception as e:
            
            images_table = Table('images', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('product_id', Integer, ForeignKey('products.id')),
                Column('image_url', String(255)),
            )
            metadata.create_all(engine)

        try:
            
            specifications_table = Table('specifications', metadata, autoload_with=engine, autoload=True)
        except Exception as e:

            specifications_table = Table('specifications', metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('product_id', Integer, ForeignKey('products.id')),
                Column('spec_name', String(255)),
                Column('spec_value', String(255)),
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

        dim_date = Table('dim_dates', self.metadata, autoload_with=conn_warehouse)
        warehouse_products_table = Table('products', self.metadata, autoload_with=conn_warehouse)
        warehouse_images_table = Table('images', self.metadata, autoload_with=conn_warehouse)
        warehouse_specifications_table = Table('specifications', self.metadata, autoload_with=conn_warehouse)
        id_dim_date = self.insert_current_date_into_dim_dates(conn_warehouse, dim_date)

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
        
        count_product = 0
        for product in products_data:
            # Bước 9.1. Check the data exists.
            check_stmt = select(warehouse_products_table.c.id, warehouse_products_table.c.price, func.coalesce(func.max(warehouse_products_table.c.sk), 0)
                                ).where(warehouse_products_table.c.product_name == product[1]
                                        ).group_by(warehouse_products_table.c.id)
            existing_products = conn_warehouse.execute(check_stmt).fetchall()

            # Lấy dữ liệu từ bảng images theo product_id
            stmt_images = select(
                images_table.c.id, 
                images_table.c.product_id, 
                images_table.c.image_url
            ).where(
                images_table.c.product_id == product[0] 
            )

            images_data = conn_control.execute(stmt_images).fetchall()

            # Lấy dữ liệu từ bảng specifications theo product_id
            stmt_specifications = select(
                specifications_table.c.id, 
                specifications_table.c.product_id, 
                specifications_table.c.spec_name, 
                specifications_table.c.spec_value
            ).where(
                specifications_table.c.product_id == product[0]
            )
            specifications_data = conn_control.execute(stmt_specifications).fetchall()

            if existing_products:
                # Bước 9.3 Check product has changed price
                if product[2] != existing_products[0][2]:
                    # Bước 9.4 Insert data to Warehouse with sk increase 1
                    product_id = existing_products[0][0]
                    sk = existing_products[0][2] + 1
                    insert_stmt = warehouse_products_table.insert().values(
                    id = product_id,
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
                    product_name=product[1],
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
                        product_id = product_id,
                        image_url = images[2],
                    )
                    conn_warehouse.execute(insert_stmt)
                for specifications in specifications_data:
                    insert_stmt = warehouse_specifications_table.insert().values(
                        product_id = product_id,
                        spec_name = specifications[2],
                        spec_value = specifications[3],
                    )
                    conn_warehouse.execute(insert_stmt)
            count_product+=1
        conn_warehouse.commit()
        return count_product

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
            print("data is not avaible in Staging ")
            return
        
        # Bước 4. Check data has been loaded from Staging to Warehouse
        if (self.check_data_warehouse(conn=control_session.connection())):
            print("Data has been loaded from Staging to Warehouse")
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
        total_product = self.insert_data_to_warehouse(conn_control=control_session.connection(), conn_warehouse= data_warehouse_session.connection())

        # Bước 10. Insert new row table logs  
        self.write_log(conn=control_session.connection(), action="Load data to Warehouse", details=f"Load data from Staging to Warehouse: {total_product} products have been added.", status="Success", config_id=2)

        # Bước 11. Close all connect to database
        control_session.close()
        data_warehouse_session.close()
if __name__ == "__main__":
    warehouse = LoadWarehouse()