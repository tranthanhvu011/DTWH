import os
import sys
import time
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Text, MetaData, ForeignKey, Date, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import DbConfig

class LoadDataMart:
    def __init__(self):
        self.start_load_datamart()
    
    def connect_control(self):
        self.db_config = DbConfig()
        self.engine = create_engine(self.db_config.db_connection, echo=True)
        self.Session = sessionmaker(bind=self.engine)
        self.metadata = MetaData()
        self.metadata.create_all(self.engine)
    
    def check_connect(self, connect):
        try:
            connect.connection()
            return True
        except OperationalError:
            return False
    
    def get_connection_to_datamart(self, config_id, control_session):
        config_table = Table('config', self.metadata, autoload_with=self.engine)
        result = control_session.query(config_table).filter(config_table.c.id == config_id).first()
        
        if result:
            db_connection_string = result[6]  
            engine_datamart = create_engine(db_connection_string)
            SessionDataMart = sessionmaker(bind=engine_datamart)
            datamart_session = SessionDataMart() 
            return datamart_session 
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
            process='datamart',
            status=status,
        ))
        conn.commit()

    def check_warehouse_data(self, conn):
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
            logs_table.c.timestamp.like(time.strftime('%Y-%m-%d') + '%'),
            logs_table.c.status == 'Success'
        )

        result = conn.execute(stmt).fetchall()
        return bool(result)

    def check_datamart_data(self, conn):
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
            logs_table.c.action == 'Load data to DataMart',
            logs_table.c.timestamp.like(time.strftime('%Y-%m-%d') + '%'),
            logs_table.c.status == 'Success'
        )

        result = conn.execute(stmt).fetchall()
        return bool(result)

    def insert_data_to_datamart(self, conn_control, conn_warehouse, conn_datamart):
        # Lấy bảng warehouse
        warehouse_products_table = Table('products', self.metadata, autoload_with=conn_warehouse)
        warehouse_images_table = Table('images', self.metadata, autoload_with=conn_warehouse)
        warehouse_specifications_table = Table('specifications', self.metadata, autoload_with=conn_warehouse)
        
        # Lấy bảng datamart
        datamart_products_table = Table('product_mart', self.metadata, autoload_with=conn_datamart)
        
        # Lấy dữ liệu từ warehouse
        stmt_products = select(
            warehouse_products_table.c.id,
            warehouse_products_table.c.product_name,
            warehouse_products_table.c.price,
            warehouse_products_table.c.discounted_price,
            warehouse_products_table.c.discount_percent,
            warehouse_products_table.c.thumb_image,
            warehouse_products_table.c.sk
        ).select_from(warehouse_products_table)
        
        products_data = conn_warehouse.execute(stmt_products).fetchall()
        
        count_product = 0
        for product in products_data:
            # Kiểm tra xem sản phẩm đã tồn tại trong datamart chưa
            check_stmt = select(datamart_products_table.c.id).where(
                (datamart_products_table.c.id == product[0]) & 
                (datamart_products_table.c.sk == product[6])
            )
            existing_product = conn_datamart.execute(check_stmt).fetchone()
            
            if not existing_product:
                # Lấy thông tin chi tiết và specs
                stmt_images = select(
                    warehouse_images_table.c.image_url
                ).where(warehouse_images_table.c.product_id == product[0])
                images_data = conn_warehouse.execute(stmt_images).fetchall()
                
                stmt_specs = select(
                    warehouse_specifications_table.c.spec_name,
                    warehouse_specifications_table.c.spec_value
                ).where(warehouse_specifications_table.c.product_id == product[0])
                specs_data = conn_warehouse.execute(stmt_specs).fetchall()
                
                # Chuẩn bị dữ liệu để insert vào datamart
                product_details = {
                    'id': product[0],
                    'product_name': product[1],
                    'price': product[2],
                    'discounted_price': product[3],
                    'discount_percent': product[4],
                    'thumb_image': product[5],
                    'sk': product[6],
                    'images': [img[0] for img in images_data],
                    'specifications': {spec[0]: spec[1] for spec in specs_data}
                }
                
                # Insert vào datamart
                insert_stmt = datamart_products_table.insert().values(**product_details)
                conn_datamart.execute(insert_stmt)
                count_product += 1
        
        conn_datamart.commit()
        return count_product

    def start_load_datamart(self):
        # Bước 1. Kết nối Database Control
        self.connect_control()
        control_session = self.Session()
        
        # Bước 2. Kiểm tra kết nối Control
        if not self.check_connect(control_session):
            self.write_log(
                conn=control_session.connection(), 
                action="Load data to DataMart", 
                details="Failed to connect to Control", 
                status="Error", 
                config_id=3  # Giả sử config_id cho DataMart là 3
            )
            return
        
        # Bước 3. Kiểm tra dữ liệu trong Warehouse
        if not self.check_warehouse_data(conn=control_session.connection()):
            print("Data is not available in Warehouse")
            return
        
        # Bước 4. Kiểm tra dữ liệu đã được load vào DataMart chưa
        if self.check_datamart_data(conn=control_session.connection()):
            print("Data has been loaded to DataMart")
            return
        
        # Bước 5. Kết nối đến Warehouse
        warehouse_session = self.get_connection_to_warehouse(config_id=2, control_session=control_session)
        
        # Bước 6. Kết nối đến DataMart
        datamart_session = self.get_connection_to_datamart(config_id=3, control_session=control_session)
        
        # Bước 7. Kiểm tra kết nối Warehouse và DataMart
        if not self.check_connect(warehouse_session) or not self.check_connect(datamart_session):
            self.write_log(
                conn=control_session.connection(), 
                action="Load data to DataMart", 
                details="Failed to connect to Warehouse or DataMart", 
                status="Error", 
                config_id=3
            )
            return
        
        # Bước 8. Load dữ liệu từ Warehouse sang DataMart
        total_product = self.insert_data_to_datamart(
            conn_control=control_session.connection(), 
            conn_warehouse=warehouse_session.connection(), 
            conn_datamart=datamart_session.connection()
        )
        
        # Bước 9. Ghi log
        self.write_log(
            conn=control_session.connection(), 
            action="Load data to DataMart", 
            details=f"Load data from Warehouse to DataMart: {total_product} products have been added.", 
            status="Success", 
            config_id=3
        )
        
        # Bước 10. Đóng các kết nối
        control_session.close()
        warehouse_session.close()
        datamart_session.close()

if __name__ == "__main__":
    datamart_loader = LoadDataMart()