import os
import smtplib
import sys
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from sqlalchemy import DateTime

from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData, func, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy import select

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config import Control

metadata = MetaData()

logs = Table('logs', metadata,
             Column('config_id', Integer),
             Column('timestamp', DateTime),
             Column('action', String(255)),
             Column('details', String(255)),
             Column('process', String(255)),
             Column('status', String(50))
             )
class LoadDataMart:

    def __init__(self):
        self.control = Control()
        self.metadata_datamart = MetaData()  # Define metadata for datamart
        self.metadata_warehouse = MetaData()  # Define metadata for warehouse
        self.connect_control()
        self.connect_staging()
        self.connect_warehouse()
        self.start_load_datamart()

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

    def _define_datamart_tables(self, engine):

        # Define datamart products table
        datamart_products_table = Table('datamart_products', self.metadata_datamart,
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
        datamart_specifications_table = Table('datamart_specifications', self.metadata_datamart,
                                              Column('id', Integer, autoincrement=True),
                                              Column('product_id', Integer, ForeignKey('datamart_products.id')),
                                              Column('spec_name', String(255)),
                                              Column('spec_value', String(255)),
                                              Column('date_update', Integer),
                                              PrimaryKeyConstraint('id')
                                              )

        # Define datamart images table
        datamart_images_table = Table('datamart_images', self.metadata_datamart,
                                      Column('id', Integer, autoincrement=True),
                                      Column('product_id', Integer, ForeignKey('datamart_products.id')),
                                      Column('image_url', String(255)),
                                      Column('date_update', Integer),
                                      PrimaryKeyConstraint('id')
                                      )
        self.metadata_datamart.create_all(engine)
        return datamart_products_table

    def check_connect(self, session):
        try:
            session.connection()
            return True
        except OperationalError:
            return False

    def write_log(self, action, details, status, process="loadDataMart"):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        self.control.write_log(action, details, status, process)

    def get_connection_to_datamart(self):
        engine_datamart = create_engine(self.control.db_config.db_connection_mart)
        self._define_datamart_tables(engine_datamart)
        SessionDataMart = sessionmaker(bind=engine_datamart)
        datamart_session = SessionDataMart()
        return datamart_session

    def insert_data_to_datamart(self, conn_warehouse, conn_datamart):
        # Define the datamart products, specifications, and images tables
        datamart_products_table = Table('datamart_products', self.metadata_datamart, autoload_with=conn_datamart)
        datamart_specifications_table = Table('datamart_specifications', self.metadata_datamart,
                                              autoload_with=conn_datamart)
        datamart_images_table = Table('datamart_images', self.metadata_datamart, autoload_with=conn_datamart)

        warehouse_products_table = Table('products', self.metadata_warehouse, autoload_with=conn_warehouse)
        specifications_table = Table('specifications', self.metadata_warehouse, autoload_with=conn_warehouse)
        images_table = Table('images', self.metadata_warehouse, autoload_with=conn_warehouse)

        # Query to get the product data from the warehouse
        stmt_products = select(
            warehouse_products_table.c.id,
            warehouse_products_table.c.product_name,
            warehouse_products_table.c.price,
            warehouse_products_table.c.discounted_price,
            warehouse_products_table.c.discount_percent,
            warehouse_products_table.c.thumb_image
        )
        products_data = conn_warehouse.execute(stmt_products).fetchall()

        count_product = 0
        for product in products_data:
            # Check if the product already exists in the DataMart based on product_name
            existing_product_stmt = select(datamart_products_table).where(
                datamart_products_table.c.product_name == product[1])
            existing_product = conn_datamart.execute(existing_product_stmt).fetchone()

            if not existing_product:
                # Insert the product into DataMart
                insert_stmt = datamart_products_table.insert().values(
                    product_name=product[1],
                    price=product[2],
                    discounted_price=product[3],
                    discount_percent=product[4],
                    thumb_image=product[5],
                    sk=1,  # Assuming SKU value
                    date_update=datetime.now()
                )
                conn_datamart.execute(insert_stmt)
                count_product += 1

                # Get specifications for the current product from the warehouse
                stmt_specs = select(specifications_table).where(specifications_table.c.product_id == product[0])
                specifications = conn_warehouse.execute(stmt_specs).fetchall()

                for spec in specifications:
                    # Insert the specifications into DataMart
                    insert_spec_stmt = datamart_specifications_table.insert().values(
                        product_id=product[0],  # Ensure the product_id is passed correctly
                        spec_name=spec.spec_name,
                        spec_value=spec.spec_value,
                        date_update=datetime.now()  # Ensure you include a date update timestamp
                    )
                    conn_datamart.execute(insert_spec_stmt)

                # Get images for the current product from the warehouse
                stmt_images = select(images_table).where(images_table.c.product_id == product[0])
                images = conn_warehouse.execute(stmt_images).fetchall()

                for image in images:
                    # Insert the images into DataMart
                    insert_image_stmt = datamart_images_table.insert().values(
                        product_id=product[0],  # Ensure the product_id is passed correctly
                        image_url=image.image_url,
                        date_update=datetime.now()  # Include the date update timestamp
                    )
                    conn_datamart.execute(insert_image_stmt)

        # Commit changes to DataMart
        conn_datamart.commit()
        return count_product

    def get_log_data(self):
        with self.engine_control.connect() as conn:
            result = conn.execute(
                select(logs.c.config_id, logs.c.timestamp, logs.c.action, logs.c.details, logs.c.process, logs.c.status)
                .order_by(logs.c.timestamp.desc())  # Order by timestamp in descending order
                .limit(1)
            ).fetchall()
            return result

    def send_email(self, subject, body, recipients):
        sender_email = "vutranorhilsun@gmail.com"
        sender_password = "cywnnoigwmgffutd"

        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipients)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email, sender_password)
            text = msg.as_string()
            server.sendmail(sender_email, recipients, text)
            server.quit()
            print(f"Email sent to: {', '.join(recipients)}")
        except Exception as e:
            print(f"Failed to send email: {e}")

    def generate_log_email_body(self, log_data):
        body = "<h2>Load DataMart Log Details</h2>"
        body += "<table border='1'><tr><th>Config ID</th><th>Timestamp</th><th>Action</th><th>Details</th><th>Process</th><th>Status</th></tr>"

        for row in log_data:
            body += f"<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td><td>{row[4]}</td><td>{row[5]}</td></tr>"

        body += "</table>"
        return body

    def start_load_datamart(self):
        # Step 1: Connect to Control Database
        self.connect_control()
        control_session = self.SessionControl()

        # Step 2: Check the connection to database Control
        if not self.check_connect(control_session):
            self.write_log(action="Load data to Data Mart", details="Failed to connect to Control", status="Error")
            log_data = self.get_log_data()
            log_body = self.generate_log_email_body(log_data)
            recipients = ['21130616@st.hcmuaf.edu.vn', '21130537@st.hcmuaf.edu.vn', '21130484@st.hcmuaf.edu.vn',
                          '20130127@st.hcmuaf.edu.vn']
            self.send_email("Load DataMart Completed", log_body, recipients)

            return

        # Step 3: Connect to Warehouse Database
        data_warehouse_session = self.get_connection_to_datamart()

        # Step 4: Load data from Warehouse to Data Mart
        total_product = self.insert_data_to_datamart(conn_warehouse=self.SessionWarehouse().connection(),
                                                     conn_datamart=data_warehouse_session.connection())

        # Step 5: Insert log entry for the load process
        self.write_log(action="Load data to Data Mart", details=f"Loaded {total_product} products into Data Mart.", status="Success")

        # Step 6: Close all sessions
        control_session.close()
        data_warehouse_session.close()
        self.SessionWarehouse().close()

        log_data = self.get_log_data()
        log_body = self.generate_log_email_body(log_data)
        recipients = ['21130616@st.hcmuaf.edu.vn', '21130537@st.hcmuaf.edu.vn', '21130484@st.hcmuaf.edu.vn',
                      '20130127@st.hcmuaf.edu.vn']
        self.send_email("Load DataMart Completed", log_body, recipients)

if __name__ == "__main__":
    datamart = LoadDataMart()
