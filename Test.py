from sqlalchemy import create_engine, Table, Column, Integer, Text, MetaData
import json

# Thiết lập kết nối tới MySQL bằng SQLAlchemy
engine = create_engine('mysql+pymysql://root:@localhost/dtwh?charset=utf8mb4', echo=True)
metadata = MetaData()

# Định nghĩa bảng config với id và data (lưu cấu hình dưới dạng JSON)
config_table = Table('config', metadata,
                     Column('id', Integer, primary_key=True, autoincrement=True),
                     Column('data', Text)  # Lưu cấu hình dưới dạng JSON
                     )

# Tạo bảng trong CSDL nếu chưa tồn tại
metadata.create_all(engine)


# Hàm chèn dữ liệu với cam kết thủ công và xử lý lỗi
def insert_config_data():
    config_data = {
        "data_dir": "/Users/thanhvu/Documents/data",
        "products_csv": "products.csv",
        "specifications_csv": "specifications.csv",
        "images_csv": "images.csv",
        "category_url": "https://gearvn.com/collections/laptop",
        "db_connection": "mysql+pymysql://root:@localhost/dtwh?charset=utf8mb4",
        "chromedriver_path": "/Users/thanhvu/Downloads/chromedriver-mac-arm64/chromedriver"
    }
    json_data = json.dumps(config_data)

    # Thực hiện chèn với xử lý ngoại lệ và cam kết thủ công
    conn = engine.connect()
    transaction = conn.begin()  # Bắt đầu giao dịch
    try:
        # Kiểm tra xem bảng config có trống không
        existing_data = conn.execute(config_table.select()).fetchone()
        if not existing_data:
            print("Bảng config trống, tiến hành chèn dữ liệu...")
            conn.execute(config_table.insert().values(data=json_data))
            transaction.commit()  # Cam kết thủ công nếu không có lỗi
            print("Dữ liệu đã được chèn vào bảng config.")
        else:
            print("Dữ liệu đã tồn tại trong bảng config, không chèn thêm.")
    except Exception as e:
        print(f"Đã xảy ra lỗi khi chèn dữ liệu: {e}")
        transaction.rollback()  # Hoàn tác giao dịch nếu có lỗi
    finally:
        conn.close()  # Đóng kết nối


# Hàm kiểm tra dữ liệu
def check_config_data():
    with engine.connect() as conn:
        result = conn.execute(config_table.select()).fetchone()
        if result:
            config_data = json.loads(result['data'])
            print("Dữ liệu trong bảng config:", config_data)
        else:
            print("Bảng config không có dữ liệu.")


# Gọi hàm chèn và kiểm tra dữ liệu
insert_config_data()
check_config_data()