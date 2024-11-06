import csv
import json
import time
import os
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, MetaData
from sqlalchemy.orm import sessionmaker
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

# Kết nối tới cơ sở dữ liệu
engine = create_engine('mysql+pymysql://root:@localhost/dtwh?charset=utf8mb4', echo=True)
Session = sessionmaker(bind=engine)
metadata = MetaData()

# Định nghĩa bảng config với dữ liệu JSON
config_table = Table('config', metadata,
                     Column('id', Integer, primary_key=True, autoincrement=True),
                     Column('data', Text)
                     )

# Định nghĩa bảng logs với cột config_id
logs_table = Table('logs', metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
                   Column('config_id', Integer),
                   Column('timestamp', String(50)),
                   Column('action', String(50)),
                   Column('details', String(255)),
                   Column('status', String(50))
                   )

# Tạo bảng nếu chưa tồn tại
metadata.create_all(engine)

# Hàm lấy cấu hình từ bảng config
def get_config_data():
    with engine.connect() as conn:
        result = conn.execute(config_table.select()).mappings().fetchone()
        if result:
            return json.loads(result['data'])
        else:
            raise ValueError("Không tìm thấy cấu hình trong bảng config.")

# Đọc cấu hình từ bảng config
config = get_config_data()

# Thiết lập kết nối MySQL với cấu hình từ config
db_connection = config['db_connection']
engine = create_engine(db_connection, echo=True)

# Hàm ghi log vào MySQL
def write_log_to_sql(action, details, status="Success"):
    try:
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        config_id = 1  # Giả sử cấu hình hiện tại có ID là 1 (cập nhật nếu cần)
        new_log = logs_table.insert().values(config_id=config_id, timestamp=timestamp, action=action, details=details, status=status)
        with engine.begin() as conn:
            conn.execute(new_log)
        print(f"Log entry added: {action} - {details} - {status}")
    except Exception as e:
        print(f"Lỗi khi ghi log vào MySQL: {e}")

# Hàm làm sạch giá trị giá tiền
def clean_price(price_str):
    if price_str:
        price_str = price_str.replace('₫', '').replace('.', '').replace(',', '.')
        try:
            return float(price_str)
        except ValueError:
            return None
    return None

# Hàm làm sạch phần trăm giảm giá
def clean_discount(discount_str):
    if discount_str:
        discount_str = discount_str.replace('%', '')
        try:
            return float(discount_str)
        except ValueError:
            return None
    return None

# Đường dẫn thư mục và các file CSV từ cấu hình
data_dir = config['data_dir']
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

products_csv = os.path.join(data_dir, config['file_product'])
specifications_csv = os.path.join(data_dir, config['file_specifications'])
images_csv = os.path.join(data_dir, config['file_image'])

# Kiểm tra xem sản phẩm đã tồn tại trong file CSV chưa
def product_exists(product_name):
    if not os.path.exists(products_csv):
        return None
    with open(products_csv, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['product_name'] == product_name:
                return int(row['id'])  # Trả về ID của sản phẩm nếu tồn tại
    return None

# Set up the webdriver
chrome_options = Options()
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-gpu')
# chrome_options.add_argument('--headless')  # Chạy ở chế độ không giao diện

service = Service(config['chromedriver_path'])
driver = webdriver.Chrome(service=service, options=chrome_options)

try:
    # Ghi log thời gian bắt đầu
    start_time = time.strftime('%Y-%m-%d %H:%M:%S')
    write_log_to_sql("Start Crawl", f"Bắt đầu crawl vào lúc {start_time}")

    # Mở trang danh mục sản phẩm
    category_url = config['category_url']
    driver.get(category_url)
    time.sleep(5)

    # Lấy các liên kết sản phẩm
    product_elements = driver.find_elements(By.CSS_SELECTOR, '.proloop .proloop-name a')
    product_links = [product.get_attribute('href') for product in product_elements]

    # Lists to store data for each table
    products_list = []
    specifications_list = []
    images_list = []
    new_products = 0

    # Duyệt qua từng sản phẩm
    for product_link in product_links:
        driver.get(product_link)
        time.sleep(3)

        # Lấy tên sản phẩm
        product_name = driver.find_element(By.CSS_SELECTOR, '.product-name h1').text

        # Kiểm tra xem sản phẩm đã tồn tại
        product_id = product_exists(product_name)
        if product_id:
            print(f"Sản phẩm '{product_name}' đã tồn tại. Bỏ qua.")
            continue

        # Get product price
        try:
            price = driver.find_element(By.CSS_SELECTOR, '.product-price .pro-price').text
            price = clean_price(price)
        except:
            price = None
        try:
            discounted_price = driver.find_element(By.CSS_SELECTOR, '.product-price del').text
            discounted_price = clean_price(discounted_price)
        except:
            discounted_price = None
        try:
            discount_percent = driver.find_element(By.CSS_SELECTOR, '.product-price .pro-percent').text
            discount_percent = clean_discount(discount_percent)
        except:
            discount_percent = None

        # Get thumbnail image
        try:
            thumb_image = driver.find_element(By.CSS_SELECTOR, '.img-default').get_attribute('src')
        except:
            thumb_image = None

        # Append product info to products list
        product_id = len(products_list) + 1  # Tạo ID sản phẩm mới
        products_list.append([product_id, product_name, price, discounted_price, discount_percent, thumb_image])
        new_products += 1

        # Get specifications data
        try:
            specs_table = driver.find_element(By.ID, 'tblGeneralAttribute')
            specs_rows = specs_table.find_elements(By.TAG_NAME, 'tr')
            for row in specs_rows:
                columns = row.find_elements(By.TAG_NAME, 'td')
                if len(columns) == 2:
                    spec_name = columns[0].text
                    spec_value = columns[1].text
                    specifications_list.append([product_id, spec_name, spec_value])
        except:
            pass

        # Get detailed images
        try:
            detail_image_elements = driver.find_elements(By.CSS_SELECTOR, '.product-gallery--photo a')
            for detail_image_element in detail_image_elements:
                image_url = detail_image_element.get_attribute('href')
                images_list.append([product_id, image_url])
        except:
            pass

except Exception as e:
    # Ghi log khi có lỗi trong quá trình crawl
    print(f"Lỗi trong quá trình crawl: {e}")
    write_log_to_sql("Crawl Failed", f"Lỗi trong quá trình crawl: {e}", "Failed")

finally:
    # Đảm bảo rằng quá trình luôn kết thúc và ghi log thời gian kết thúc
    end_time = time.strftime('%Y-%m-%d %H:%M:%S')
    write_log_to_sql("End Crawl", f"Kết thúc crawl vào lúc {end_time}")
    driver.quit()  # Đóng trình duyệt sau khi hoàn tất

    # Append new products to CSV if there are any
    if products_list:
        with open(products_csv, mode='a', newline='', encoding='utf-8') as product_file:
            product_writer = csv.writer(product_file)
            if os.stat(products_csv).st_size == 0:
                product_writer.writerow(['id', 'product_name', 'price', 'discounted_price', 'discount_percent', 'thumb_image'])
            for product in products_list:
                product_writer.writerow(product)

        with open(specifications_csv, mode='a', newline='', encoding='utf-8') as spec_file:
            spec_writer = csv.writer(spec_file)
            if os.stat(specifications_csv).st_size == 0:
                spec_writer.writerow(['product_id', 'spec_name', 'spec_value'])
            spec_writer.writerows(specifications_list)

        with open(images_csv, mode='a', newline='', encoding='utf-8') as img_file:
            img_writer = csv.writer(img_file)
            if os.stat(images_csv).st_size == 0:
                img_writer.writerow(['product_id', 'image_url'])
            img_writer.writerows(images_list)

        write_log_to_sql("Add Products", f"Đã thêm {new_products} sản phẩm mới vào file.", "Success")
    else:
        write_log_to_sql("Add Products", "Không có sản phẩm mới.", "Info")

# Tổng kết số lượng sản phẩm
write_log_to_sql("Summary", f"Tổng số sản phẩm crawl được: {len(product_links)}")
write_log_to_sql("Summary", f"Tổng số sản phẩm mới thêm vào: {new_products}")

print("Data saved to CSV files and log saved to MySQL successfully.")