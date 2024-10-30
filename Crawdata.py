import csv
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker

# Thiết lập kết nối tới MySQL bằng SQLAlchemy với chế độ echo để kiểm tra câu truy vấn
engine = create_engine('mysql+pymysql://root:@localhost/dtwh?charset=utf8mb4', echo=True)
Session = sessionmaker(bind=engine)
metadata = MetaData()

# Định nghĩa bảng logs nếu chưa có
logs_table = Table('logs', metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True),
                   Column('timestamp', String(50)),
                   Column('action', String(50)),
                   Column('details', String(255)),
                   Column('status', String(50))  # Thêm cột status
                   )
metadata.create_all(engine)  # Tạo bảng nếu chưa có

# Hàm ghi log vào MySQL với xử lý lỗi và thông báo
def write_log_to_sql(action, details, status="Success"):  # Thêm tham số status
    try:
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        new_log = logs_table.insert().values(timestamp=timestamp, action=action, details=details, status=status)
        with engine.begin() as conn:
            conn.execute(new_log)
        print(f"Log entry added: {action} - {details} - {status}")
    except Exception as e:
        print(f"Lỗi khi ghi log vào MySQL: {e}")

# Đường dẫn thư mục lưu trữ
data_dir = '/Users/thanhvu/Documents/data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

# Đường dẫn đến các file CSV
products_csv = os.path.join(data_dir, 'products.csv')
specifications_csv = os.path.join(data_dir, 'specifications.csv')
images_csv = os.path.join(data_dir, 'images.csv')

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

# Kiểm tra xem sản phẩm đã tồn tại trong file CSV chưa
def product_exists(product_name):
    if not os.path.exists(products_csv):
        return False
    with open(products_csv, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)  # Sử dụng DictReader để đọc theo tên cột
        for row in reader:
            if row['product_name'] == product_name:  # Chỉ kiểm tra product_name
                return True  # Sản phẩm đã tồn tại
    return False

# Ghi log thời gian bắt đầu
start_time = time.strftime('%Y-%m-%d %H:%M:%S')
write_log_to_sql("Start Crawl", f"Bắt đầu crawl vào lúc {start_time}")

# Set up the webdriver
service = Service('/Users/thanhvu/Downloads/chromedriver-mac-arm64/chromedriver')
driver = webdriver.Chrome(service=service)

# Open the product category page
category_url = 'https://gearvn.com/collections/laptop'
driver.get(category_url)
time.sleep(5)  # Wait for the page to load

# Extract product links
product_elements = driver.find_elements(By.CSS_SELECTOR, '.proloop .proloop-name a')
product_links = [product.get_attribute('href') for product in product_elements]

# Lists to store data for each table
products_list = []
specifications_list = []
images_list = []

# Đếm số sản phẩm mới
new_products = 0

# Iterate over each product link
for product_link in product_links:
    driver.get(product_link)
    time.sleep(2)  # Wait for the product page to load

    # Get product name
    product_name = driver.find_element(By.CSS_SELECTOR, '.product-name h1').text

    # Kiểm tra xem sản phẩm đã tồn tại
    if product_exists(product_name):
        print(f"Sản phẩm '{product_name}' đã tồn tại. Bỏ qua.")
        continue

    # Get product price
    try:
        price = driver.find_element(By.CSS_SELECTOR, '.product-price .pro-price').text
        price = clean_price(price)
    except Exception as e:
        print(f"Could not retrieve price for product {product_name}: {e}")
        price = None
    try:
        discounted_price = driver.find_element(By.CSS_SELECTOR, '.product-price del').text
        discounted_price = clean_price(discounted_price)
    except Exception as e:
        print(f"Could not retrieve discounted price for product {product_name}: {e}")
        discounted_price = None
    try:
        discount_percent = driver.find_element(By.CSS_SELECTOR, '.product-price .pro-percent').text
        discount_percent = clean_discount(discount_percent)
    except Exception as e:
        print(f"Could not retrieve discount percent for product {product_name}: {e}")
        discount_percent = None

    # Get thumbnail image
    try:
        thumb_image = driver.find_element(By.CSS_SELECTOR, '.img-default').get_attribute('src')
    except Exception as e:
        print(f"Could not retrieve thumbnail image for product {product_name}: {e}")
        thumb_image = None

    # Append product info to products list
    products_list.append([product_name, price, discounted_price, discount_percent, thumb_image])
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
                specifications_list.append([product_name, spec_name, spec_value])
    except Exception as e:
        print(f"No specification table found for product {product_name}: {e}")

    # Get detailed images
    try:
        detail_image_elements = driver.find_elements(By.CSS_SELECTOR, '.product-gallery--photo a')
        for detail_image_element in detail_image_elements:
            image_url = detail_image_element.get_attribute('href')
            images_list.append([product_name, image_url])
    except Exception as e:
        print(f"Could not find images for product {product_name}: {e}")

# Close the browser
driver.quit()

# Append new products to CSV if there are any
if products_list:
    with open(products_csv, mode='a', newline='', encoding='utf-8') as product_file:
        product_writer = csv.writer(product_file)
        if os.stat(products_csv).st_size == 0:
            product_writer.writerow(['id', 'product_name', 'price', 'discounted_price', 'discount_percent', 'thumb_image'])
        for idx, product in enumerate(products_list, start=1):
            product_writer.writerow([idx] + product)

    with open(specifications_csv, mode='a', newline='', encoding='utf-8') as spec_file:
        spec_writer = csv.writer(spec_file)
        if os.stat(specifications_csv).st_size == 0:
            spec_writer.writerow(['product_name', 'spec_name', 'spec_value'])
        spec_writer.writerows(specifications_list)

    with open(images_csv, mode='a', newline='', encoding='utf-8') as img_file:
        img_writer = csv.writer(img_file)
        if os.stat(images_csv).st_size == 0:
            img_writer.writerow(['product_name', 'image_url'])
        img_writer.writerows(images_list)

    write_log_to_sql("Add Products", f"Đã thêm {new_products} sản phẩm mới vào file.", "Success")
else:
    write_log_to_sql("Add Products", "Không có sản phẩm mới.", "Info")

# Ghi log thời gian kết thúc
end_time = time.strftime('%Y-%m-%d %H:%M:%S')
write_log_to_sql("End Crawl", f"Kết thúc crawl vào lúc {end_time}")
write_log_to_sql("Summary", f"Tổng số sản phẩm crawl được: {len(product_links)}")
write_log_to_sql("Summary", f"Tổng số sản phẩm mới thêm vào: {new_products}")

print("Data saved to CSV files and log saved to MySQL successfully.")
