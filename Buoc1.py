from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Text, DECIMAL

# Kết nối với MySQL với charset utf8mb4
engine = create_engine('mysql+pymysql://root:@localhost/dtwh?charset=utf8mb4')
metadata = MetaData()

# Tạo bảng trung gian và bảng chính
staging_products = Table(
    'staging_products', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('price', DECIMAL(10, 2)),
    Column('discounted_price', DECIMAL(10, 2)),
    Column('discount_percent', DECIMAL(5, 2)),
    Column('thumb_image', String(255, collation='utf8mb4_unicode_ci'))
)

# Tạo bảng với cột spec_value là TEXT để lưu trữ chuỗi dài
staging_specifications = Table(
    'staging_specifications', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('spec_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('spec_value', Text(collation='utf8mb4_unicode_ci'))
)

staging_images = Table(
    'staging_images', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('image_url', String(255, collation='utf8mb4_unicode_ci'))
)

products = Table(
    'products', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('price', DECIMAL(10, 2)),
    Column('discounted_price', DECIMAL(10, 2)),
    Column('discount_percent', DECIMAL(5, 2)),
    Column('thumb_image', String(255, collation='utf8mb4_unicode_ci'))
)

specifications = Table(
    'specifications', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('spec_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('spec_value', Text(collation='utf8mb4_unicode_ci'))
)

images = Table(
    'images', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('product_name', String(255, collation='utf8mb4_unicode_ci')),
    Column('image_url', String(255, collation='utf8mb4_unicode_ci'))
)

config_table = Table(
    'config_table', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('source_table', String(100, collation='utf8mb4_unicode_ci')),
    Column('target_table', String(100, collation='utf8mb4_unicode_ci')),
    Column('column_mapping', Text(collation='utf8mb4_unicode_ci'))
)

# Tạo các bảng
metadata.create_all(engine)
print("Tất cả các bảng đã được tạo thành công!")
