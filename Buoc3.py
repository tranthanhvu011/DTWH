from sqlalchemy import create_engine, MetaData, Table, insert, text

engine = create_engine('mysql+pymysql://root:@localhost/dtwh')
metadata = MetaData()

config_table = Table('config_table', metadata, autoload_with=engine)

try:
    with engine.begin() as conn:  # engine.begin() để tự động commit sau khi chèn
        conn.execute(insert(config_table), [
            {
                'source_table': 'staging_products',
                'target_table': 'products',
                'column_mapping': '{"product_name": "product_name", "price": "price", "discounted_price": "discounted_price", "discount_percent": "discount_percent", "thumb_image": "thumb_image"}'
            },
            {
                'source_table': 'staging_specifications',
                'target_table': 'specifications',
                'column_mapping': '{"product_name": "product_name", "spec_name": "spec_name", "spec_value": "spec_value"}'
            },
            {
                'source_table': 'staging_images',
                'target_table': 'images',
                'column_mapping': '{"product_name": "product_name", "image_url": "image_url"}'
            }
        ])
    print("Dữ liệu đã được chèn vào bảng config_table thành công!")
except Exception as e:
    print(f"Có lỗi xảy ra khi chèn dữ liệu: {e}")
