import json
from sqlalchemy import create_engine, text

engine = create_engine('mysql+pymysql://root:@localhost/dtwh')


def migrate_data(source_table, target_table, column_mapping, conn):
    columns = json.loads(column_mapping)  # Chuyển JSON thành dictionary
    columns_insert = ', '.join([f"{col}" for col in columns.values()])

    if target_table == "products":
        sql_query = f"""
        INSERT INTO {target_table} ({columns_insert})
        SELECT s.product_name, MAX(s.price) AS price, MAX(s.discounted_price) AS discounted_price,
               MAX(s.discount_percent) AS discount_percent, MAX(s.thumb_image) AS thumb_image
        FROM {source_table} s
        GROUP BY s.product_name
        HAVING NOT EXISTS (
            SELECT 1 FROM {target_table} p WHERE p.product_name = s.product_name
        )
        ON DUPLICATE KEY UPDATE
        price = VALUES(price), 
        discounted_price = VALUES(discounted_price), 
        discount_percent = VALUES(discount_percent), 
        thumb_image = VALUES(thumb_image);
        """
    elif target_table == "specifications":
        sql_query = f"""
        INSERT INTO {target_table} (product_name, spec_name, spec_value)
        SELECT s.product_name, s.spec_name, s.spec_value
        FROM {source_table} s
        GROUP BY s.product_name, s.spec_name
        HAVING NOT EXISTS (
            SELECT 1 FROM {target_table} sp WHERE sp.product_name = s.product_name AND sp.spec_name = s.spec_name
        )
        ON DUPLICATE KEY UPDATE
        spec_value = VALUES(spec_value);
        """
    elif target_table == "images":
        # Tạo câu lệnh SQL cho bảng images
        sql_query = f"""
        INSERT INTO {target_table} (product_name, image_url)
        SELECT s.product_name, s.image_url
        FROM {source_table} s
        GROUP BY s.product_name, s.image_url
        HAVING NOT EXISTS (
            SELECT 1 FROM {target_table} i WHERE i.product_name = s.product_name AND i.image_url = s.image_url
        )
        ON DUPLICATE KEY UPDATE
        image_url = VALUES(image_url);
        """
    else:
        print(f"Error: Unrecognized target table '{target_table}'.")
        return

    print(f"Executing query:\n{sql_query}\n")

    conn.execute(text(sql_query))


# Di chuyển dữ liệu dựa trên config_table
with engine.begin() as conn:  # Dùng begin() để tự động commit sau khi thực hiện truy vấn
    result = conn.execute(text("SELECT * FROM config_table"))

    for row in result:
        source_table = row[1]  # 'source_table' ở vị trí chỉ mục 1
        target_table = row[2]  # 'target_table' ở vị trí chỉ mục 2
        column_mapping = row[3]  # 'column_mapping' ở vị trí chỉ mục 3

        # Gọi hàm di chuyển dữ liệu từ bảng staging sang bảng chính
        migrate_data(source_table, target_table, column_mapping, conn)

print("Dữ liệu đã được di chuyển và cập nhật thành công, loại bỏ trùng lặp!")
