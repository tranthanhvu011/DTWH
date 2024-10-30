import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:@localhost/dtwh?charset=utf8mb4')

products_df = pd.read_csv('products.csv')
specifications_df = pd.read_csv('specifications.csv')
images_df = pd.read_csv('images.csv')

products_df.to_sql('staging_products', con=engine, if_exists='append', index=False)

specifications_df.to_sql('staging_specifications', con=engine, if_exists='append', index=False)

images_df.to_sql('staging_images', con=engine, if_exists='append', index=False)

print("Dữ liệu đã được load vào các bảng trung gian.")
