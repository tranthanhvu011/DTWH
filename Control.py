from flask import Flask, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

# Định nghĩa bộ lọc format_price
@app.template_filter('format_price')
def format_price(value):
    return "{:,.2f}".format(value) if value is not None else ""

# Cấu hình cơ sở dữ liệu
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:@localhost/mart'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

# Mô hình dữ liệu cho bảng sản phẩm
class Product(db.Model):
    __tablename__ = 'datamart_products'

    id = db.Column(db.Integer, primary_key=True)
    product_name = db.Column(db.String(255))
    price = db.Column(db.Float)
    discounted_price = db.Column(db.Float)
    discount_percent = db.Column(db.Float)
    thumb_image = db.Column(db.String(255))
    date_update = db.Column(db.Integer)
    sk = db.Column(db.Integer)

    # Mối quan hệ với bảng images
    images = db.relationship('Image', backref='product', lazy=True)

    # Mối quan hệ với bảng specifications
    specifications = db.relationship('Specification', backref='product', lazy=True)

    def __repr__(self):
        return f'<Product {self.product_name}>'

# Mô hình dữ liệu cho bảng images
class Image(db.Model):
    __tablename__ = 'datamart_images'

    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey('datamart_products.id'), nullable=False)
    image_url = db.Column(db.String(255))
    date_update = db.Column(db.Integer)

    def __repr__(self):
        return f'<Image {self.image_url}>'

# Mô hình dữ liệu cho bảng specifications
class Specification(db.Model):
    __tablename__ = 'datamart_specifications'

    id = db.Column(db.Integer, primary_key=True)
    product_id = db.Column(db.Integer, db.ForeignKey('datamart_products.id'), nullable=False)
    spec_name = db.Column(db.String(255))
    spec_value = db.Column(db.String(255))
    date_update = db.Column(db.Integer)

    def __repr__(self):
        return f'<Specification {self.spec_name}: {self.spec_value}>'

# Trang chủ
@app.route('/')
def index():
    # Truy vấn tất cả sản phẩm từ cơ sở dữ liệu
    products = Product.query.all()
    return render_template('index.html', products=products)

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080)  # Chạy trên port 8080 thay vì 5000
