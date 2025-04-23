import pymysql
from flask import Flask
from flask_cors import CORS
from .views import views
from .auth import auth

def create_app():
    app = Flask(__name__,static_folder='static')
    CORS(app)
    app.config['SECRET_KEY'] = 'Khang dep zai vl'

    # Cấu hình kết nối database
    app.config['DB_CONFIG'] = {
        'host': 'clouddb.cluicays6umz.ap-southeast-2.rds.amazonaws.com',
        'port': 3306,
        'database': 'file_converter',
        'user': 'admin',
        'password': 'mypassword',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    def get_db_connection():
        connection = pymysql.connect(**app.config['DB_CONFIG'])
        return connection

    app.get_db_connection = get_db_connection
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(auth, url_prefix='/')

    return app