from datetime import datetime, timedelta
import time
from apscheduler.schedulers.background import BackgroundScheduler
from pathlib import Path
import boto3
import pymysql
from flask import Blueprint, render_template, current_app, request, session, jsonify
from werkzeug.utils import secure_filename
import os
import json
import logging
from ETL_System.resolve.extract_S3 import extract_s3
from ETL_System.resolve.load_S3 import load_s3
from ETL_System.resolve.transform_S3 import transform_s3
from resolve.load_file1 import extract_files_task, transform_data_task, load_data_task
from resolve.load_file2 import extract_file2, transform_file2, load_file2
from resolve.extract_api import extract_data_api
from resolve.transform_api import transform_data_api
from  resolve.load_api import load_data_api
from resolve.extract_database import extract_data_database
from resolve.transform_database import transform_data_database
from resolve.load_database import load_data_database
from resolve.etl_job import load_s3_buckets, run_etl_job
import sqlite3

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
views = Blueprint('views', __name__)

@views.route('/')
def home():
    session.pop('word_pdf_records', None)
    session.pop('word_pdf_headers', None)
    session.pop('excel_csv_records', None)
    session.pop('excel_csv_headers', None)
    return render_template("home.html", result=None, step=None)

@views.route('/extract', methods=['POST'])
def extract_data():
    if 'file' not in request.files:
        return jsonify({'error': 'Không có file được tải lên'}), 400

    files = request.files.getlist('file')
    if not files or all(file.filename == '' for file in files):
        return jsonify({'error': 'Chưa chọn file'}), 400

    upload_folder = os.path.join(current_app.root_path, 'Uploads')
    os.makedirs(upload_folder, exist_ok=True)

    # Xóa các file cũ trong thư mục uploads
    for old_file in os.listdir(upload_folder):
        old_file_path = os.path.join(upload_folder, old_file)
        if os.path.isfile(old_file_path):
            try:
                os.remove(old_file_path)
            except PermissionError:
                pass

    saved_files = []
    for file in files:
        if file and file.filename.lower().endswith(('.docx', '.pdf')):
            filename = secure_filename(file.filename)
            file_path = os.path.join(upload_folder, filename)
            file.save(file_path)
            saved_files.append(filename)

    if not saved_files:
        return jsonify({'error': 'Không có file hợp lệ được tải lên'}), 400

    try:
        result = extract_files_task(current_app.root_path, filenames=saved_files)
        records, headers, errors = result

        if not records:
            error_msg = errors[0] if errors else 'Không có dữ liệu được trích xuất từ file'
            return jsonify({'error': error_msg}), 400

        session['word_pdf_records'] = records
        session['word_pdf_headers'] = headers

        for filename in saved_files:
            file_path = os.path.join(upload_folder, filename)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except PermissionError:
                    pass

        return jsonify({
            'records': records,
            'headers': headers
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi trích xuất: {str(e)}"}), 500

@views.route('/transform', methods=['POST'])
def transform_data():
    data = request.get_json()
    if not data or 'records' not in data or 'headers' not in data:
        return jsonify({'error': 'Dữ liệu không hợp lệ'}), 400

    records = data['records']
    headers = data['headers']

    try:
        transformed_records, transformed_headers = transform_data_task(records, headers)
        session['word_pdf_records'] = transformed_records
        session['word_pdf_headers'] = transformed_headers

        return jsonify({
            'records': transformed_records,
            'headers': transformed_headers
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi chuẩn hóa: {str(e)}"}), 500

@views.route('/load', methods=['POST'])
def load_data():
    if 'word_pdf_records' not in session or 'word_pdf_headers' not in session:
        return jsonify({'error': 'Không có dữ liệu để lưu. Vui lòng thực hiện các bước trước.'}), 400

    data = request.get_json()
    if not data or 'table_name' not in data:
        return jsonify({'error': 'Tên bảng không được cung cấp'}), 400

    records = session['word_pdf_records']
    headers = session['word_pdf_headers']
    table_name = data['table_name']

    try:
        table_name = load_data_task(records, headers, current_app.get_db_connection, table_name=table_name)
        return jsonify({
            'table_name': table_name
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi lưu dữ liệu: {str(e)}"}), 500

@views.route('/extract_excel_csv', methods=['POST'])
def extract_excel_csv():
    if 'file' not in request.files:
        return jsonify({'error': 'Không có file được tải lên'}), 400

    files = request.files.getlist('file')
    if not files or all(file.filename == '' for file in files):
        return jsonify({'error': 'Chưa chọn file'}), 400

    upload_folder = os.path.join(current_app.root_path, 'Uploads')
    os.makedirs(upload_folder, exist_ok=True)

    # Xóa các file cũ trong thư mục uploads
    for old_file in os.listdir(upload_folder):
        old_file_path = os.path.join(upload_folder, old_file)
        if os.path.isfile(old_file_path):
            try:
                os.remove(old_file_path)
            except PermissionError:
                pass

    saved_files = []
    for file in files:
        if file and file.filename.lower().endswith(('.xlsx', '.xls', '.csv')):
            filename = secure_filename(file.filename)
            file_path = os.path.join(upload_folder, filename)
            file.save(file_path)
            saved_files.append(filename)

    if not saved_files:
        return jsonify({'error': 'Không có file hợp lệ được tải lên'}), 400

    try:
        result = extract_file2(current_app.root_path, filenames=saved_files)
        records, headers, errors = result

        if not records:
            error_msg = errors[0] if errors else 'Không có dữ liệu được trích xuất từ file'
            return jsonify({'error': error_msg}), 400

        session['excel_csv_records'] = records
        session['excel_csv_headers'] = headers

        for filename in saved_files:
            file_path = os.path.join(upload_folder, filename)
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except PermissionError:
                    pass

        return jsonify({
            'records': records,
            'headers': headers
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi trích xuất: {str(e)}"}), 500

@views.route('/transform_excel_csv', methods=['POST'])
def transform_excel_csv():
    data = request.get_json()
    if not data or 'records' not in data or 'headers' not in data:
        return jsonify({'error': 'Dữ liệu không hợp lệ'}), 400

    records = data['records']
    headers = data['headers']

    try:
        transformed_records, transformed_headers = transform_file2(records, headers)
        session['excel_csv_records'] = transformed_records
        session['excel_csv_headers'] = transformed_headers

        return jsonify({
            'records': transformed_records,
            'headers': transformed_headers
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi chuẩn hóa: {str(e)}"}), 500

@views.route('/load_excel_csv', methods=['POST'])
def load_excel_csv():
    if 'excel_csv_records' not in session or 'excel_csv_headers' not in session:
        return jsonify({'error': 'Không có dữ liệu để lưu. Vui lòng thực hiện các bước trước.'}), 400

    data = request.get_json()
    if not data or 'table_name' not in data:
        return jsonify({'error': 'Tên bảng không được cung cấp'}), 400

    records = session['excel_csv_records']
    headers = session['excel_csv_headers']
    table_name = data['table_name']

    try:
        table_name = load_file2(records, headers, current_app.get_db_connection, table_name=table_name)
        return jsonify({
            'table_name': table_name
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi lưu dữ liệu: {str(e)}"}), 500


@views.route('/api/extract', methods=['POST'])
def extract():
    data = request.get_json()
    api_url = data.get('apiUrl')
    if not api_url:
        logger.error("API URL is required")
        return jsonify({"status": "error", "message": "API URL is required"}), 400
    result = extract_data_api(api_url)
    return jsonify(result)

@views.route('/api/transform', methods=['POST'])
def transform():
    data = request.get_json()
    records = data.get('data')
    if not records:
        logger.error("No data provided for transformation")
        return jsonify({"status": "error", "message": "No data to transform"}), 400
    transformed_records = transform_data_api(records)
    return jsonify({"status": "success", "data": transformed_records})

@views.route('/api/load', methods=['POST'])
def load():
    data = request.get_json()
    records = data.get('data')
    table_name = data.get('tableName')
    if not records:
        logger.error("No data provided for loading")
        return jsonify({"status": "error", "message": "No data to load"}), 400
    if not table_name:
        logger.error("Table name is required for loading")
        return jsonify({"status": "error", "message": "Table name is required"}), 400
    result = load_data_api(records, table_name)
    return jsonify(result)

@views.route('/api/databases', methods=['GET'])
def get_databases():
    conn = None
    cursor = None
    try:
        conn = current_app.get_db_connection()
        logger.info("Successfully connected to MySQL")
        cursor = conn.cursor()  # DictCursor từ __init__.py
        logger.info("Executing SHOW DATABASES")
        cursor.execute("SHOW DATABASES")
        raw_data = cursor.fetchall()
        logger.info(f"Raw data from SHOW DATABASES: {raw_data}")
        if not raw_data:
            logger.warning("No databases returned from SHOW DATABASES")
            return jsonify({"status": "success", "databases": []})

        databases = [row['Database'] for row in raw_data]
        logger.info(f"Fetched {len(databases)} databases: {databases}")
        return jsonify({"status": "success", "databases": databases})
    except pymysql.Error as mysql_err:
        logger.error(f"MySQL error fetching databases: {mysql_err.args[1]} (Error code: {mysql_err.args[0]})")
        return jsonify({"status": "error", "message": f"MySQL error: {mysql_err.args[1]}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error fetching databases: {str(e)} (Type: {type(e).__name__})")
        return jsonify({"status": "error", "message": f"Unexpected error: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
            logger.info("Cursor closed")
        if conn:
            conn.close()
            logger.info("MySQL connection closed")

@views.route('/api/tables', methods=['POST'])
def get_tables():
    conn = None
    cursor = None
    data = request.get_json()
    database_name = data.get('database')
    if not database_name:
        logger.error("Database name is required for fetching tables")
        return jsonify({"status": "error", "message": "Database name is required"}), 400
    try:
        logger.info(f"Fetching tables for database: {database_name}")
        conn = current_app.get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"USE {database_name}")
        cursor.execute("SHOW TABLES")
        raw_data = cursor.fetchall()
        logger.info(f"Raw data from SHOW TABLES: {raw_data}")
        tables = [row[f'Tables_in_{database_name}'] for row in raw_data]
        logger.info(f"Fetched {len(tables)} tables from {database_name}: {tables}")
        return jsonify({"status": "success", "tables": tables})
    except pymysql.Error as mysql_err:
        logger.error(f"MySQL error fetching tables from {database_name}: {mysql_err.args[1]} (Error code: {mysql_err.args[0]})")
        return jsonify({"status": "error", "message": f"MySQL error: {mysql_err.args[1]}"}), 500
    except Exception as e:
        logger.error(f"Unexpected error fetching tables from {database_name}: {str(e)} (Type: {type(e).__name__})")
        return jsonify({"status": "error", "message": f"Unexpected error: {str(e)}"}), 500
    finally:
        if cursor:
            cursor.close()
            logger.info("Cursor closed")
        if conn:
            conn.close()
            logger.info("MySQL connection closed")

@views.route('/api/extract_database', methods=['POST'])
def extract_database():
    data = request.get_json()
    database_name = data.get('database')
    table_name = data.get('table')
    if not database_name or not table_name:
        logger.error("Database name and table name are required for extraction")
        return jsonify({"status": "error", "message": "Database name and table name are required"}), 400
    result = extract_data_database(database_name, table_name)
    return jsonify(result)

@views.route('/api/transform_database', methods=['POST'])
def transform_database():
    data = request.get_json()
    records = data.get('data')
    if not records:
        logger.error("No data provided for transformation")
        return jsonify({"status": "error", "message": "No data to transform"}), 400
    transformed_records = transform_data_database(records)
    return jsonify({"status": "success", "data": transformed_records})

@views.route('/api/load_database', methods=['POST'])
def load_database():
    data = request.get_json()
    records = data.get('data')
    database_name = data.get('database')
    table_name = data.get('tableName')
    if not records:
        logger.error("No data provided for loading")
        return jsonify({"status": "error", "message": "No data to load"}), 400
    if not database_name or not table_name:
        logger.error("Database name and table name are required for loading")
        return jsonify({"status": "error", "message": "Database name and table name are required"}), 400
    result = load_data_database(records, database_name, table_name)
    return jsonify(result)


@views.route('/get-buckets_S3', methods=['POST'])
def get_buckets_s3():
    try:
        data = request.get_json()
        if not data or 'accessKey' not in data or 'secretKey' not in data:
            logger.error("Invalid request data: accessKey or secretKey missing")
            return jsonify({'message': 'Invalid request: accessKey or secretKey missing'}), 400

        s3_client = boto3.client(
            's3',
            aws_access_key_id=data['accessKey'],
            aws_secret_access_key=data['secretKey'],
            region_name='us-east-1'
        )
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        logger.info(f"Retrieved buckets: {buckets}")
        return jsonify({'buckets': buckets})
    except Exception as e:
        logger.error(f"Failed to list buckets: {str(e)}")
        return jsonify({'message': f"Failed to list buckets: {str(e)}"}), 500


@views.route('/get-files_S3', methods=['POST'])
def get_files_s3():
    try:
        data = request.get_json()

        if not data or 'accessKey' not in data or 'secretKey' not in data or 'bucket' not in data:
            logger.error("Invalid request data: accessKey, secretKey, or bucket missing")
            return jsonify({'message': 'Invalid request: accessKey, secretKey, or bucket missing'}), 400

        s3_client = boto3.client(
            's3',
            aws_access_key_id=data['accessKey'],
            aws_secret_access_key=data['secretKey'],
            region_name='us-east-1'
        )
        response = s3_client.list_objects_v2(Bucket=data['bucket'])
        files = [obj['Key'] for obj in response.get('Contents', [])]
        logger.info(f"Retrieved files: {files}")
        return jsonify({'files': files})
    except Exception as e:
        logger.error(f"Failed to list files: {str(e)}")
        return jsonify({'message': f"Failed to list files: {str(e)}"}), 500


@views.route('/extract_S3', methods=['POST'])
def extract_s3_route():
    try:
        data = request.get_json()
        result = extract_s3(data['accessKey'], data['secretKey'], data['bucket'], data['fileKey'])
        return jsonify({'data': result})
    except Exception as e:
        logger.error(f"Extract failed: {str(e)}")
        return jsonify({'message': f"Extract failed: {str(e)}"}), 500


@views.route('/transform_S3', methods=['POST'])
def transform_s3_route():
    try:
        data = request.get_json()
        logger.info(f"Received data for transform: {data}")
        result = transform_s3(data['data'])
        return jsonify({'data': result})
    except Exception as e:
        logger.error(f"Transform failed: {str(e)}")
        return jsonify({'message': f"Transform failed: {str(e)}"}), 500


@views.route('/load_S3', methods=['POST'])
def load_s3_route():
    try:
        data = request.get_json()
        result = load_s3(data['accessKey'], data['secretKey'], data['bucket'], data['file_path'], data['data'])
        return jsonify({'message': 'Lưu thành công'})
    except Exception as e:
        logger.error(f"Load failed: {str(e)}")
        return jsonify({'message': f"Lưu thất bại: {str(e)}"}), 500


# Khởi tạo scheduler
scheduler = BackgroundScheduler()
scheduler.start()


# Khởi tạo SQLite database và reset dữ liệu
def init_db():
    conn = sqlite3.connect('etl_jobs.db')
    c = conn.cursor()
    # Xóa bảng nếu đã tồn tại và tạo lại để reset dữ liệu
    c.execute('''DROP TABLE IF EXISTS etl_jobs''')
    c.execute('''CREATE TABLE etl_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    timestamp TEXT NOT NULL
                 )''')
    conn.commit()
    conn.close()
    print("Đã reset bảng etl_jobs khi khởi động server")


# Gọi init_db() khi server khởi động
init_db()


# Hàm để parse schedule_time (ví dụ: "5m" -> 5 phút)
def parse_schedule_time(schedule_time):
    if not schedule_time:
        return None
    unit = schedule_time[-1]  # Đơn vị: m (phút), h (giờ), d (ngày)
    value = int(schedule_time[:-1])  # Giá trị số
    if unit == 'm':
        return value  # Số phút
    elif unit == 'h':
        return value * 60  # Số phút (chuyển từ giờ)
    elif unit == 'd':
        return value * 60 * 24  # Số phút (chuyển từ ngày)
    else:
        return None


# Route để tải S3 buckets
@views.route('/fetch_s3_buckets', methods=['POST'])
def load_s3_buckets_route():
    data = request.json
    access_key = data.get('access_key')
    secret_key = data.get('secret_key')

    if not access_key or not secret_key:
        print("Access Key hoặc Secret Key không được cung cấp, thử dùng IAM Role")
    else:
        print(f"Yêu cầu lấy S3 buckets với Access Key: {access_key}")

    try:
        buckets = load_s3_buckets(access_key, secret_key)
        print(f"Danh sách buckets nhận được: {buckets}")
        return jsonify({'buckets': buckets})
    except Exception as e:
        print(f"Lỗi khi lấy S3 buckets: {str(e)}")
        return jsonify({'error': str(e)}), 500


# Route để lên lịch chạy ETL Job định kỳ
@views.route('/schedule_etl', methods=['POST'])
def schedule_etl_route():
    access_key = request.form.get('access_key')
    secret_key = request.form.get('secret_key')
    bucket = request.form.get('bucket')
    schedule_time = request.form.get('schedule_time')
    script_file = request.files.get('script_file')

    if not all([access_key, secret_key, bucket, schedule_time, script_file]):
        return jsonify(
            {'error': 'Thiếu thông tin cần thiết: Access Key, Secret Key, bucket, thời gian, hoặc file script'}), 400

    # Lưu file Python vào thư mục etl_data với tên duy nhất
    timestamp = str(int(time.time()))
    script_filename = f"{timestamp}_{script_file.filename}"
    script_path = Path('etl_data') / script_filename
    script_path.parent.mkdir(parents=True, exist_ok=True)
    script_file.save(str(script_path))

    # Parse schedule_time
    interval_minutes = parse_schedule_time(schedule_time)
    if interval_minutes is None or interval_minutes <= 0:
        return jsonify({'error': 'Thời gian định kỳ không hợp lệ. Vui lòng nhập dạng như 5m, 1h, 1d'}), 400

    try:
        # Lưu thông tin job vào file tạm
        job_info = {
            'access_key': access_key,
            'secret_key': secret_key,
            'bucket': bucket,
            'script_path': str(script_path),
            'interval_minutes': interval_minutes
        }
        job_info_path = Path('temp') / f"job_{timestamp}.json"
        job_info_path.parent.mkdir(parents=True, exist_ok=True)
        with open(job_info_path, 'w') as f:
            json.dump(job_info, f)

        # Lên lịch chạy ETL job định kỳ
        job_id = f'etl_job_{timestamp}'
        # Tính thời gian chạy đầu tiên (ngay lập tức hoặc sau 1 giây để đảm bảo scheduler hoạt động)
        first_run = datetime.now() + timedelta(seconds=1)
        scheduler.add_job(
            run_scheduled_etl_job,
            'interval',
            minutes=interval_minutes,
            args=[str(job_info_path), job_id],
            id=job_id,
            next_run_time=first_run
        )

        return jsonify({
            'status': 'success',
            'message': f"ETL Job đã được lên lịch chạy định kỳ mỗi {interval_minutes} phút. Lần chạy tiếp theo: {first_run.strftime('%Y-%m-%d %H:%M:%S')}",
            'job_id': job_id
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi lên lịch ETL Job: {str(e)}"}), 500


# Route để hủy lịch chạy ETL Job
@views.route('/cancel_etl', methods=['POST'])
def cancel_etl_route():
    data = request.json
    job_id = data.get('job_id')

    if not job_id:
        return jsonify({'error': 'Thiếu job_id'}), 400

    try:
        scheduler.remove_job(job_id)

        # Xóa file job info
        job_info_path = Path('temp') / f"{job_id.replace('etl_job_', 'job_')}.json"
        if job_info_path.exists():
            with open(job_info_path, 'r') as f:
                job_info = json.load(f)
            script_path = Path(job_info['script_path'])
            if script_path.exists():
                script_path.unlink()
            job_info_path.unlink()

        return jsonify({'status': 'success', 'message': f"Đã hủy lịch chạy ETL Job {job_id}"})
    except Exception as e:
        return jsonify({'error': f"Lỗi khi hủy lịch ETL Job: {str(e)}"}), 500


# Hàm để chạy ETL job theo lịch
def run_scheduled_etl_job(job_info_path, job_id):
    try:
        # Đọc thông tin job từ file
        with open(job_info_path, 'r') as f:
            job_info = json.load(f)

        access_key = job_info['access_key']
        secret_key = job_info['secret_key']
        bucket = job_info['bucket']
        script_path = job_info['script_path']

        # Chạy ETL job
        result = run_etl_job(access_key, secret_key, bucket)

        # Lưu trạng thái job vào database
        conn = sqlite3.connect('etl_jobs.db')
        c = conn.cursor()
        c.execute('''INSERT INTO etl_jobs (job_id, status, message, timestamp)
                     VALUES (?, ?, ?, ?)''',
                  (job_id, result['status'], result['message'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()

        print(f"ETL Job result at {time.strftime('%Y-%m-%d %H:%M:%S')}: {result}")
    except Exception as e:
        # Lưu trạng thái thất bại vào database
        conn = sqlite3.connect('etl_jobs.db')
        c = conn.cursor()
        c.execute('''INSERT INTO etl_jobs (job_id, status, message, timestamp)
                     VALUES (?, ?, ?, ?)''',
                  (job_id, 'failed', f"Lỗi: {str(e)}", datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        conn.commit()
        conn.close()
        print(f"Lỗi khi chạy ETL Job theo lịch: {str(e)}")


# Route để lấy thống kê ETL jobs
@views.route('/get_etl_stats', methods=['GET'])
def get_etl_stats():
    range_param = request.args.get('range', '1d')  # Mặc định là 1 ngày
    if range_param == '1d':
        days = 1
    elif range_param == '7d':
        days = 7
    elif range_param == '30d':
        days = 30
    else:
        days = 1

    # Tính thời gian bắt đầu
    start_time = datetime.now() - timedelta(days=days)
    start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

    try:
        conn = sqlite3.connect('etl_jobs.db')
        c = conn.cursor()

        # Lấy tất cả job trong khoảng thời gian
        c.execute('''SELECT status, timestamp FROM etl_jobs
                     WHERE timestamp >= ?''', (start_time_str,))
        jobs = c.fetchall()

        # Tính số job thành công và thất bại
        success_jobs = sum(1 for job in jobs if job[0] == 'success')
        failed_jobs = sum(1 for job in jobs if job[0] == 'failed')

        conn.close()
        return jsonify({
            'success_jobs': success_jobs,
            'failed_jobs': failed_jobs
        })
    except Exception as e:
        return jsonify({'error': f"Lỗi khi lấy thống kê: {str(e)}"}), 500