from flask import Blueprint, render_template, current_app, request, session, jsonify
from werkzeug.utils import secure_filename
import os
from resolve.load_file1 import extract_files_task, transform_data_task, load_data_task
from resolve.load_file2 import extract_file2, transform_file2, load_file2

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