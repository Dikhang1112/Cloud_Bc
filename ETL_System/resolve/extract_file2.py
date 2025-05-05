import os
import pandas as pd
from pathlib import Path
from .extract_file1 import ensure_upload_folder
import numpy as np


def extract_from_excel_or_csv(file_path):
    """Trích xuất dữ liệu từ file Excel hoặc CSV, chuẩn hóa tất cả giá trị thành chuỗi"""
    try:
        if file_path.lower().endswith('.csv'):
            df = pd.read_csv(file_path)
        else:  # .xlsx hoặc .xls
            df = pd.read_excel(file_path)

        # Loại bỏ cột không tên và giá trị null
        df = df.dropna(how='all').dropna(axis=1, how='all')
        if df.empty:
            return [], [], [f"No data found in {file_path}"]

        # Chuyển tất cả giá trị thành chuỗi, thay NaN bằng chuỗi rỗng
        df = df.astype(str).replace('nan', '')

        headers = df.columns.tolist()
        records = df.to_dict('records')
        return records, headers, []
    except Exception as e:
        return [], [], [f"Error extracting from {file_path}: {str(e)}"]


def extract_multiple_files(app_root_path, filenames=None):
    """Trích xuất dữ liệu từ nhiều file Excel/CSV"""
    upload_folder = os.path.join(app_root_path, "Uploads")
    if not os.path.exists(upload_folder):
        return [], [], ["Upload folder does not exist"]

    files = os.listdir(upload_folder)
    if not files:
        return [], [], ["No files found in upload folder"]

    # Nếu filenames được cung cấp, chỉ xử lý các file trong danh sách
    if filenames:
        files = [f for f in files if f in filenames]
        if not files:
            return [], [], ["No specified files found in upload folder"]

    records = []
    headers = None
    errors = []

    for filename in files:
        file_path = os.path.join(upload_folder, filename)
        if filename.lower().endswith(('.xlsx', '.xls', '.csv')):
            file_records, file_headers, file_errors = extract_from_excel_or_csv(file_path)
        else:
            continue

        if file_errors:
            errors.extend(file_errors)
        if file_records:
            if headers is None:
                headers = file_headers
            if file_headers != headers:
                normalized_headers = [h.lower().replace(" ", "") for h in headers]
                normalized_file_headers = [h.lower().replace(" ", "") for h in file_headers]
                if normalized_headers == normalized_file_headers:
                    mapping = {file_headers[i]: headers[i] for i in range(len(file_headers))}
                    for record in file_records:
                        new_record = {mapping[key]: value for key, value in record.items()}
                        records.append(new_record)
                else:
                    errors.append(f"Headers in {filename} do not match: {file_headers} vs {headers}")
                    continue
            else:
                records.extend(file_records)

    return records, headers, errors