import os
import docx


def ensure_upload_folder(app_root_path):
    upload_folder = os.path.join(app_root_path, "uploads")
    os.makedirs(upload_folder, exist_ok=True)
    return upload_folder

def extract_from_docx(file_path):
    try:
        doc = docx.Document(file_path)
        records = []
        headers = None
        for table in doc.tables:
            for i, row in enumerate(table.rows):
                if i == 0:  # Dòng đầu tiên là header
                    headers = [cell.text.strip() for cell in row.cells if cell.text.strip()]
                else:
                    record = {}
                    for j, cell in enumerate(row.cells):
                        if j < len(headers):
                            record[headers[j]] = cell.text.strip()
                    if record:
                        records.append(record)
        if not headers:
            return [], [], [f"No table found in {file_path}"]
        return records, headers, []
    except Exception as e:
        return [], [], [f"Error extracting from {file_path}: {str(e)}"]

import pdfplumber

def extract_from_pdf(file_path):
    try:
        with pdfplumber.open(file_path) as pdf:
            records = []
            headers = None
            for page in pdf.pages:
                table = page.extract_table()
                if table:
                    for i, row in enumerate(table):
                        if i == 0:  # Dòng đầu tiên là header
                            headers = [cell.strip() for cell in row if cell]
                        else:
                            if len(row) != len(headers):
                                continue
                            record = {headers[j]: row[j].strip() if row[j] else '' for j in range(len(headers))}
                            records.append(record)
            if not headers:
                return [], [], [f"No table found in {file_path}"]
            return records, headers, []
    except Exception as e:
        return [], [], [f"Error extracting from {file_path}: {str(e)}"]


def extract_multiple_files(app_root_path, filenames=None):
    upload_folder = os.path.join(app_root_path, "uploads")
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
        if filename.lower().endswith('.docx'):
            file_records, file_headers, file_errors = extract_from_docx(file_path)
        elif filename.lower().endswith('.pdf'):
            file_records, file_headers, file_errors = extract_from_pdf(file_path)
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

