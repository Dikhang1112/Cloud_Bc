import pandas as pd
import re
from datetime import datetime

def to_camel_case(text):
    # Loại bỏ ký tự đặc biệt (trừ ký tự Unicode tiếng Việt), khoảng trắng thừa
    text = re.sub(r'[^a-zA-ZÀ-ỹ\s]', '', text).strip()
    words = text.split()
    if not words:
        return ''
    # Chuyển từ đầu tiên thành chữ thường, các từ sau viết hoa chữ cái đầu
    result = words[0].lower() + ''.join(word.capitalize() for word in words[1:])
    # Đổi tên cột 'sDt' thành 'sdt' và 'daTe' thành 'date'
    if result == 'sDt':
        return 'sdt'
    if result == 'daTe':
        return 'date'
    return result

def clean_sdt(text):
    # Loại bỏ ký tự đặc biệt (giữ lại số), khoảng trắng thừa
    return re.sub(r'[^0-9]', '', str(text)).strip()

def capitalize_first_letter(text):
    # Loại bỏ ký tự đặc biệt (trừ ký tự Unicode tiếng Việt), khoảng trắng thừa
    text = re.sub(r'[^a-zA-ZÀ-ỹ\s]', '', str(text)).strip()
    if not text:
        return ''
    # Viết hoa chữ cái đầu của mỗi từ
    return ' '.join(word.capitalize() for word in text.split())

def standardize_date(date_str):
    if pd.isnull(date_str) or date_str == '////' or not date_str:
        return None
    try:
        # Loại bỏ ký tự không phải số hoặc /
        date_str = re.sub(r'[^0-9/]', '', date_str)
        date = datetime.strptime(date_str, '%d/%m/%Y')
        return date.strftime('%d/%m/%Y')
    except:
        try:
            # Xử lý định dạng như 25/13/2025
            date_str = date_str.replace(':', '/')
            date = datetime.strptime(date_str, '%d/%m/%Y')
            return date.strftime('%d/%m/%Y')
        except:
            return None

def transform_s3(data):
    try:
        # Chuyển list of dict thành DataFrame
        df = pd.DataFrame(data)

        # Chuẩn hóa header (dạng lạc đà)
        df.columns = [to_camel_case(col) for col in df.columns]

        # Chuẩn hóa dữ liệu
        for col in df.columns:
            if col == 'date':  # Kiểm tra cột ngày tháng
                df[col] = df[col].apply(standardize_date)
            elif col == 'sdt':  # Kiểm tra cột sdt
                df[col] = df[col].apply(clean_sdt)
            else:
                # Chuẩn hóa các cột khác: loại bỏ ký tự đặc biệt, viết hoa chữ cái đầu
                df[col] = df[col].apply(capitalize_first_letter)

        # Chuyển lại thành list of dict
        return df.to_dict(orient='records')

    except Exception as e:
        raise Exception(f"Transform error: {str(e)}")