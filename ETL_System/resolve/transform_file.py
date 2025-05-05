import re
import logging
import unicodedata
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_data(records, headers):
    """
    Transform dữ liệu: chuẩn hóa headers thành camelCase, làm sạch và chuyển giá trị thành uppercase.

    Args:
        records: List[dict], danh sách bản ghi với key là header gốc.
        headers: List[str], danh sách tên cột.

    Returns:
        transformed_records: List[dict], bản ghi đã transform với header mới.
        transformed_headers: List[str], header đã chuẩn hóa thành camelCase.
    """
    if not records or not headers:
        logger.warning("No records or headers provided for transformation")
        return [], []


    def clean_value(value, header=None):
        """Làm sạch giá trị: giữ ký tự Unicode, thêm khoảng trắng khi loại bỏ ký tự đặc biệt, xử lý số, ngày sinh và giá trị bất thường"""

        if not isinstance(value, str):
            value = str(value)

        # Loại bỏ giá trị bất thường
        if value.lower() in ['n/a', 'null', 'na', '-', '']:
            return ''

        # Chuẩn hóa Unicode về dạng NFKC (giữ dấu tiếng Việt)
        value = unicodedata.normalize('NFKC', value)

        # Xử lý ngày sinh nếu header liên quan đến ngày (ví dụ: 'birthday', 'date')
        if header and ('birth' in header.lower() or 'date' in header.lower()):
            # Kiểm tra định dạng ngày: YYYY/MM/DD, YYYY-MM-DD, DD/MM/YYYY, hoặc DD-MM-YYYY
            # Định dạng 1: YYYY/MM/DD hoặc YYYY-MM-DD
            date_pattern_1 = r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})'
            # Định dạng 2: DD/MM/YYYY hoặc DD-MM-YYYY
            date_pattern_2 = r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})'

            # Thử định dạng YYYY/MM/DD
            match_1 = re.match(date_pattern_1, value)
            if match_1:
                year, month, day = match_1.groups()
                try:
                    # Xác thực ngày hợp lệ
                    datetime(int(year), int(month), int(day))
                    # Chuẩn hóa thành YYYY-MM-DD
                    return f'{year}-{month.zfill(2)}-{day.zfill(2)}'
                except ValueError:
                    logger.warning(f"Invalid date format for {header}: {value}")
                    return ''

            # Thử định dạng DD/MM/YYYY
            match_2 = re.match(date_pattern_2, value)
            if match_2:
                day, month, year = match_2.groups()
                try:
                    # Xác thực ngày hợp lệ
                    datetime(int(year), int(month), int(day))
                    # Chuẩn hóa thành YYYY-MM-DD
                    return f'{year}-{month.zfill(2)}-{day.zfill(2)}'
                except ValueError:
                    logger.warning(f"Invalid date format for {header}: {value}")
                    return ''

            # Nếu không khớp với bất kỳ định dạng nào, trả về rỗng
            logger.warning(f"Unrecognized date format for {header}: {value}")
            return ''
        value = re.sub(r'[^\w\s]', ' ', value, flags=re.UNICODE)

        # Loại bỏ khoảng trắng thừa
        value = ' '.join(value.strip().split())

        # Kiểm tra xem giá trị có phải là số không
        try:
            float_val = float(value)
            if float_val.is_integer():
                return str(int(float_val))  # Số nguyên: "123.0" → "123"
            return str(round(float_val, 2))  # Số thập phân: "123.456789" → "123.46"
        except ValueError:
            pass

        # Chuyển thành uppercase
        return value.upper()

    # Chuẩn hóa headers thành camelCase
    transformed_headers = []
    for header in headers:
        words = header.lower().replace(' ', '_').split('_')
        transformed_header = words[0] + ''.join(word.capitalize() for word in words[1:])
        transformed_headers.append(transformed_header)

    # Chuẩn hóa dữ liệu
    transformed_records = []
    for record in records:
        transformed_record = {}
        for j, header in enumerate(headers):
            value = record.get(header, '')
            transformed_value = clean_value(value, header)  # Truyền header để kiểm tra ngày sinh
            transformed_record[transformed_headers[j]] = transformed_value
        transformed_records.append(transformed_record)

    logger.info(f"Transformed {len(transformed_records)} records with headers: {transformed_headers}")
    return transformed_records, transformed_headers
