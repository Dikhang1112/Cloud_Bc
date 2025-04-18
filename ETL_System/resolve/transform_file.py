import re
import logging
import unicodedata

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

    def clean_value(value):
        """Làm sạch giá trị: giữ ký tự Unicode, thêm khoảng trắng khi loại bỏ ký tự đặc biệt, xử lý số và giá trị bất thường"""
        if not isinstance(value, str):
            value = str(value)

        # Loại bỏ giá trị bất thường
        if value.lower() in ['n/a', 'null', 'na', '-', '']:
            return ''

        # Chuẩn hóa Unicode về dạng NFKC (giữ dấu tiếng Việt)
        value = unicodedata.normalize('NFKC', value)

        # Thay thế ký tự đặc biệt bằng khoảng trắng để giữ tách từ
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
            transformed_value = clean_value(value)
            transformed_record[transformed_headers[j]] = transformed_value
        transformed_records.append(transformed_record)

    logger.info(f"Transformed {len(transformed_records)} records with headers: {transformed_headers}")
    return transformed_records, transformed_headers
