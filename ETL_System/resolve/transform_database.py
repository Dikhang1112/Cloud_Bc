import logging
import re
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def transform_data_database(data):
    """
    Chuẩn hóa dữ liệu một cách động, áp dụng được cho nhiều loại dữ liệu.

    Args:
        data (list): Danh sách dữ liệu từ bước Extract, phải là danh sách các dictionary.

    Returns:
        list: Danh sách dữ liệu đã được chuẩn hóa.
    """
    logger.info("Starting data transformation")

    # Kiểm tra dữ liệu đầu vào
    if not isinstance(data, list):
        logger.error("Input data must be a list")
        return []

    if not data:
        logger.warning("No data to transform")
        return []

    # Kiểm tra tất cả phần tử là dictionary
    if not all(isinstance(item, dict) for item in data):
        logger.error("All items in data must be dictionaries")
        return []

    # Xử lý trùng lặp ID (nếu có trường 'id')
    seen_ids = set()
    unique_data = []
    for item in data:
        item_id = item.get("id")
        if item_id is None:  # Nếu không có trường 'id', giữ nguyên bản ghi
            unique_data.append(item)
            continue
        if item_id in seen_ids:
            logger.warning(f"Duplicate ID found and removed: {item_id}")
            continue
        seen_ids.add(item_id)
        unique_data.append(item)

    transformed_data = []
    current_date = datetime.strptime("2025-04-19", "%Y-%m-%d")  # Ngày hiện tại

    # Các định dạng ngày hỗ trợ
    date_formats = ["%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"]

    for item in unique_data:
        transformed_item = item.copy()

        # Chuẩn hóa các trường
        for key in transformed_item:
            value = transformed_item.get(key, "")

            # Chuẩn hóa các trường có thể là ngày (nếu tên trường chứa 'date' hoặc 'Date')
            if "date" in key.lower():
                date_value = value
                if not date_value:  # Nếu giá trị rỗng, giữ nguyên
                    transformed_item[key] = ""
                    continue

                # Thử parse với các định dạng ngày
                date_obj = None
                for date_format in date_formats:
                    try:
                        date_obj = datetime.strptime(date_value, date_format)
                        break
                    except (ValueError, TypeError):
                        continue

                if date_obj:
                    # Kiểm tra ngày trong tương lai
                    if date_obj > current_date:
                        logger.warning(
                            f"Future date found for {key} in item {item.get('id', 'unknown')}: {date_value}. Setting to current date.")
                        transformed_item[key] = "2025-04-19"
                    else:
                        transformed_item[key] = date_value
                else:
                    # Nếu không parse được với bất kỳ định dạng nào, đặt thành chuỗi rỗng
                    logger.warning(
                        f"Invalid date format for {key} in item {item.get('id', 'unknown')}: {date_value}. Setting to empty.")
                    transformed_item[key] = ""
                continue

            # Xử lý các giá trị không phải ngày
            if value is None:
                transformed_item[key] = ""
                continue

            value_str = str(value).strip()

            # Kiểm tra giá trị số và giữ dấu chấm cho số thực
            try:
                num_value = float(value_str)
                if num_value < 0:
                    logger.warning(
                        f"Negative number found for {key} in item {item.get('id', 'unknown')}: {num_value}. Converting to positive.")
                    num_value = abs(num_value)
                transformed_item[key] = str(num_value)
                continue
            except (ValueError, TypeError):
                pass  # Không phải số, tiếp tục xử lý làm sạch chuỗi

            # Xóa ký tự đặc biệt dư thừa (giữ chữ cái, số, dấu cách, và dấu chấm cho số thực)
            if value_str:
                # Giữ dấu chấm trong số thực, xóa các ký tự đặc biệt khác
                cleaned_value = re.sub(r'[^\w\s.]', '', value_str)
                # Chuẩn hóa khoảng trắng
                cleaned_value = re.sub(r'\s+', ' ', cleaned_value).strip()
            else:
                cleaned_value = ""

            transformed_item[key] = cleaned_value

        transformed_data.append(transformed_item)

    logger.info(f"Transformed {len(transformed_data)} items successfully")
    return transformed_data