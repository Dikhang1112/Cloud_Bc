import logging
import re
from datetime import datetime

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def transform_data_api(data):
    """
    Chuẩn hóa dữ liệu một cách động, áp dụng được cho nhiều loại dữ liệu.

    Args:
        data (list): Danh sách dữ liệu từ bước Extract, phải là danh sách các dictionary.

    Returns:
        list: Danh sách dữ liệu đã được chuẩn hóa.
    """
    # Kiểm tra dữ liệu đầu vào
    if not isinstance(data, list):
        logger.error("Dữ liệu đầu vào phải là một danh sách")
        return []

    if not data:
        logger.warning("Không có dữ liệu để chuẩn hóa")
        return []

    # Kiểm tra tất cả phần tử là dictionary
    if not all(isinstance(item, dict) for item in data):
        logger.error("Tất cả phần tử trong dữ liệu phải là dictionary")
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
            logger.warning(f"Phát hiện và xóa ID trùng lặp: {item_id}")
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

            # Bảo vệ trường 'id', giữ nguyên giá trị gốc
            if key.lower() == "id":
                transformed_item[key] = value
                continue

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
                            f"Phát hiện ngày trong tương lai cho {key} trong mục {item.get('id', 'không xác định')}: {date_value}. Đặt thành ngày hiện tại.")
                        transformed_item[key] = "2025-04-19"
                    else:
                        transformed_item[key] = date_value
                else:
                    # Nếu không parse được với bất kỳ định dạng nào, đặt thành chuỗi rỗng
                    logger.warning(
                        f"Định dạng ngày không hợp lệ cho {key} trong mục {item.get('id', 'không xác định')}: {date_value}. Đặt thành rỗng.")
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
                        f"Phát hiện số âm cho {key} trong mục {item.get('id', 'không xác định')}: {num_value}. Chuyển thành số dương.")
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

    logger.info(f"Đã chuẩn hóa {len(transformed_data)} mục")
    return transformed_data