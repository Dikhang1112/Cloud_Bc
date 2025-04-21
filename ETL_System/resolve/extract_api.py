import requests
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data_api(api_url):
    """
    Lấy dữ liệu nguyên thủy từ API (JSON Server).

    Args:
        api_url (str): URL của API (ví dụ: http://localhost:3000/products).

    Returns:
        dict: Dữ liệu JSON nguyên thủy hoặc thông báo lỗi.
    """
    try:
        logger.info(f"Extracting data from {api_url}")
        response = requests.get(api_url)
        response.raise_for_status()  # Ném lỗi nếu HTTP status code không phải 200
        data = response.json()
        logger.info("Data extracted successfully")
        return {"status": "success", "data": data}
    except requests.exceptions.RequestException as e:
        logger.error(f"Error extracting data: {e}")
        return {"status": "error", "message": str(e)}