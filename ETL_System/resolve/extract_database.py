import logging
import pymysql
from flask import current_app

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data_database(database_name, table_name):
    """
    Lấy dữ liệu từ bảng trong MySQL database.

    Args:
        database_name (str): Tên database.
        table_name (str): Tên bảng.

    Returns:
        dict: Dữ liệu từ bảng hoặc thông báo lỗi.
    """
    if not database_name or not table_name:
        logger.error("Database name and table name are required")
        return {"status": "error", "message": "Database name and table name are required"}

    conn = None
    cursor = None
    try:
        logger.info(f"Attempting to connect to MySQL for {database_name}.{table_name}")
        conn = current_app.get_db_connection()
        logger.info("Successfully connected to MySQL")
        cursor = conn.cursor()  # DictCursor từ __init__.py

        # Chuyển sang database được chọn
        logger.info(f"Executing USE {database_name}")
        cursor.execute(f"USE {database_name}")
        logger.info(f"Switched to database: {database_name}")

        # Lấy tất cả dữ liệu từ bảng
        query = f"SELECT * FROM {table_name} LIMIT 100"
        logger.info(f"Executing query: {query}")
        cursor.execute(query)
        data = cursor.fetchall()
        logger.info(f"Extracted {len(data)} records from {table_name}: {data[:2]}")  # Log 2 bản ghi đầu tiên
        return {"status": "success", "data": data}
    except pymysql.Error as mysql_err:
        logger.error(f"MySQL error extracting data from {database_name}.{table_name}: {mysql_err.args[1]} (Error code: {mysql_err.args[0]})")
        return {"status": "error", "message": f"MySQL error: {mysql_err.args[1]}"}
    except Exception as e:
        logger.error(f"Unexpected error extracting data from {database_name}.{table_name}: {str(e)} (Type: {type(e).__name__})")
        return {"status": "error", "message": f"Unexpected error: {str(e)}"}
    finally:
        if cursor:
            cursor.close()
            logger.info("Cursor closed")
        if conn:
            conn.close()
            logger.info("MySQL connection closed")