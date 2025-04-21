import logging
import pymysql
from flask import current_app

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_database(data, database_name, table_name):
    """
    Lưu dữ liệu vào MySQL database một cách động. Luôn xóa bảng cũ (nếu có) và tạo bảng mới trước khi chèn dữ liệu.

    Args:
        data (list): Danh sách dữ liệu đã chuẩn hóa.
        database_name (str): Tên database.
        table_name (str): Tên bảng để lưu.

    Returns:
        dict: Thông báo kết quả.
    """
    if not data:
        logger.warning("No data to load")
        return {"status": "error", "message": "No data to load"}

    if not database_name or not table_name:
        logger.error("Database name and table name are required")
        return {"status": "error", "message": "Database name and table name are required"}

    conn = None
    cursor = None
    try:
        logger.info(f"Attempting to connect to MySQL for loading into {database_name}.{table_name}")
        conn = current_app.get_db_connection()
        logger.info("Successfully connected to MySQL")
        cursor = conn.cursor()

        # Chuyển sang database được chọn
        logger.info(f"Executing USE {database_name}")
        cursor.execute(f"USE {database_name}")
        logger.info(f"Switched to database: {database_name}")

        # Lấy danh sách các cột từ bản ghi đầu tiên
        columns = list(data[0].keys())
        if not columns:
            logger.error("No columns found in data")
            return {"status": "error", "message": "No columns found in data"}

        # Xóa bảng nếu đã tồn tại
        drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
        logger.info(f"Executing drop table query: {drop_table_query}")
        cursor.execute(drop_table_query)
        logger.info(f"Table {table_name} dropped successfully")

        # Tạo bảng mới
        column_definitions = []
        for col in columns:
            if col.lower() == "id":
                column_definitions.append("id VARCHAR(255) PRIMARY KEY")
            else:
                column_definitions.append(f"{col} VARCHAR(255)")
        column_definitions_str = ", ".join(column_definitions)

        create_table_query = f"""
        CREATE TABLE {table_name} (
            {column_definitions_str}
        )
        """
        logger.info(f"Executing create table query: {create_table_query}")
        cursor.execute(create_table_query)
        logger.info(f"Created table {table_name} successfully")

        # Chuẩn bị câu lệnh INSERT
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        insert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        """
        logger.info(f"Insert query prepared: {insert_query}")

        # Chèn dữ liệu
        for item in data:
            values = [item.get(col, "") for col in columns]
            cursor.execute(insert_query, values)

        # Commit giao dịch
        conn.commit()
        logger.info(f"Data loaded into {database_name}.{table_name} successfully")
        return {"status": "success", "message": f"Data loaded into {database_name}.{table_name}"}
    except pymysql.Error as mysql_err:
        logger.error(f"MySQL error loading data into {database_name}.{table_name}: {mysql_err.args[1]} (Error code: {mysql_err.args[0]})")
        return {"status": "error", "message": f"MySQL error: {mysql_err.args[1]}"}
    except Exception as e:
        logger.error(f"Unexpected error loading data into {database_name}.{table_name}: {str(e)} (Type: {type(e).__name__})")
        return {"status": "error", "message": f"Unexpected error: {str(e)}"}
    finally:
        if cursor:
            cursor.close()
            logger.info("Cursor closed")
        if conn:
            conn.close()
            logger.info("MySQL connection closed")
