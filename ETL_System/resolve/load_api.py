import logging
from flask import current_app

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_data_api(data, table_name):
    """
    Lưu dữ liệu vào MySQL database một cách động, áp dụng cho nhiều loại dữ liệu.

    Args:
        data (list): Danh sách dữ liệu đã chuẩn hóa.
        table_name (str): Tên bảng do người dùng đặt.

    Returns:
        dict: Thông báo kết quả.
    """
    if not data:
        logger.warning("No data to load")
        return {"status": "error", "message": "No data to load"}

    if not table_name or not table_name.strip():
        logger.error("Table name is empty")
        return {"status": "error", "message": "Table name cannot be empty"}

    try:
        # Lấy kết nối từ app context
        conn = current_app.get_db_connection()
        cursor = conn.cursor()

        # Lấy danh sách các cột từ bản ghi đầu tiên
        if not data:
            return {"status": "error", "message": "Data is empty"}

        columns = list(data[0].keys())
        if not columns:
            return {"status": "error", "message": "No columns found in data"}

        # Tạo bảng động dựa trên các cột
        column_definitions = []
        for col in columns:
            # Nếu cột là 'id', đặt làm PRIMARY KEY, còn lại là VARCHAR(255)
            if col.lower() == "id":
                column_definitions.append("id VARCHAR(255) PRIMARY KEY")
            else:
                column_definitions.append(f"{col} VARCHAR(255)")
        column_definitions_str = ", ".join(column_definitions)

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions_str}
        )
        """
        cursor.execute(create_table_query)

        # Chuẩn bị câu lệnh INSERT động
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        update_str = ", ".join([f"{col} = VALUES({col})" for col in columns if col.lower() != "id"])
        insert_query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({placeholders})
        """
        if update_str:
            insert_query += f" ON DUPLICATE KEY UPDATE {update_str}"

        # Chèn dữ liệu
        for item in data:
            values = [item.get(col, "") for col in columns]
            cursor.execute(insert_query, values)

        # Commit và đóng kết nối
        conn.commit()
        logger.info(f"Data loaded into table {table_name} successfully")
        return {"status": "success", "message": f"Data loaded into table {table_name}"}
    except Exception as e:
        logger.error(f"Error loading data into MySQL database: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        if 'conn' in locals():
            cursor.close()
            conn.close()