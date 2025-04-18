import logging
from .extract_file2 import ensure_upload_folder, extract_multiple_files
from .transform_file import transform_data

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def ensure_upload_folder_file2(app_root_path):
    """Đảm bảo thư mục uploads tồn tại"""
    return ensure_upload_folder(app_root_path)

def extract_file2(app_root_path, filenames=None):
    """Trích xuất dữ liệu từ các file Excel/CSV"""
    logger.info(f"Starting extraction from files in {app_root_path}/Uploads")

    records, headers, errors = extract_multiple_files(app_root_path, filenames)
    if errors:
        for error in errors:
            logger.warning(error)

    if not records:
        if errors:
            logger.warning("No data extracted due to errors")
            return [], [], errors
        else:
            logger.error("No data extracted")
            raise ValueError("No data extracted")

    logger.info(f"Successfully extracted {len(records)} records with headers: {headers}")
    return records, headers, []

def transform_file2(records, headers):
    """Transform dữ liệu đã trích xuất (tái sử dụng transform_data)"""
    logger.info("Starting data transformation")

    transformed_records, transformed_headers = transform_data(records, headers)
    logger.info(f"Transformed {len(transformed_records)} records")
    return transformed_records, transformed_headers

def load_file2(records, headers, get_db_connection, table_name='Data_Transformed1'):
    """Lưu dữ liệu vào bảng trong database"""
    logger.info(f"Starting data loading into {table_name} table")

    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            # Kiểm tra xem bảng có tồn tại không
            cursor.execute("""
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = DATABASE() AND table_name = %s
            """, (table_name,))
            table_exists = cursor.fetchone()['COUNT(*)'] > 0

            if table_exists:
                logger.error(f"Table {table_name} already exists")
                raise ValueError(f"Table {table_name} already exists")

            # Tạo bảng với tên cột dựa trên headers
            if not headers:
                raise ValueError("No headers provided to create table columns")

            columns = [(header, 'VARCHAR(255)') for header in headers]
            create_table_sql = f"""
                CREATE TABLE {table_name} (
                    {', '.join(f'{col[0]} {col[1]}' for col in columns)},
                    PRIMARY KEY ({headers[0]})
                )
            """
            cursor.execute(create_table_sql)

            # Lưu dữ liệu vào các cột tương ứng
            for record in records:
                values = [record.get(header, '') for header in headers]
                placeholders = ', '.join(['%s'] * len(headers))
                columns_str = ', '.join(headers)
                sql = f"""
                    INSERT INTO {table_name} ({columns_str})
                    VALUES ({placeholders})
                """
                cursor.execute(sql, values)

        connection.commit()
        logger.info(f"Successfully loaded {len(records)} records into {table_name}")
        return table_name
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}")
        raise
    finally:
        connection.close()

def main(app_root_path, get_db_connection):
    """Hàm chính để chạy quy trình ETL"""
    ensure_upload_folder_file2(app_root_path)
    records, headers, errors = extract_file2(app_root_path)
    if not records and errors:
        logger.warning(f"ETL failed due to extraction errors: {errors}")
        return [], [], errors
    transformed_records, transformed_headers = transform_file2(records, headers)
    table_name = load_file2(transformed_records, transformed_headers, get_db_connection)
    return transformed_records, transformed_headers