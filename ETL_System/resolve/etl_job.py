import boto3
from botocore.exceptions import ClientError
import importlib.util
from pathlib import Path


# Hàm tải danh sách S3 buckets
def load_s3_buckets(access_key, secret_key):
    try:
        print(f"Khởi tạo S3 client với Access Key: {access_key}")
        # Nếu không có Access Key và Secret Key, thử dùng IAM Role (trên EC2)
        if not access_key or not secret_key:
            print("Không có Access Key/Secret Key, thử dùng IAM Role...")
            s3_client = boto3.client('s3')
        else:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )

        print("Gọi API list_buckets...")
        response = s3_client.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        print(f"Danh sách buckets: {buckets}")
        return buckets
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        raise Exception(f"Lỗi xác thực AWS (Code: {error_code}): {error_message}")
    except Exception as e:
        raise Exception(f"Không thể tải S3 buckets: {str(e)}")


# Hàm tải dữ liệu từ folder input trong S3
def download_from_s3(s3_client, bucket, input_folder='input'):
    try:
        print(f"Tải dữ liệu từ bucket {bucket}, folder {input_folder}")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=input_folder)
        if 'Contents' not in response:
            print("Không có file nào trong folder input")
            return []

        local_files = []
        for obj in response['Contents']:
            key = obj['Key']
            if key == input_folder + '/':
                continue
            local_path = Path('temp') / Path(key).name
            local_path.parent.mkdir(parents=True, exist_ok=True)  # Tạo thư mục cha nếu chưa tồn tại
            print(f"Tải file {key} về {local_path}")
            s3_client.download_file(bucket, key, str(local_path))
            local_files.append(str(local_path))
        return local_files
    except ClientError as e:
        raise Exception(f"Không thể tải dữ liệu từ S3: {str(e)}")


# Hàm upload dữ liệu lên folder output trong S3
def upload_to_s3(s3_client, bucket, local_file, output_folder='output'):
    try:
        # Tạo key với dấu / làm phân cách, đảm bảo file nằm trong thư mục output/
        key = f"{output_folder}/{Path(local_file).name}"
        print(f"Upload file {local_file} lên S3 bucket {bucket} với key {key}")
        s3_client.upload_file(str(local_file), bucket, key)
    except ClientError as e:
        raise Exception(f"Không thể upload lên S3: {str(e)}")


# Hàm gọi script ETL từ file Python được upload
def run_etl_script(script_path, input_file, output_file):
    try:
        print(f"Thực thi script ETL: {script_path}")
        # Import động script Python từ file
        spec = importlib.util.spec_from_file_location("etl_script", script_path)
        etl_script = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(etl_script)

        # Gọi hàm process_etl từ script
        if hasattr(etl_script, 'process_etl'):
            return etl_script.process_etl(input_file, output_file)
        else:
            raise Exception("Script không có hàm process_etl")
    except Exception as e:
        raise Exception(f"Không thể thực thi script ETL: {str(e)}")


# Hàm chạy ETL Job (đồng bộ, không dùng Celery)
def run_etl_job(access_key, secret_key, bucket):
    try:
        # Khởi tạo S3 client
        print("Khởi tạo S3 client trong run_etl_job")
        if not access_key or not secret_key:
            print("Không có Access Key/Secret Key, thử dùng IAM Role...")
            s3_client = boto3.client('s3')
        else:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )

        # Tải dữ liệu từ folder input
        input_files = download_from_s3(s3_client, bucket)
        if not input_files:
            return {'status': 'failed', 'message': f"Không có file nào trong folder input của bucket {bucket}"}

        # Tìm script Python trong thư mục etl_data
        script_path = None
        etl_data_dir = Path('etl_data')
        for file in etl_data_dir.iterdir():
            if file.suffix == '.py':
                script_path = file
                break
        if not script_path:
            return {'status': 'failed', 'message': "Không tìm thấy script Python trong thư mục etl_data"}

        # Transform dữ liệu bằng script
        output_files = []
        for input_file in input_files:
            if input_file.endswith('.docx'):
                output_file = Path('temp') / f"transformed_{Path(input_file).name.replace('.docx', '.csv')}"
                output_file, log_file = run_etl_script(str(script_path), input_file, str(output_file))
                output_files.append(output_file)
                if log_file:
                    output_files.append(log_file)

        # Upload kết quả lên folder output
        for output_file in output_files:
            upload_to_s3(s3_client, bucket, output_file)

        # Xóa file tạm
        for file in input_files + output_files:
            file_path = Path(file)
            if file_path.exists():
                file_path.unlink()

        return {'status': 'success', 'message': f"ETL Job hoàn thành cho bucket {bucket}"}
    except Exception as e:
        return {'status': 'failed', 'message': f"Lỗi khi chạy ETL Job: {str(e)}"}