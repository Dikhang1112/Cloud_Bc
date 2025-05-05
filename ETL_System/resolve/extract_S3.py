import boto3
import pandas as pd
from io import StringIO, BytesIO
import docx
import pdfplumber
import openpyxl

def extract_s3(access_key, secret_key, bucket, file_key):
    try:
        # Khởi tạo S3 client với credentials
        s3_client = boto3.client(
            's3',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )

        # Lấy file từ S3
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        file_content = response['Body'].read()

        # Xác định loại file dựa trên đuôi
        file_extension = file_key.lower().split('.')[-1]

        if file_extension == 'csv':
            # Xử lý file CSV
            csv_content = file_content.decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            return df.to_dict(orient='records')

        elif file_extension in ['doc', 'docx']:
            # Xử lý file Word (DOC/DOCX)
            doc = docx.Document(BytesIO(file_content))
            tables = doc.tables
            if not tables:
                raise Exception("No tables found in Word document")

            # Lấy bảng đầu tiên
            table = tables[0]
            data = []
            headers = [cell.text.strip() for cell in table.rows[0].cells]
            for row in table.rows[1:]:
                row_data = {headers[i]: cell.text.strip() for i, cell in enumerate(row.cells)}
                data.append(row_data)
            return data

        elif file_extension == 'pdf':
            # Xử lý file PDF
            with pdfplumber.open(BytesIO(file_content)) as pdf:
                tables = []
                for page in pdf.pages:
                    extracted_tables = page.extract_tables()
                    if extracted_tables:
                        tables.extend(extracted_tables)
                if not tables:
                    raise Exception("No tables found in PDF document")

                # Lấy bảng đầu tiên
                table = tables[0]
                headers = table[0]
                data = []
                for row in table[1:]:
                    row_data = {headers[i]: row[i] if row[i] else '' for i in range(len(headers))}
                    data.append(row_data)
                return data

        elif file_extension in ['xlsx', 'xls']:
            # Xử lý file Excel
            df = pd.read_excel(BytesIO(file_content))
            return df.to_dict(orient='records')

        else:
            raise Exception(f"Unsupported file type: {file_extension}")

    except Exception as e:
        raise Exception(f"Extract error: {str(e)}")