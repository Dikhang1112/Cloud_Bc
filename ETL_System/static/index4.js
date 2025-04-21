document.addEventListener('DOMContentLoaded', () => {
    console.log('DOM fully loaded, initializing script...');

    const s3SelectForm = document.getElementById('s3SelectForm');
    const bucketSelect = document.getElementById('bucketSelect');
    const fileSelect = document.getElementById('fileSelect');
    const extractBtn = document.getElementById('extractBtn');
    const transformBtn = document.getElementById('transformBtn');
    const loadBtn = document.getElementById('loadBtn');
    let extractedData = null;
    let transformedData = null;

    if (!s3SelectForm) {
        console.error('s3SelectForm not found in DOM');
        return;
    }

    // Xử lý form nhập credentials và lấy danh sách bucket
    s3SelectForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        console.log('Form submitted, processing request...');

        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;

        if (!accessKey || !secretKey) {
            console.error('Access Key or Secret Key is empty');
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Access Key và Secret Key là bắt buộc'
            });
            return;
        }

        const payload = { accessKey, secretKey };
        console.log('Sending payload to /get-buckets_S3:', payload);

        try {
            const response = await fetch('/get-buckets_S3', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            console.log('Response status:', response.status);
            const result = await response.json();
            console.log('Response data:', result);

            if (response.ok) {
                bucketSelect.innerHTML = '<option value="">Chọn bucket</option>';
                result.buckets.forEach(bucket => {
                    const option = document.createElement('option');
                    option.value = bucket;
                    option.textContent = bucket;
                    bucketSelect.appendChild(option);
                });
                Swal.fire({
                    icon: 'success',
                    title: 'Thành công',
                    text: 'Đã tải danh sách bucket thành công'
                });
                extractBtn.disabled = false;
            } else {
                console.error('Failed to load buckets:', result.message);
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: result.message || 'Không thể tải danh sách bucket'
                });
            }
        } catch (error) {
            console.error('Fetch error:', error);
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: `Không thể kết nối đến server: ${error.message}`
            });
        }
    });

    // Xử lý khi chọn bucket để lấy danh sách file
    bucketSelect.addEventListener('change', async () => {
        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;
        const bucket = bucketSelect.value;

        if (!bucket) {
            fileSelect.innerHTML = '<option value="">Chọn file</option>';
            return;
        }

        const payload = { accessKey, secretKey, bucket };
        console.log('Sending payload to /get-files_S3:', payload);

        try {
            const response = await fetch('/get-files_S3', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            console.log('Response status:', response.status);
            const result = await response.json();
            console.log('Response data:', result);

            if (response.ok) {
                fileSelect.innerHTML = '<option value="">Chọn file</option>';
                result.files.forEach(file => {
                    const option = document.createElement('option');
                    option.value = file;
                    option.textContent = file;
                    fileSelect.appendChild(option);
                });
            } else {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: result.message || 'Không thể tải danh sách file'
                });
            }
        } catch (error) {
            console.error('Fetch error:', error);
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: `Không thể tải danh sách file: ${error.message}`
            });
        }
    });

    // Xử lý nút Extract
    extractBtn.addEventListener('click', async () => {
        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;
        const bucket = document.getElementById('bucketSelect').value;
        const fileKey = document.getElementById('fileSelect').value;

        try {
            const response = await fetch('/extract_S3', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ accessKey, secretKey, bucket, fileKey })
            });

            const result = await response.json();

            if (response.ok) {
                extractedData = result.data;
                displayData(extractedData);
                Swal.fire({
                    icon: 'success',
                    title: 'Thành công',
                    text: 'Dữ liệu đã được trích xuất thành công'
                });
                transformBtn.disabled = false;
            } else {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: result.message || 'Không thể trích xuất dữ liệu'
                });
            }
        } catch (error) {
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: `Không thể trích xuất dữ liệu: ${error.message}`
            });
        }
    });

    // Xử lý nút Transform
    transformBtn.addEventListener('click', async () => {
        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;
        const bucket = document.getElementById('bucketSelect').value;
        const fileKey = document.getElementById('fileSelect').value;

        try {
            const response = await fetch('/transform_S3', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({ accessKey, secretKey, bucket, fileKey, data: extractedData })
            });

            const result = await response.json();

            if (response.ok) {
                transformedData = result.data;
                displayData(transformedData);
                Swal.fire({
                    icon: 'success',
                    title: 'Thành công',
                    text: 'Dữ liệu đã được chuẩn hóa thành công'
                });
                loadBtn.disabled = false;
            } else {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: result.message || 'Không thể chuẩn hóa dữ liệu'
                });
            }
        } catch (error) {
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: `Không thể chuẩn hóa dữ liệu: ${error.message}`
            });
        }
    });

    // Xử lý nút Load
    loadBtn.addEventListener('click', async () => {
        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;
        const bucket = document.getElementById('bucketSelect').value;

        // Hiển thị pop-up để nhập tên folder, tên file và chọn định dạng
        Swal.fire({
            title: 'Lưu lên S3',
            html: `
                <div>
                    <label for="folderName" class="form-label">Tên thư mục (Tùy chọn)</label>
                    <input type="text" id="folderName" class="swal2-input" placeholder="Ví dụ: output">
                </div>
                <div>
                    <label for="fileName" class="form-label">Tên file (Không cần nhập đuôi)</label>
                    <input type="text" id="fileName" class="swal2-input" placeholder="Ví dụ: processed_data">
                </div>
                <div>
                    <label for="fileFormat" class="form-label">Định dạng file</label>
                    <select id="fileFormat" class="swal2-select">
                        <option value="csv">CSV (.csv)</option>
                        <option value="xlsx">Excel (.xlsx)</option>
                        <option value="json">JSON (.json)</option>
                        <option value="docx">DOCX (.docx)</option>
                        <option value="pdf">PDF (.pdf)</option>
                    </select>
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: 'Lưu',
            cancelButtonText: 'Hủy',
            preConfirm: () => {
                const folderName = document.getElementById('folderName').value.trim();
                const fileName = document.getElementById('fileName').value.trim();
                const fileFormat = document.getElementById('fileFormat').value;

                if (!fileName) {
                    Swal.showValidationMessage('Tên file là bắt buộc');
                    return false;
                }

                // Tạo tên file với đuôi tương ứng
                const fileNameWithExt = `${fileName}.${fileFormat}`;
                const filePath = folderName ? `${folderName}/${fileNameWithExt}` : fileNameWithExt;

                return { filePath };
            }
        }).then(async (result) => {
            if (result.isConfirmed) {
                const { filePath } = result.value;

                try {
                    const response = await fetch('/load_S3', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json'
                        },
                        body: JSON.stringify({ accessKey, secretKey, bucket, file_path: filePath, data: transformedData })
                    });

                    const result = await response.json();

                    if (response.ok) {
                        Swal.fire({
                            icon: 'success',
                            title: 'Thành công',
                            text: `Dữ liệu đã được lưu lên s3://${bucket}/${filePath}`
                        });
                        loadBtn.disabled = true;
                    } else {
                        Swal.fire({
                            icon: 'error',
                            title: 'Lỗi',
                            text: result.message || 'Không thể lưu dữ liệu'
                        });
                    }
                } catch (error) {
                    Swal.fire({
                        icon: 'error',
                        title: 'Lỗi',
                        text: `Không thể lưu dữ liệu: ${error.message}`
                    });
                }
            }
        });
    });

    // Hàm hiển thị dữ liệu trong bảng
    function displayData(data) {
        const tableHeader = document.getElementById('tableHeader');
        const tableBody = document.getElementById('tableBody');

        tableHeader.innerHTML = '';
        tableBody.innerHTML = '';

        if (data.length > 0) {
            // Tạo header
            Object.keys(data[0]).forEach(key => {
                const th = document.createElement('th');
                th.textContent = key;
                tableHeader.appendChild(th);
            });

            // Tạo body
            data.forEach(row => {
                const tr = document.createElement('tr');
                Object.values(row).forEach(value => {
                    const td = document.createElement('td');
                    td.textContent = value;
                    tr.appendChild(td);
                });
                tableBody.appendChild(tr);
            });
        }
    }
});