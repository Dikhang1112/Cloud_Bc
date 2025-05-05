document.addEventListener('DOMContentLoaded', function() {
    // Quản lý trạng thái của form
    const formState = {
        extractedData: null,
        transformedData: null,
        headers: null
    };

    // Hàm kích hoạt/tắt các nút dựa trên bước
    function enableButtons(step) {
        const btnTransform = document.getElementById('transformBtn');
        const btnLoad = document.getElementById('loadBtn');

        if (step === 'extract') {
            btnTransform.disabled = false;
            btnLoad.disabled = true;
        } else if (step === 'transform') {
            btnTransform.disabled = true;
            btnLoad.disabled = false;
        } else if (step === 'load') {
            btnTransform.disabled = true;
            btnLoad.disabled = true;
            document.getElementById('urlForm').reset(); // Reset form sau khi hoàn thành
        }
    }

    const extractBtn = document.getElementById('extractBtn');
    const transformBtn = document.getElementById('transformBtn');
    const loadBtn = document.getElementById('loadBtn');
    const apiUrlInput = document.getElementById('apiUrl');
    const tableNameInput = document.getElementById('tableName');
    const noDataMessage = document.getElementById('noDataMessage');
    const dataTable = document.getElementById('dataTable');
    const dataTableHead = document.getElementById('dataTableHead');
    const dataTableBody = document.getElementById('dataTableBody');

    // Hàm hiển thị dữ liệu lên bảng động
    function displayData(data, table, tableHead, tableBody, noDataMessage) {
        if (!data || data.length === 0) {
            noDataMessage.classList.remove('d-none');
            table.classList.add('d-none');
            return;
        }

        // Lấy danh sách cột từ bản ghi đầu tiên
        const headers = Object.keys(data[0] || {});
        if (!headers.length) {
            noDataMessage.classList.remove('d-none');
            table.classList.add('d-none');
            return;
        }

        // Cập nhật headers trong formState
        formState.headers = headers;

        // Tạo tiêu đề bảng động
        tableHead.innerHTML = '';
        const headerRow = document.createElement('tr');
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            headerRow.appendChild(th);
        });
        tableHead.appendChild(headerRow);

        // Hiển thị dữ liệu
        noDataMessage.classList.add('d-none');
        table.classList.remove('d-none');
        tableBody.innerHTML = '';

        data.forEach(item => {
            const row = document.createElement('tr');
            headers.forEach(header => {
                const td = document.createElement('td');
                td.textContent = item[header] || '';
                row.appendChild(td);
            });
            tableBody.appendChild(row);
        });
    }

    // Extract: Gọi API để lấy dữ liệu nguyên thủy
    extractBtn.addEventListener('click', async function() {
        const url = apiUrlInput.value.trim();
        if (!url) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Vui lòng nhập URL!',
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        Swal.fire({
            title: 'Đang trích xuất...',
            text: 'Vui lòng chờ trong giây lát.',
            icon: 'info',
            showConfirmButton: false,
            allowOutsideClick: false,
            didOpen: () => {
                Swal.showLoading();
            }
        });

        try {
            const response = await fetch('/api/extract', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache'
                },
                body: JSON.stringify({ apiUrl: url }),
            });
            const result = await response.json();

            console.log('Extract response:', result); // Log để debug

            if (result.status === 'success') {
                formState.extractedData = result.data;
                formState.transformedData = null; // Reset dữ liệu chuẩn hóa
                displayData(formState.extractedData, dataTable, dataTableHead, dataTableBody, noDataMessage);
                Swal.fire({
                    title: 'Hoàn Tất!',
                    text: 'Dữ liệu đã được trích xuất. Nhấn "Transform" để chuẩn hóa dữ liệu.',
                    icon: 'success',
                    confirmButtonText: 'OK'
                });
                enableButtons('extract');
            } else {
                Swal.fire({
                    title: 'Lỗi!',
                    text: result.message,
                    icon: 'error',
                    confirmButtonText: 'OK'
                });
            }
        } catch (error) {
            console.error('Extract error:', error);
            Swal.fire({
                title: 'Lỗi!',
                text: 'Đã có lỗi xảy ra khi trích xuất dữ liệu: ' + error.message,
                icon: 'error',
                confirmButtonText: 'OK'
            });
        }
    });

    // Transform: Chuẩn hóa dữ liệu
    transformBtn.addEventListener('click', async function() {
        if (!formState.extractedData) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Vui lòng trích xuất dữ liệu trước khi chuẩn hóa.',
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        Swal.fire({
            title: 'Đang chuẩn hóa...',
            text: 'Vui lòng chờ trong giây lát.',
            icon: 'info',
            showConfirmButton: false,
            allowOutsideClick: false,
            didOpen: () => {
                Swal.showLoading();
            }
        });

        try {
            const response = await fetch('/api/transform', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache'
                },
                body: JSON.stringify({ data: formState.extractedData }),
            });
            const result = await response.json();

            console.log('Transform response:', result); // Log để debug

            if (result.status === 'success') {
                formState.transformedData = result.data;
                displayData(formState.transformedData, dataTable, dataTableHead, dataTableBody, noDataMessage);
                Swal.fire({
                    title: 'Hoàn Tất!',
                    text: 'Dữ liệu đã được chuẩn hóa. Nhấn "Load" để lưu vào cơ sở dữ liệu.',
                    icon: 'success',
                    confirmButtonText: 'OK'
                });
                enableButtons('transform');
            } else {
                Swal.fire({
                    title: 'Lỗi!',
                    text: result.message,
                    icon: 'error',
                    confirmButtonText: 'OK'
                });
            }
        } catch (error) {
            console.error('Transform error:', error);
            Swal.fire({
                title: 'Lỗi!',
                text: 'Đã có lỗi xảy ra khi chuẩn hóa dữ liệu: ' + error.message,
                icon: 'error',
                confirmButtonText: 'OK'
            });
        }
    });

    // Load: Lưu dữ liệu vào database
    loadBtn.addEventListener('click', async function() {
        if (!formState.transformedData) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Vui lòng chuẩn hóa dữ liệu trước khi lưu.',
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        Swal.fire({
            title: 'Nhập tên bảng',
            input: 'text',
            inputLabel: 'Tên bảng (để trống để sử dụng mặc định: Data_Transformed)',
            inputPlaceholder: 'Nhập tên bảng...',
            showCancelButton: true,
            confirmButtonText: 'Lưu',
            cancelButtonText: 'Hủy',
            inputValidator: (value) => {
                if (value && !/^[a-zA-Z0-9_]+$/.test(value)) {
                    return 'Tên bảng chỉ được chứa chữ cái, số và dấu gạch dưới (_)!';
                }
            }
        }).then(async (result) => {
            if (result.isConfirmed) {
                const tableName = result.value || 'Data_Transformed';

                Swal.fire({
                    title: 'Đang lưu dữ liệu...',
                    text: 'Vui lòng chờ trong giây lát.',
                    icon: 'info',
                    showConfirmButton: false,
                    allowOutsideClick: false,
                    didOpen: () => {
                        Swal.showLoading();
                    }
                });

                try {
                    const response = await fetch('/api/load', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                            'Cache-Control': 'no-cache'
                        },
                        body: JSON.stringify({ data: formState.transformedData, tableName: tableName }),
                    });
                    const result = await response.json();

                    console.log('Load response:', result); // Log để debug

                    if (result.status === 'success') {
                        Swal.fire({
                            title: 'Hoàn Tất!',
                            text: `Dữ liệu đã được lưu vào cơ sở dữ liệu: ${tableName}.`,
                            icon: 'success',
                            confirmButtonText: 'OK'
                        });
                        enableButtons('load');
                    } else {
                        Swal.fire({
                            title: 'Lỗi!',
                            text: result.message,
                            icon: 'error',
                            confirmButtonText: 'OK'
                        });
                    }
                } catch (error) {
                    console.error('Load error:', error);
                    Swal.fire({
                        title: 'Lỗi!',
                        text: 'Đã có lỗi xảy ra khi lưu dữ liệu: ' + error.message,
                        icon: 'error',
                        confirmButtonText: 'OK'
                    });
                }
            }
        });
    });
});