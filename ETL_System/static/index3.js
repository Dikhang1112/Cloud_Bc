document.addEventListener('DOMContentLoaded', async function() {
    // Quản lý trạng thái của form
    const formState = {
        extractedData: null,
        transformedData: null,
        headers: null
    };

    // Hàm kích hoạt/tắt các nút dựa trên bước
    function enableButtons(step) {
        const btnTransform = document.getElementById('transformBtn2');
        const btnLoad = document.getElementById('loadBtn2');

        if (step === 'extract') {
            btnTransform.disabled = false;
            btnLoad.disabled = true;
        } else if (step === 'transform') {
            btnTransform.disabled = true;
            btnLoad.disabled = false;
        } else if (step === 'load') {
            btnTransform.disabled = true;
            btnLoad.disabled = true;
            document.getElementById('databaseForm').reset();
            document.getElementById('tableSelect2').disabled = true;
        }
    }

    const extractBtn = document.getElementById('extractBtn2');
    const transformBtn = document.getElementById('transformBtn2');
    const loadBtn = document.getElementById('loadBtn2');
    const databaseSelect = document.getElementById('databaseSelect2');
    const tableSelect = document.getElementById('tableSelect2');
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

        const headers = Object.keys(data[0] || {});
        if (!headers.length) {
            noDataMessage.classList.remove('d-none');
            table.classList.add('d-none');
            return;
        }

        formState.headers = headers;

        tableHead.innerHTML = '';
        const headerRow = document.createElement('tr');
        headers.forEach(header => {
            const th = document.createElement('th');
            th.textContent = header;
            headerRow.appendChild(th);
        });
        tableHead.appendChild(headerRow);

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

    // Tải danh sách database
    async function loadDatabases() {
        try {
            const response = await fetch('/api/databases');
            const result = await response.json();
            if (result.status === 'success') {
                databaseSelect.innerHTML = '<option value="">-- Chọn database --</option>';
                result.databases.forEach(db => {
                    const option = document.createElement('option');
                    option.value = db;
                    option.textContent = db;
                    databaseSelect.appendChild(option);
                });
            } else {
                Swal.fire({
                    title: 'Lỗi!',
                    text: result.message,
                    icon: 'error',
                    confirmButtonText: 'OK'
                });
            }
        } catch (error) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Không thể tải danh sách database: ' + error.message,
                icon: 'error',
                confirmButtonText: 'OK'
            });
        }
    }

    // Tải danh sách bảng khi chọn database
    databaseSelect.addEventListener('change', async function() {
        const database = databaseSelect.value;
        tableSelect.innerHTML = '<option value="">-- Chọn bảng --</option>';
        tableSelect.disabled = true;

        if (database) {
            try {
                const response = await fetch('/api/tables', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ database })
                });
                const result = await response.json();
                if (result.status === 'success') {
                    tableSelect.disabled = false;
                    result.tables.forEach(table => {
                        const option = document.createElement('option');
                        option.value = table;
                        option.textContent = table;
                        tableSelect.appendChild(option);
                    });
                } else {
                    Swal.fire({
                        title: 'Lỗi!',
                        text: result.message,
                        icon: 'error',
                        confirmButtonText: 'OK'
                    });
                }
            } catch (error) {
                Swal.fire({
                    title: 'Lỗi!',
                    text: 'Không thể tải danh sách bảng: ' + error.message,
                    icon: 'error',
                    confirmButtonText: 'OK'
                });
            }
        }
    });

    // Extract: Lấy dữ liệu từ database
    extractBtn.addEventListener('click', async function() {
        const database = databaseSelect.value;
        const table = tableSelect.value;

        if (!database || !table) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Vui lòng chọn database và bảng!',
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
            const response = await fetch('/api/extract_database', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' },
                body: JSON.stringify({ database, table })
            });
            const result = await response.json();

            console.log('Extract response:', result);

            if (result.status === 'success') {
                formState.extractedData = result.data;
                formState.transformedData = null;
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
            const response = await fetch('/api/transform_database', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' },
                body: JSON.stringify({ data: formState.extractedData })
            });
            const result = await response.json();

            console.log('Transform response:', result);

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
            title: 'Nhập thông tin lưu trữ',
            html: `
                <div class="mb-3">
                    <label for="swal-database" class="form-label">Chọn Database:</label>
                    <select class="form-select" id="swal-database">
                        <option value="">-- Chọn database --</option>
                        ${databaseSelect.innerHTML}
                    </select>
                </div>
                <div class="mb-3">
                    <label for="swal-table" class="form-label">Tên bảng mới:</label>
                    <input type="text" class="form-control" id="swal-table" placeholder="Nhập tên bảng...">
                </div>
            `,
            showCancelButton: true,
            confirmButtonText: 'Lưu',
            cancelButtonText: 'Hủy',
            preConfirm: () => {
                const database = document.getElementById('swal-database').value;
                const tableName = document.getElementById('swal-table').value;
                if (!database) {
                    Swal.showValidationMessage('Vui lòng chọn database!');
                    return false;
                }
                if (!tableName) {
                    Swal.showValidationMessage('Vui lòng nhập tên bảng!');
                    return false;
                }
                if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
                    Swal.showValidationMessage('Tên bảng chỉ được chứa chữ cái, số và dấu gạch dưới (_)!');
                    return false;
                }
                return { database, tableName };
            }
        }).then(async (result) => {
            if (result.isConfirmed) {
                const { database, tableName } = result.value;

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
                    const response = await fetch('/api/load_database', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' },
                        body: JSON.stringify({ data: formState.transformedData, database, tableName })
                    });
                    const result = await response.json();

                    console.log('Load response:', result);

                    if (result.status === 'success') {
                        Swal.fire({
                            title: 'Hoàn Tất!',
                            text: `Dữ liệu đã được lưu vào ${database}.${tableName}.`,
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

    // Tải danh sách database khi trang được tải
    await loadDatabases();
});