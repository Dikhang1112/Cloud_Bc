// Quản lý trạng thái riêng cho mỗi form
const formStates = {
    wordPdfForm: { extractedData: null, transformedData: null, headers: null },
    excelCsvForm: { extractedData: null, transformedData: null, headers: null }
};

function enableButtons(formId, step) {
    const form = document.getElementById(formId);
    const btnTransform = form.querySelector('.btn-transform');
    const btnLoad = form.querySelector('.btn-load');

    if (step === 'extract') {
        btnTransform.disabled = false;
        btnLoad.disabled = true;
    } else if (step === 'transform') {
        btnTransform.disabled = true;
        btnLoad.disabled = false;
    } else if (step === 'load') {
        btnTransform.disabled = true;
        btnLoad.disabled = true;
        form.reset(); // Reset form sau khi hoàn thành
    }
}

// Hàm tạo bảng HTML từ headers và records
function createTable(headers, records) {
    if (!headers || !records || records.length === 0) {
        return '<p class="text-muted no-data-message">Không có dữ liệu để hiển thị.</p>';
    }

    // Tạo header của bảng
    const thead = `
        <thead>
            <tr>
                ${headers.map(header => `<th>${header}</th>`).join('')}
            </tr>
        </thead>
    `;

    // Tạo body của bảng
    const tbody = `
        <tbody>
            ${records.map(record => `
                <tr>
                    ${headers.map(header => `<td>${record[header] || ''}</td>`).join('')}
                </tr>
            `).join('')}
        </tbody>
    `;

    // Kết hợp thành bảng hoàn chỉnh
    return `
        <div class="table-responsive">
            <table class="table table-custom">
                ${thead}
                ${tbody}
            </table>
        </div>
    `;
}

function extractData(formId) {
    const form = document.getElementById(formId);
    const fileInput = form.querySelector('input[type="file"]');
    const resultSection = document.getElementById('result-section');
    const extractUrl = form.dataset.extractUrl;

    if (!fileInput.files.length) {
        Swal.fire({
            title: 'Lỗi!',
            text: 'Vui lòng chọn ít nhất một file để trích xuất.',
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

    const formData = new FormData(form);
    fetch(extractUrl, {
        method: 'POST',
        body: formData,
        headers: {
            'Cache-Control': 'no-cache'
        }
    })
    .then(response => response.json())
    .then(data => {
        console.log(`Extract response (${formId}):`, data); // Log để debug

        if (data.error) {
            Swal.fire({
                title: 'Lỗi!',
                text: data.error,
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        if (!data.records || !data.headers) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Không tìm thấy dữ liệu trích xuất.',
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        formStates[formId].extractedData = data.records;
        formStates[formId].headers = data.headers;

        // Hiển thị dữ liệu dưới dạng bảng
        resultSection.innerHTML = createTable(data.headers, data.records);

        Swal.fire({
            title: 'Hoàn Tất!',
            text: 'Dữ liệu đã được trích xuất. Nhấn "Chuẩn Hóa" để chuẩn hóa dữ liệu.',
            icon: 'success',
            confirmButtonText: 'OK'
        });

        enableButtons(formId, 'extract');
    })
    .catch(error => {
        console.error(`Extract error (${formId}):`, error); // Log để debug
        Swal.fire({
            title: 'Lỗi!',
            text: 'Đã có lỗi xảy ra khi trích xuất dữ liệu: ' + error.message,
            icon: 'error',
            confirmButtonText: 'OK'
        });
    });
}

function transformData(formId) {
    const form = document.getElementById(formId);
    const resultSection = document.getElementById('result-section');
    const transformUrl = form.dataset.transformUrl;

    if (!formStates[formId].extractedData || !formStates[formId].headers) {
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

    fetch(transformUrl, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache'
        },
        body: JSON.stringify({ records: formStates[formId].extractedData, headers: formStates[formId].headers })
    })
    .then(response => response.json())
    .then(data => {
        console.log(`Transform response (${formId}):`, data); // Log để debug

        if (data.error) {
            Swal.fire({
                title: 'Lỗi!',
                text: data.error,
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        if (!data.records || !data.headers) {
            Swal.fire({
                title: 'Lỗi!',
                text: 'Không tìm thấy dữ liệu chuẩn hóa.',
                icon: 'error',
                confirmButtonText: 'OK'
            });
            return;
        }

        formStates[formId].transformedData = data.records;
        formStates[formId].headers = data.headers;

        // Hiển thị dữ liệu đã chuẩn hóa dưới dạng bảng
        resultSection.innerHTML = createTable(data.headers, data.records);

        Swal.fire({
            title: 'Hoàn Tất!',
            text: 'Dữ liệu đã được chuẩn hóa. Nhấn "Lưu" để lưu vào cơ sở dữ liệu.',
            icon: 'success',
            confirmButtonText: 'OK'
        });

        enableButtons(formId, 'transform');
    })
    .catch(error => {
        console.error(`Transform error (${formId}):`, error); // Log để debug
        Swal.fire({
            title: 'Lỗi!',
            text: 'Đã có lỗi xảy ra khi chuẩn hóa dữ liệu: ' + error.message,
            icon: 'error',
            confirmButtonText: 'OK'
        });
    });
}

function loadData(formId) {
    const form = document.getElementById(formId);
    const resultSection = document.getElementById('result-section');
    const loadUrl = form.dataset.loadUrl;

    if (!formStates[formId].transformedData || !formStates[formId].headers) {
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
        inputLabel: 'Tên bảng (để trống để sử dụng mặc định: Data_Transformed1)',
        inputPlaceholder: 'Nhập tên bảng...',
        showCancelButton: true,
        confirmButtonText: 'Lưu',
        cancelButtonText: 'Hủy',
        inputValidator: (value) => {
            if (value && !/^[a-zA-Z0-9_]+$/.test(value)) {
                return 'Tên bảng chỉ được chứa chữ cái, số và dấu gạch dưới (_)!';
            }
        }
    }).then((result) => {
        if (result.isConfirmed) {
            const tableName = result.value || 'Data_Transformed1';

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

            fetch(loadUrl, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Cache-Control': 'no-cache'
                },
                body: JSON.stringify({ table_name: tableName })
            })
            .then(response => response.json())
            .then(data => {
                console.log(`Load response (${formId}):`, data); // Log để debug

                if (data.error) {
                    Swal.fire({
                        title: 'Lỗi!',
                        text: data.error,
                        icon: 'error',
                        confirmButtonText: 'OK'
                    });
                    return;
                }

                // Hiển thị dữ liệu đã lưu dưới dạng bảng và thêm thông báo
                let tableContent = createTable(formStates[formId].headers, formStates[formId].transformedData);
                tableContent += `<p class="mt-3 text-success">Dữ liệu đã được lưu vào cơ sở dữ liệu: ${data.table_name}</p>`;
                resultSection.innerHTML = tableContent;

                Swal.fire({
                    title: 'Hoàn Tất!',
                    text: `Dữ liệu đã được lưu vào cơ sở dữ liệu: ${data.table_name}.`,
                    icon: 'success',
                    confirmButtonText: 'OK'
                });

                enableButtons(formId, 'load');
            })
            .catch(error => {
                console.error(`Load error (${formId}):`, error); // Log để debug
                Swal.fire({
                    title: 'Lỗi!',
                    text: 'Đã có lỗi xảy ra khi lưu dữ liệu: ' + error.message,
                    icon: 'error',
                    confirmButtonText: 'OK'
                });
            });
        }
    });
}

document.addEventListener('DOMContentLoaded', () => {
    const resultDataElement = document.getElementById('result-data');
    if (!resultDataElement) return;

    const resultData = resultDataElement.dataset.result;
    const step = resultDataElement.dataset.step;

    // Không áp dụng trạng thái chung cho cả hai form
    if (resultData === 'true' && step) {
        console.log(`Initial step: ${step}`);
    }
});