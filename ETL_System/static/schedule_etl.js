document.addEventListener('DOMContentLoaded', () => {
    let currentJobId = null;
    let cancelButton = null;
    let jobChart = null;

    // Tải danh sách S3 buckets
    const loadS3Btn = document.getElementById('loadS3Btn');
    const etlForm = document.getElementById('etlForm');
    const s3BucketsSelect = document.getElementById('s3Buckets');
    const reportRangeSelect = document.getElementById('reportRange');
    const successJobsElement = document.getElementById('successJobs');
    const failedJobsElement = document.getElementById('failedJobs');
    const jobChartCanvas = document.getElementById('jobChart'); // Lấy phần tử canvas
    const jobChartContext = jobChartCanvas.getContext('2d'); // Lấy context

    // Kiểm tra trạng thái job khi trang được tải
    currentJobId = localStorage.getItem('currentJobId');
    if (currentJobId) {
        createCancelButton();
        Swal.fire({
            icon: 'info',
            title: 'Job đang chạy',
            text: `Có một ETL Job đang chạy với ID: ${currentJobId}`,
        });
    }

    // Hàm tạo nút Hủy Lịch Chạy
    function createCancelButton() {
        if (!cancelButton) {
            cancelButton = document.createElement('button');
            cancelButton.type = 'button';
            cancelButton.className = 'btn btn-danger w-100 mt-3';
            cancelButton.innerHTML = '<i class="fas fa-stop me-2"></i>Hủy Lịch Chạy';
            cancelButton.addEventListener('click', cancelEtlJob);
            etlForm.appendChild(cancelButton);
        }
    }

    // Hàm khởi tạo biểu đồ tròn
    function initChart(successJobs, failedJobs) {
        if (jobChart) {
            jobChart.destroy();
        }

        // Nếu không có dữ liệu, hiển thị thông báo trên canvas
        if (successJobs === 0 && failedJobs === 0) {
            jobChartContext.font = '16px Arial';
            jobChartContext.fillText('Chưa có dữ liệu để hiển thị', 50, 100);
            return;
        }

        jobChart = new Chart(jobChartCanvas, {
            type: 'pie',
            data: {
                labels: ['Jobs Thành Công', 'Jobs Thất Bại'],
                datasets: [{
                    data: [successJobs, failedJobs],
                    backgroundColor: [
                        'rgba(40, 167, 69, 0.8)',  // Màu xanh cho Jobs Thành Công
                        'rgba(220, 53, 69, 0.8)'   // Màu đỏ cho Jobs Thất Bại
                    ],
                    borderColor: [
                        'rgba(40, 167, 69, 1)',
                        'rgba(220, 53, 69, 1)'
                    ],
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'top',
                    },
                    title: {
                        display: true,
                        text: 'Tỷ lệ Jobs Thành Công và Thất Bại'
                    }
                }
            }
        });
    }

    // Hàm lấy dữ liệu thống kê từ server
    function fetchEtlStats() {
        const range = reportRangeSelect.value || '1d';
        fetch(`/get_etl_stats?range=${range}`)
            .then(response => {
                if (!response.ok) {
                    throw new Error(`HTTP error! Status: ${response.status}`);
                }
                return response.json();
            })
            .then(data => {
                if (data.error) {
                    Swal.fire({
                        icon: 'error',
                        title: 'Lỗi',
                        text: data.error,
                    });
                } else {
                    // Cập nhật số liệu
                    successJobsElement.textContent = data.success_jobs;
                    failedJobsElement.textContent = data.failed_jobs;

                    // Cập nhật biểu đồ
                    initChart(data.success_jobs, data.failed_jobs);
                }
            })
            .catch(error => {
                console.error('Error fetching ETL stats:', error);
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: 'Không thể lấy dữ liệu thống kê: ' + error.message,
                });
            });
    }

    // Gọi fetchEtlStats khi trang được tải và tự động làm mới mỗi 30 giây
    fetchEtlStats();
    setInterval(fetchEtlStats, 180000); // Làm mới mỗi 180 giây
    reportRangeSelect.addEventListener('change', fetchEtlStats);

    loadS3Btn.addEventListener('click', () => {
        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;

        if (!accessKey || !secretKey) {
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Vui lòng nhập Access Key và Secret Access Key!',
            });
            return;
        }

        console.log('Gửi yêu cầu lấy S3 buckets với:', { access_key: accessKey, secret_key: secretKey });

        fetch('/fetch_s3_buckets', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ access_key: accessKey, secret_key: secretKey })
        })
        .then(response => response.json().catch(() => {
            throw new Error(`Phản hồi từ server không phải JSON, status: ${response.status}`);
        }))
        .then(data => {
            console.log('Dữ liệu nhận được:', data);
            if (data.error) {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: data.error,
                });
            } else if (!data.buckets) {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: 'Không nhận được danh sách buckets từ server',
                });
            } else {
                s3BucketsSelect.innerHTML = '<option value="">Chọn bucket</option>';
                data.buckets.forEach(bucket => {
                    const option = document.createElement('option');
                    option.value = bucket;
                    option.textContent = bucket;
                    s3BucketsSelect.appendChild(option);
                });
                s3BucketsSelect.disabled = false;
                Swal.fire({
                    icon: 'success',
                    title: 'Thành công',
                    text: 'Đã tải danh sách S3 buckets!',
                });
            }
        })
        .catch(error => {
            console.error('Error fetching S3 buckets:', error);
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Không thể tải S3 buckets: ' + error.message,
            });
        });
    });

    // Xử lý form chạy ETL Job
    etlForm.addEventListener('submit', (event) => {
        event.preventDefault();

        const accessKey = document.getElementById('accessKey').value;
        const secretKey = document.getElementById('secretKey').value;
        const bucket = document.getElementById('s3Buckets').value;
        const scheduleTime = document.getElementById('scheduleTime').value;
        const scriptFile = document.getElementById('scriptFile').files[0];

        if (!accessKey || !secretKey || !bucket || !scheduleTime || !scriptFile) {
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Vui lòng điền đầy đủ thông tin!',
            });
            return;
        }

        const formData = new FormData();
        formData.append('access_key', accessKey);
        formData.append('secret_key', secretKey);
        formData.append('bucket', bucket);
        formData.append('schedule_time', scheduleTime);
        formData.append('script_file', scriptFile);

        fetch('/schedule_etl', {
            method: 'POST',
            body: formData
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.error) {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: data.error,
                });
            } else {
                currentJobId = data.job_id;
                // Lưu currentJobId vào localStorage
                localStorage.setItem('currentJobId', currentJobId);
                Swal.fire({
                    icon: 'success',
                    title: 'Thành công',
                    text: data.message,
                    showCancelButton: true,
                    cancelButtonText: 'Hủy Lịch',
                    showConfirmButton: true,
                    confirmButtonText: 'OK',
                }).then((result) => {
                    if (result.dismiss === Swal.DismissReason.cancel) {
                        cancelEtlJob();
                    }
                });

                // Thêm nút Hủy Lịch vào form
                createCancelButton();
            }
        })
        .catch(error => {
            console.error('Error running ETL job:', error);
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Không thể chạy ETL Job: ' + error.message,
            });
        });
    });

    // Hàm hủy lịch chạy ETL Job
    function cancelEtlJob() {
        if (!currentJobId) {
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Không có lịch chạy để hủy!',
            });
            return;
        }

        fetch('/cancel_etl', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_id: currentJobId })
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! Status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            if (data.error) {
                Swal.fire({
                    icon: 'error',
                    title: 'Lỗi',
                    text: data.error,
                });
            } else {
                Swal.fire({
                    icon: 'success',
                    title: 'Thành công',
                    text: data.message,
                });
                currentJobId = null;
                // Xóa currentJobId khỏi localStorage
                localStorage.removeItem('currentJobId');
                if (cancelButton) {
                    cancelButton.remove();
                    cancelButton = null;
                }
            }
        })
        .catch(error => {
            console.error('Error cancelling ETL job:', error);
            Swal.fire({
                icon: 'error',
                title: 'Lỗi',
                text: 'Không thể hủy lịch ETL Job: ' + error.message,
            });
        });
    }
});