# REPORT — Lab Day 9

| | |
|---|---|
| Họ và tên | Võ Minh Quân |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602429` |
| GitHub | `vminhquan` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02086` (lấy phần số `202602429` của MSSV: `202602429 % 26 = 3`, phần tử thứ 4 trong danh sách trip đã sắp xếp) |

## Kết quả cuối

- `dataset_version` của bạn: `fffdeb8d8f2a` (đối chiếu `report/evidence/run_summary.json`).
- Số khung hình train / val / test của bạn: **738 / 129 / 153** (tổng 1.020, từ 26 trip).
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A có các cột `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `gas`, `brake`; nguồn B tương ứng dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `throttle_pct`, `brake_pct`. A ghi thời gian ISO UTC và tốc độ m/s; B ghi epoch mili giây, tốc độ km/h, bàn đạp theo phần trăm, đồng hồ nhanh hơn video 1.500 ms. Hai bảng Bronze riêng giữ nguyên schema và giá trị gốc để có thể truy vết, nạp lại theo SHA-256, cũng như điều chỉnh quy tắc chuẩn hóa mà không mất thông tin thô. Cột dạng chữ giữ được cả ô trống và giá trị lỗi để Silver phân loại vào quarantine.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi `spd_kmh` của B sang m/s bằng cách chia 3,6; đổi `timestamp_ms` sang UTC rồi trừ 1.500 ms theo `bronze.video_files`; đổi các tên cột và chia `throttle_pct`, `brake_pct` cho 100. Nguồn A đã dùng m/s và ISO UTC. Sau chuẩn hóa, B bắt đầu lúc **2025-10-08 14:30:00.009 UTC**, lệch video chỉ 9 ms; A bắt đầu lúc **2025-10-07 08:00:00.014 UTC**. Silver sạch có 2.968 dòng A và 3.181 dòng B.

| Drive | Trùng | Thiếu tốc độ | Tốc độ ngoài miền | Tổng cách ly |
|---|---:|---:|---:|---:|
| A | 20 | 30 | 3 | 53 |
| B | 20 | 30 | 3 | 53 |

Đối chiếu đầu vào: 2.968 + 53 = 3.021 (A), 3.181 + 53 = 3.234 (B). `silver_telemetry_rejects` giữ dòng lỗi và `reject_reason` để truy vết, kiểm toán, sửa luật và xử lý lại; xóa im lặng sẽ làm mất khả năng giải thích chênh lệch số dòng.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` phát hiện khoảng mất tín hiệu đã biết: cần cảnh báo để điều tra nhưng các đoạn khác vẫn dùng được. `in_range` phát hiện tốc độ ngoài 0–70 m/s, là giá trị không hợp lệ cho dataset và phải dừng dòng chảy. Khi nới luật làm sạch lên 1.000 m/s, `dbt test` trả `PASS=5 WARN=1 ERROR=1`: test `in_range_silver_telemetry_speed_mps__70__0` bắt **6 dòng** (3 mỗi drive). Khôi phục luật 70 m/s thì `PASS=6 WARN=1 ERROR=0`. Cổng chất lượng độc lập với bước làm sạch: nếu quy tắc làm sạch sai, test vẫn ngăn dữ liệu xấu đi tiếp.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Với `trip-02086`, bảng Silver frames có **1.246 dòng** trước và sau hai lần trích lại; dấu vân tay đều là `cb6118dbb0b8`. Trip này có 60 khung. Ghi `append` hai lần vào bảng thử tạo **120 dòng**, gấp đôi. Delta `overwrite` theo predicate `trip_id` thay đúng một partition bằng một commit nguyên tử; khi ghi lỗi giữa chừng, bản đã commit vẫn đọc được. Cách xoá trước rồi ghi sau có khoảng trống có thể làm mất trip nếu job sập, còn `append` tạo bản sao khi retry.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 60 | 8 |
| 2 | 149 | 52 |
| 6 | 363 | 244 |

Khung đen có độ sáng trung bình dưới 12, thường là mất hình và bị loại. Khung tối nằm từ 12 đến dưới 40, có thể là cảnh thật như đường hầm hoặc bóng râm nên được gắn cờ và giữ nếu không mắc lỗi khác. Kết quả: A có 6 khung đen và 4 khung tối; B có 4 khung đen và 61 khung tối. Tôi đồng ý giữ khung tối vì bỏ chúng sẽ làm dataset thiếu điều kiện ánh sáng khó. Tôi chọn ngưỡng dHash **2 bit** đang dùng: nó bắt 149 khung A và 52 khung B; ngưỡng 0 bỏ lọt nhiều khung gần giống, còn 6 có thể loại cả chuyển động thật. Xe chạy chậm làm các khung liên tiếp giống nhau hơn; khi xe nhanh, cùng khoảng 0,5 giây có thể đổi cảnh đáng kể nên ngưỡng cao càng rủi ro.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong **1.020 khung** hợp lệ thuộc **26 trip**, chia ngẫu nhiên theo frame làm cả **26/26 trip** nằm ở nhiều split; cả 26 trip có khung ở cả train lẫn test. Chia theo trip của Gold làm **0/26 trip** bị chia cắt. Các khung sát nhau chia sẻ cùng cảnh, thời tiết, đường và người tham gia giao thông; mô hình đã học gần như cùng mẫu với tập test, nên điểm test tăng nhưng không chứng minh khả năng tổng quát sang chuyến đi khác.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` là 12 ký tự đầu của SHA-256 tính từ danh sách `frame_id|split` đã sắp xếp. Cùng nội dung cho cùng mã, nên có thể kiểm tra lại và tra cứu chính xác dataset đã dùng; ngày giờ hoặc số lần chạy sẽ đổi dù ảnh và split không đổi. Lần xuất Gold đầu và lần `export-dataset` kế tiếp đều cho **`fffdeb8d8f2a`**, 1.020 frame, train/val/test **738/129/153**. `LAB_STUDENT_ID=2A202602429` được đưa vào `md5(trip_id || MSSV)` khi xếp trip; MSSV khác làm trip vào split khác, thay chuỗi băm dù video đầu vào giống nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest cuối của `fffdeb8d8f2a` ghi nguồn `bronze.raw_telemetry_a` v0, `bronze.raw_telemetry_b` v0, `bronze.trips_raw` v0, `bronze.video_files` v1; các bảng trung gian `silver.frames` v25, `silver.telemetry` v0, `silver.frames_flagged` v0 và `silver.frame_telemetry` v0. Video A có SHA-256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, video B là `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Để tái tạo sau 3 tháng cần đúng các file đầu vào (kiểm SHA-256), phiên bản bảng Delta, mã và cấu hình lọc/split, cùng MSSV `2A202602429`. Manifest còn ghi quy tắc loại frame xấu, split theo trip và SHA-256 của `manifest.parquet` là `a396a2124824ec31d9fbd3f6f5bacb666f5ebb8001c4f3ff93b4a4995aa7d30c` để đối chiếu đầu ra.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với `LAB_FAIL_ONCE=trip-02086`, đúng **1 task đỏ** là `silver_frames[3]` (map index 3); log ghi `RuntimeError: simulated crash while extracting trip-02086`. **7 task downstream** chuyển `upstream_failed`, còn 29 task đã thành công. Tôi chọn **chỉ task đỏ** trong giao diện và dùng Clear and Retry; hộp thoại cho thấy **8 task bị ảnh hưởng** gồm task đó và 7 task phụ thuộc. `silver_frames[3]` thành công ở lần thử thứ **2**, DAG trở lại **37/37 task xanh**. Kiểm tra sau retry vẫn **27/27**, Silver có 1.246 `frame_id` duy nhất, Gold có 1.020 frame; `dataset_version` giữ **`fffdeb8d8f2a`**. Bằng chứng: `fail_task_log.png`, `airflow_retry_graph.png` và `report/verify.txt`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba nút thắt đầu tiên là: (1) FFmpeg giải mã video trong một container Airflow, với `max_active_tis_per_dagrun=1` cho các trip, khiến thời gian xử lý tăng gần tuyến tính; tôi sẽ tách thành job trên nhiều worker và giới hạn tải theo hàng đợi. (2) DuckDB/dbt chạy trên một file và một máy, không phù hợp với nhiều nguồn ghi và biến đổi đồng thời; tôi sẽ chuyển phép biến đổi lớn sang một engine phân tán, giữ các kiểm tra chất lượng trước khi publish. (3) Việc xuất nhiều ảnh JPEG và ghi manifest/Delta từng đối tượng lên MinIO tạo áp lực I/O, số lượng object và metadata; tôi sẽ lưu theo partition, gom file hợp lý, đặt chính sách vòng đời và phân tách luồng phục vụ dashboard khỏi luồng tạo dataset. Cần đo thời gian, thông lượng và tài nguyên ở từng bước trước khi chọn số worker.

## Câu hỏi riêng của bạn trên Metabase

Dashboard Metabase chạy trên cổng `3001` ở máy thực hiện vì cổng `3000` đã có ứng dụng khác sử dụng. Hai câu SQL dưới đây đã được lưu thành thẻ trên dashboard; ảnh chụp là `report/screenshots/metabase_dashboard.png`.

**Trip nào mất nhiều frame nhất theo tỷ lệ?** Tôi thêm câu SQL trên mart đã nạp vào Postgres:

```sql
SELECT drive_id, trip_id, frames, frames_kept,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 1) AS drop_pct
FROM marts.mart_trip_summary
ORDER BY drop_pct DESC, trip_id
LIMIT 8;
```

Đứng đầu là `drive_a / trip-04622`: **34/60 frame bị loại (56,7%)**. Truy vấn chi tiết Silver cho thấy cả 34 frame này có cờ `is_near_dup`, không có frame đen, mờ hay mất đồng bộ; vì vậy nên xem đoạn camera đứng hình hoặc xe đi chậm trước khi chỉnh ngưỡng dHash.

**Câu hỏi vận hành thứ hai (bonus): nhóm tốc độ nào bị loại frame nhiều hơn?**

```sql
SELECT CASE WHEN avg_speed_kmh < 30 THEN '<30 km/h'
            WHEN avg_speed_kmh < 60 THEN '30-59 km/h'
            ELSE '>=60 km/h' END AS speed_band,
       COUNT(*) AS trips, SUM(frames) AS frames, SUM(frames_kept) AS kept,
       ROUND(100.0 * (SUM(frames) - SUM(frames_kept)) / NULLIF(SUM(frames), 0), 1) AS drop_pct
FROM marts.mart_trip_summary
GROUP BY 1 ORDER BY 1;
```

Nhóm dưới 30 km/h có **29,3%** frame bị loại (256 frame, giữ 181), so với **17,9%** ở 30–59 km/h và **13,8%** ở từ 60 km/h. Đây là tương quan trên 26 trip, chưa chứng minh tốc độ là nguyên nhân; ánh sáng và cảnh cũng có thể ảnh hưởng.

## Nhìn lại

Quan sát đáng chú ý nhất là drive B có 61 khung tối nhưng vẫn giữ được trong dataset; loại mọi ảnh thiếu sáng sẽ làm dữ liệu kém đại diện. Tôi muốn hiểu sâu hơn cách chọn ngưỡng dHash theo tốc độ và loại đường, vì một ngưỡng 2 bit cố định có thể không tối ưu cho mọi cảnh. Pipeline hiện kiểm tra miền tốc độ và khoảng đồng bộ, nhưng chưa thấy kiểm tra bước nhảy GPS vô lý giữa hai mẫu liên tiếp dù từng điểm GPS riêng lẻ vẫn hợp lệ; đây là phép thử tôi muốn bổ sung khi mở rộng hệ thống.
