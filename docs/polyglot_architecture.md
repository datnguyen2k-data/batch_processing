# Kiến trúc Hệ Thống Dữ Liệu Phân Tán (TypeScript + PySpark)

Tài liệu này đóng vai trò là **Bản Đặc Tả Kỹ Thuật (Data Contract)** chính thức kết nối 2 Service độc lập: **Control Plane (viết bằng TypeScript)** và **Data Plane (viết bằng PySpark/Flink)**.

---

## Phần 1: Tổ Chức Thư Mục (Code Structure)

Môi trường Development được tách ra làm 2 Sub-Projects độc lập hoàn toàn để tránh đụng độ thư viện.

```text
batch_processing/
├── control_plane/                   # (Service Mới: Viết bằng TypeScript)
│   ├── package.json
│   ├── tsconfig.json
│   ├── src/
│   │   ├── api/                     # REST API (Quản lý User, Pipelines, Jobs)
│   │   ├── parser/                  # Trình biên dịch AST (nhận String Expression -> xuất JSON AST)
│   │   ├── orchestrator/            # Sinh tiến trình / Gọi lệnh spark-submit, quản lý REST Callback
│   │   └── database/                # Mã nguồn ORM (TypeORM / Prisma) kết nối Postgres Control DB
│   └── docs/                        # Tài liệu API (Swagger/Postman)
│
├── spark_data_plane/                # (Service Hiện Tại: Dùng PySpark)
│   ├── requirements.txt             
│   ├── src/
│   │   ├── application/
│   │   │   └── pipeline/
│   │   │       ├── ast_visitor.py   # Pattern phân tích JSON AST sang object `F.col/F.when`
│   │   │       ├── webhook_logger.py# Bắn HTTP requests cập nhật status về TypeScript
│   │   │       └── job.py           # Core Runner nhận Pydantic config -> gọi Transformer & Data Port
│   │   └── domain/                 
│   └── examples/
│       └── test_ast_pipeline.py     # Script mốc giả lập gửi Array JSON
```

---

## Phần 2: Đặc tả Giao Tiếp Liên Dịch Vụ (IPC / Service Communication)

Việc giao tiếp chia làm 2 giai đoạn: **Chiều đi (Submit Job)** và **Chiều Về (Audit Callback)**.

### Chiều Đi (Control Plane -> PySpark Engine)

TypeScript Control Plane sẽ gửi cấu hình tới PySpark thông qua lưu file ra JSON và submit via CLI. Spark Worker chỉ nhận JSON mà không query thêm DB config nào.

```bash
# TypeScript Orchestrator gọi qua Node.js `child_process.exec`
spark-submit \
  --master spark://spark-master:7077 \
  --name "Pipeline_Auto_01" \
  examples/run_json_job.py /path/to/generated/temp_job.json
```

**Định dạng Data Contract (TypeScript sinh ra - Python phân tích):**
```json
{
  "run_id": "job_1234abc",
  "pipeline_name": "Transform_Status",
  "control_plane_url": "http://control-plane:3000",
  "source": { 
    "type": "clickhouse", 
    "database": "ldz", 
    "table": "orders" 
  },
  "target": { 
    "type": "clickhouse", 
    "database": "hmz", 
    "table": "orders", 
    "write_mode": "upsert", 
    "primary_keys": ["order_id"] 
  },
  "transform": {
    "column_mapping": [
      {
        "target": "discounted_price",
        "type": "double",
        "ast": {
          "type": "FunctionCall",
          "name": "IIF",
          "args": [
             {
               "type": "BinaryOp",
               "op": ">",
               "left": { "type": "ColumnRef", "name": "price" },
               "right": { "type": "Number", "value": 100 }
             },
             {
               "type": "BinaryOp",
               "op": "*",
               "left": { "type": "ColumnRef", "name": "price" },
               "right": { "type": "Number", "value": 0.8 }
             },
             { "type": "ColumnRef", "name": "price" }
          ]
        }
      }
    ]
  }
}
```

### Chiều Về (PySpark Engine -> Control Plane Webhook)

PySpark Worker thông qua class `CentralizedWebhookLogger` sẽ phát các HTTP POST về API của Control Plane để duy trì log và trạng thái Job.

- **Bắt Đầu Chạy**: 
  - `POST http://control-plane:3000/api/audit/job_1234abc/status`
  - Body: `{ "status": "RUNNING" }`
- **Chạy Thành Công**:
  - `POST http://control-plane:3000/api/audit/job_1234abc/status`
  - Body: `{ "status": "SUCCESS" }`
- **Bị Lỗi (Exception)**:
  - `POST http://control-plane:3000/api/audit/job_1234abc/status`
  - Body: `{ "status": "FAILED", "error_reason": "SparkException: Memory limit exception..." }`

🚨 *Lưu ý*: Nếu Application PySpark tự crash (văng Exit Code), Module Orchestrator bên TypeScript phải tự đọc tiến trình Exit != 0 để ép status DB về `FAILED`.

---

## Phần 3: Trách nhiệm xử lý Bộ biên dịch (AST Parser) trên TypeScript

Để tương thích với Cấu trúc `transform.column_mapping[].ast` bên trên, Service TypeScript được khuyến nghị sử dụng Generator Parsing Engine (ví dụ: **`PEG.js`** hoặc **`nearley`**).

1. Hệ thống UI hoặc API của Control Plane cho phép User nhập các câu lệnh Expression tính toán dạng Text giống hệt Tableau: 
  `IIF([price] > 100, [price]*0.8, [price])`
2. Trình phân tích ngữ pháp `PEG.js` phân rã câu Text trên, tự động nhả ra cho anh/chị Object JSON AST sâu bên trong thuộc phần `args`.
3. Orchestrator lắp cái JSON Node đó vào field `ast` của property `column_mapping` và bắt đầu gọi quá trình Pipeline sinh Job chạy trên Data Plane.
