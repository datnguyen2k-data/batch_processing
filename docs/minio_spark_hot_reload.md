# Môi Trường Hybrid Development: Hot-Reload Spark Code qua MinIO (S3)

Theo triết lý Data Engineering của các Storage Lakehouse như Databricks, Developer không cần Build Docker Image mỗi khi đổi một dòng code Python. Toàn bộ mã nguồn thực thi được gói lại (Zip) và đẩy lên Object Storage (MinIO) làm tham số để Image gốc tự tải về lúc khởi chạy.

Tài liệu này chuẩn hóa quy trình Developer Experience (DX) của anh/chị cho mô hình Test chạy K8s Spark Operator.

## Quy Trình (The Pipeline WorkFlow)

Mỗi khi Anh/Chị thay đổi code bên trong Editor (máy tính Local):
1. **Zipping**: Gom toàn bộ folder `/src` thành file `src.zip`.
2. **Uploading**: Bắn `src.zip` và file gốc `examples/run_json_job.py` lên MinIO bucket (Vd: `s3://spark-dev-bucket`). Tốn khoảng ~0.5s.
3. **Execution**: Control Plane gọi lên K8s Test. Spark Pod được sinh ra, tự động dùng giao thức `s3a://` để fetch file chạy mà không cần image docker mới!

---

## Bước 1: Script Tự Động Phân Phối Code (Makefile)

Anh/chị tạo một file `Makefile` ở thư mục gốc của dự án `batch_processing` với nội dung tự động hoá sau:

```makefile
# Thông số MinIO
MINIO_ENDPOINT=http://localhost:9000
MINIO_BUCKET=s3://spark-dev
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin

deploy-code:
	@echo "1. Đóng gói mã nguồn Python..."
	# Xoá file cũ nếu có, zip thư mục src (không lấy __pycache__)
	rm -f src.zip
	cd src && zip -r ../src.zip . -x "*__pycache__*"
	
	@echo "2. Uploading lên MinIO..."
	# Upload code lõi
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	aws --endpoint-url $(MINIO_ENDPOINT) s3 cp src.zip $(MINIO_BUCKET)/code/src.zip
	
	# Upload Script Entrypoint nhận Arguments
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	aws --endpoint-url $(MINIO_ENDPOINT) s3 cp examples/run_json_job.py $(MINIO_BUCKET)/code/run_json_job.py
	
	@echo "Done! Sẵn sàng submit Spark Job."
```

_Lúc lập trình, anh/chị chỉ cần gõ duy nhất 1 lệnh Terminal ở máy nhà: `make deploy-code`!_

---

## Bước 2: Cấu trúc TypeScript SparkApplication (K8s CRD)

Service TypeScript Control Plane khi submit Job lên K8s sẽ phải "chỉ điểm" cho Spark lấy file ở URL S3 (để Spark tải từ MinIO). Cấu trúc `manifest` bằng JSON / YAML sẽ được điều chỉnh thiết yếu ở các tham số sau:

```typescript
const sparkJobManifest = {
  apiVersion: "sparkoperator.k8s.io/v1beta2",
  kind: "SparkApplication",
  metadata: { name: `pipeline-ast-demo-${Date.now()}` },
  spec: {
    type: "Python",
    // 1. Image là Base mặc định (KHÔNG CÓ CODE PYTHON CỦA BẠN TRONG NÀY, base image chứa Hadoop/AWS Jars là đủ)
    image: "company-registry/spark-py-s3-base:3.5.3", 
    imagePullPolicy: "IfNotPresent",
    
    // 2. Chấm thẳng file chạy chính từ MinIO Bucket!
    mainApplicationFile: "s3a://spark-dev/code/run_json_job.py", 
    
    deps: {
      // 3. Tải kèm bộ "Source Thư Viện Lõi" (src.zip) để Pods import các hàm Pydantic, AstVisitor...
      pyFiles: [
        "s3a://spark-dev/code/src.zip"
      ]
    },
    
    // 4. Môi trường (Env) MinIO cấp cho Driver & Executor để tụi nó có quyền kéo File
    hadoopConf: {
      "fs.s3a.endpoint": "http://minio.test-region.svc.cluster.local:9000",
      "fs.s3a.access.key": "minioadmin",
      "fs.s3a.secret.key": "minioadmin",
      "fs.s3a.path.style.access": "true",
      "fs.s3a.connection.ssl.enabled": "false"
    },
    
    arguments: [
       // Config String từ TS gởi qua...
       payloadString
    ],
    // Thêm các setup râu ria (executor ram, logs, TTL...) ...
  }
}
```

---

## 💡 Tư Duy Xử Lý Ngầm Bên Trong Thư Mục Pod (How it works under the hood)

Nhiều Developer sợ rằng khi ném `pyFiles=src.zip`, thì trong Code `import src.domain....` sẽ bị vỡ đường dẫn. Câu trả lời là CÓ, nếu cấu trúc không chuẩn. Tuy nhiên Spark xử lý vô cùng "Ma Thuật":

1. Khi pod Spark khởi động, nó thấy có biến `pyFiles: s3a://.../src.zip`. Nó tự dùng HTTP request tải từ MinIO lưu vứt thẳng vào thư mục gốc `work-dir` làm việc.
2. Spark tự động nhét đường dẫn biến môi trường `PYTHONPATH=$PYTHONPATH:/opt/spark/work-dir/src.zip` vào cho bộ thông dịch Python.
3. Khi Script chính (`run_json_job.py`) chạy tới lệnh: `from src.domain.pipeline.models import PipelineConfig`, Python Interpreter tự động tìm thấy folder `src` **ngay bên trong cục src.zip kia** và truy xuất ngon ơ như một thư viện cài ngoài (PIP)!

✨ KẾT QUẢ: Code Local dưới máy, gõ 1 phím đổi hàm. Gõ dòng Terminal `make deploy-code`. Giao diện Test chạy nhận ngay file mới, logic mới. Khối lượng thời gian Deploy Build Docker bằng **Không (Zero Build Time)**.
