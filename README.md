# Batch Processing với Spark - Kiến trúc DDD

Dự án batch processing sử dụng Apache Spark với kiến trúc Domain-Driven Design (DDD) được tùy chỉnh cho data pipeline.

## Cấu trúc Dự án

```
batch_processing/
├── src/
│   ├── domain/              # Business logic (Domain Layer)
│   │   ├── loading/         # Các chiến lược loading
│   │   │   ├── delete_insert.py
│   │   │   ├── merge_upsert.py
│   │   │   └── scd_type2.py
│   │   └── join/            # Các chiến lược join
│   │       └── join_strategies.py
│   ├── infrastructure/      # Technical implementations
│   │   └── connectors/      # Spark connectors
│   │       └── spark_connector.py
│   ├── application/         # Use cases
│   │   └── batch_processor.py
│   └── shared/              # Shared utilities
│       └── config.py
├── examples/                # Examples
│   ├── loading_examples.py
│   ├── join_examples.py
│   └── run_all_examples.py
├── env.example              # Template cho .env
└── docker-compose.yml
```

## Tính năng

### 1. Loading Strategies

#### Delete + Insert
- Xóa dữ liệu cũ và chèn dữ liệu mới
- Phù hợp cho full refresh
- Hỗ trợ filter condition để xóa có chọn lọc

#### Merge/Upsert
- Cập nhật bản ghi đã tồn tại hoặc chèn bản ghi mới
- Phù hợp cho incremental updates
- Sử dụng merge keys để xác định bản ghi

#### SCD Type 2
- Theo dõi lịch sử thay đổi
- Tự động expire bản ghi cũ và tạo bản ghi mới
- Hỗ trợ effective_date và expiry_date

### 2. Join Strategies

#### Basic Joins
- Inner Join
- Left Join
- Right Join
- Full Outer Join

#### Advanced Joins
- Conditional Join (với điều kiện SQL tùy chỉnh)
- Range Join (date ranges, numeric ranges)
- Multi-table Join
- Coalesce Join (ưu tiên left, fallback right)

#### Broadcast Join
- Tối ưu cho bảng nhỏ
- Tự động broadcast bảng nhỏ

#### Bucket Join
- Tối ưu cho bảng đã được bucket
- Hỗ trợ Iceberg clustering

### 3. Connectors

#### MinIO (Iceberg)
- Kết nối đến MinIO S3-compatible storage
- Sử dụng Apache Iceberg cho table format
- Hỗ trợ ACID transactions

#### ClickHouse
- Kết nối đến ClickHouse database
- Hỗ trợ read/write operations

## Cài đặt

### Docker Image Setup

Docker image đã được build sẵn với tên `spark-exp-native:3.5.3` và bao gồm:

**Spark Connectors (JAR files):**
- ClickHouse Spark Runtime (0.9.0)
- Iceberg Spark Runtime (1.9.2)
- Hadoop AWS + AWS SDK (cho MinIO)
- Spark Kafka connector

**Python Dependencies trong Docker Image:**

File `requirements.txt` chứa các dependencies Python cần thiết:
```
pydantic>=2.0.0
pydantic-settings>=2.0.0
```

**Lưu ý:** Dự án sử dụng Pydantic v2 với `pydantic-settings` để quản lý configuration từ file `.env` thay vì `python-dotenv`.

**Thêm Python Dependencies Mới:**

Nếu cần thêm Python package mới:

1. **Thêm vào `requirements.txt`:**
   ```txt
   python-dotenv>=1.2.1
   your-new-package>=1.0.0
   ```

2. **Build lại Docker image:**
   ```bash
   docker build -f Dockerfile.example -t spark-exp-native:3.5.3 .
   ```
   (Hoặc sử dụng Dockerfile thực tế của bạn)

3. **Image đã sẵn sàng sử dụng** - không cần thay đổi `docker-compose.yml` nếu giữ nguyên tên image

### Local Setup

1. **Cài đặt dependencies:**
```bash
pip install -e .
# Hoặc
pip install -r requirements.txt
```

2. **Cấu hình môi trường:**
```bash
cp env.example .env
# Chỉnh sửa .env với thông tin kết nối của bạn
```

3. **Chạy Spark cluster:**
```bash
docker-compose up -d
```

## Sử dụng

### Loading Examples

```python
from src.infrastructure.connectors.spark_connector import SparkConnector
from src.application.batch_processor import BatchProcessor

# Tạo Spark session
spark = SparkConnector.create_with_all_catalogs("MyApp")

# Tạo processor
processor = BatchProcessor(spark)

# Delete + Insert
processor.load_data(
    source_df=df,
    target_table="customers",
    strategy="delete_insert",
    target_catalog="iceberg",
    filter_condition="date = '2024-01-01'"
)

# Merge/Upsert
processor.load_data(
    source_df=df,
    target_table="customers",
    strategy="merge_upsert",
    target_catalog="iceberg",
    merge_keys=["customer_id"]
)

# SCD Type 2
processor.load_data(
    source_df=df,
    target_table="customers",
    strategy="scd_type2",
    target_catalog="iceberg",
    business_keys=["customer_id"]
)
```

### Join Examples

```python
from src.domain.join import BasicJoinStrategy, AdvancedJoinStrategy

basic_join = BasicJoinStrategy(spark)

# Inner join
result = basic_join.inner_join(
    left_df=customers_df,
    right_df=orders_df,
    join_keys=["customer_id"]
)

# Advanced conditional join
advanced_join = AdvancedJoinStrategy(spark)
result = advanced_join.conditional_join(
    left_df=orders_df,
    right_df=date_ranges_df,
    join_condition="orders.order_date >= date_ranges.start_date AND orders.order_date <= date_ranges.end_date"
)
```

### Chạy Examples

```bash
# Chạy tất cả examples
python examples/run_all_examples.py

# Hoặc chạy từng loại
python examples/loading_examples.py
python examples/join_examples.py
```

## Cấu hình

### Environment Variables

Xem `env.example` để biết các biến môi trường cần thiết:

- **Spark**: `SPARK_APP_NAME`, `SPARK_MASTER`
- **MinIO**: `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`
- **ClickHouse**: `CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`

## Kiến trúc DDD

Dự án tuân theo nguyên tắc DDD với các layer:

1. **Domain Layer** (`src/domain/`): Business logic, strategies
2. **Infrastructure Layer** (`src/infrastructure/`): Technical implementations, connectors
3. **Application Layer** (`src/application/`): Use cases, orchestration
4. **Shared Layer** (`src/shared/`): Common utilities, configuration

## Dependencies

### Python Dependencies

Các thư viện Python cần thiết (đã có trong `requirements.txt`):

- **pydantic** (>=2.0.0): Data validation và settings management
- **pydantic-settings** (>=2.0.0): Đọc biến môi trường từ file `.env` với validation

**Lợi ích của Pydantic v2:**
- Type validation tự động
- Đọc và validate config từ `.env` file
- Hỗ trợ default values và aliases
- Better error messages khi config sai

**Lưu ý về PySpark:**
- PySpark đã được tích hợp sẵn trong Spark Docker image
- Không cần cài đặt thêm PySpark trong Docker container
- Nếu phát triển local (ngoài Docker), có thể cần: `pip install pyspark>=3.5.0`

### Spark JAR Dependencies (đã có trong Docker image)

Các JAR files đã được tải vào `/opt/spark/jars/`:

### Thêm Dependencies Mới

#### Thêm Python Dependencies

1. **Thêm vào `requirements.txt`:**
   ```txt
   python-dotenv>=1.2.1
   your-new-package>=1.0.0
   ```

2. **Build lại Docker image:**
   ```bash
   docker build -f Dockerfile.example -t spark-exp-native:3.5.3 .
   ```
   (Sử dụng Dockerfile thực tế của bạn nếu khác)

3. **Image đã sẵn sàng** - `docker-compose.yml` sẽ tự động sử dụng image mới

#### Thêm Spark JAR Dependencies

Nếu cần thêm Spark connector JAR mới:

1. **Thêm vào Dockerfile:**
   ```dockerfile
   RUN curl -fL \
     https://repo1.maven.org/maven2/group/artifact/version/artifact-version.jar \
     -o /opt/spark/jars/artifact-version.jar
   ```

2. **Build lại Docker image**

## Lưu ý

- Đảm bảo Spark cluster đã được khởi động trước khi chạy
- MinIO và ClickHouse cần được cấu hình và chạy
- Tất cả Spark JAR dependencies đã được cài đặt trong Docker image
- Python dependencies được cài đặt từ `requirements.txt` trong Dockerfile

## License

MIT

