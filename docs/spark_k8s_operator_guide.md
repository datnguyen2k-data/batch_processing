# Kiến trúc Deploy Spark Operator trên Kubernetes

Cách tiếp cận **"1 Pipeline = 1 Mẻ Job = 1 SparkApplication Pod (Chạy xong tự xóa)"** là **tiêu chuẩn vàng (Industry Standard)** trong môi trường Cloud-Native Data Engineering (như Uber, Apple, hay các hệ thống Data Platform hiện đại đều dùng). Quá trình này được gọi là **Ephemeral Spark Clusters**.

## 1. Đánh giá tính khả thi và độ ổn định (Có ổn không?)

**Hoàn toàn ổn và RẤT TỐT**, vì:
- **Ngăn chặn OOM (Out Of Memory) chéo**: Nếu 1 Data Pipeline tính toán lượng data cực lớn bị nổ RAM, pod đó sẽ sập và báo lỗi, nhưng TẤT CẢ các Pipeline khác đang chạy ở các Pod khác hoàn toàn không bị vạ lây (Khác với dùng chung 1 cụm Spark Standalone Master lâu đời).
- **Tối ưu Chi phí / Tài nguyên**: Khi Job khởi chạy, Spark Operator mới nài nỉ K8s cấp RAM và CPU. Khi Job kết thúc (hoặc văng lỗi), Spark Operator tự động "giết" các Executor Pods và Driver Pod, giải phóng 100% tài nguyên để K8s xếp lịch cho ứng dụng khác. Tránh tình trạng lãng phí tài nguyên idle.
- **Microservices Isolation**: Job Spark giờ đây hoạt động y hệt một AWS Lambda function.

## 2. Cách thực thi trên TypeScript Control Plane

Để làm được việc này, thay vì gọi Bash shell `spark-submit`, Service TypeScript của anh/chị sẽ giao tiếp tĩnh lên **Kubernetes API Server**.

### Bước 1: Cài đặt thư viện trên Node.js
Anh/chị sử dụng thư viện k8s chính thức của node: `@kubernetes/client-node`.

### Bước 2: Tạo Template SparkApplication (Kubernetes CRD)
Mỗi lần có 1 JSON Config cần chạy, logic TypeScript sẽ sinh ra 1 file manifest Custom Resource Definition (CRD) của Google Spark Operator như sau:

```typescript
const k8s = require('@kubernetes/client-node');

const sparkJobManifest = {
  apiVersion: "sparkoperator.k8s.io/v1beta2",
  kind: "SparkApplication",
  metadata: {
    // Generate tên ngẫu nhiên duy nhất cho mỗi run
    name: `pipeline-transform-orders-${Date.now()}`,
    namespace: "spark-jobs"
  },
  spec: {
    type: "Python",
    pythonVersion: "3",
    mode: "cluster",
    image: "company-registry/pyspark-data-plane:latest", // Docker image chứa code .py của ta
    imagePullPolicy: "Always",
    mainApplicationFile: "local:///opt/spark/work-dir/examples/run_json_job.py", // Gọi script vào điểm vào
    arguments: [
      // Truyền thẳng cục JSON Config được Serialize thành String vào làm Argument!
      JSON.stringify(pipelineConfigJson)
    ],
    sparkVersion: "3.5.3",
    restartPolicy: {
      type: "Never" // Lỗi là báo FAILED ngay để Webhook bắt được
    },
    driver: {
      cores: 1,
      coreLimit: "1200m",
      memory: "2048m",
      labels: { version: "3.5.3" },
      serviceAccount: "spark-operator-sa"
    },
    executor: {
      cores: 1,
      instances: 2, // Động tùy vào độ lớn của Pipeline (Load từ DB Settings ra)
      memory: "2048m"
    },
    // Quan trọng: Tự động dọn rác (Remove pod đi) sau khi chạy xong!!
    timeToLiveSeconds: 120 // Pod chạy xong -> chờ 2 phút cho debug -> Tự bốc hơi khỏi K8s
  }
};
```

### Bước 3: Control Plane Bắn Job lên K8s (Trigger Pod)

Bên trong code TypeScript Orchestrator:
```typescript
const kcA = new k8s.KubeConfig();
kcA.loadFromDefault();
const customObjectsApi = kcA.makeApiClient(k8s.CustomObjectsApi);

async function submitSparkJob(manifest) {
  try {
    const res = await customObjectsApi.createNamespacedCustomObject(
      "sparkoperator.k8s.io", // group
      "v1beta2",              // version
      "spark-jobs",           // namespace
      "sparkapplications",    // plural
      manifest
    );
    console.log("Đã khởi tạo thành công K8s Pod Spark Driver!");
  } catch (err) {
    console.error("Lỗi khi submit job lên K8s:", err.body);
  }
}
```

## 3. Quy trình Vòng đời Tổng Thể (The Lifecycle)

1. TypeScript (Cronjob) -> Đọc DB -> Tạo Payload `pipelineConfigJson`.
2. Truyền chuỗi Json đó vào field `arguments` của CRD `SparkApplication`.
3. Submit CRD lên Kubernetes API.
4. **Spark Operator K8s** phát hiện CRD mới -> Tự phân bổ cấp phát 1 Pod Driver và N Pods Executor.
5. Script PySpark khởi động, lấy biến JSON từ `sys.argv[1]`, dịch AST, kết nối Clickhouse và làm việc.
6. Script PySpark dùng hàm `requests.post()` để Push lại 2 chữ `SUCCESS` qua Webhook về HTTP API của Control Plane.
7. Job kết thúc Exit(0). Nhờ cấu hình `timeToLiveSeconds: 120`, Kubernetes Operator tự dọn dẹp quét sạch Container Pod khỏi Cluster Node.

Quy trình này khép kín và là thiết kế chuẩn mực nhất của các nền tảng Data Engineering hiện nay! Khẳng định phương hướng đi của anh/chị là cực kỳ hiện đại.
