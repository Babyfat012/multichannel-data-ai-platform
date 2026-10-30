"""AWS data/ML deployment; sources are Compose AWS overlays and infra manifests."""

from common import Diagram


def make():
    """Build batch, replay, training and drift flows on the actual AWS runtimes."""
    d = Diagram("aws-architecture-layer12", "AWS · Data Platform / ML Platform", 2840, 2000)
    d.deployment = "AWS · ap-southeast-2"
    d.frame(320, 90, 2500, 1590, "AWS · ap-southeast-2")
    d.frame(320, 130, 2480, 1280, "VPC · 10.42.0.0/16 · kết nối IP riêng")
    d.frame(340, 180, 1320, 1200, "EC2 · Docker Compose · r6i.xlarge · 10.42.0.151 · IAM role")
    d.frame(1700, 180, 1080, 1200, "EKS · 2 node · tag = namespace", True)
    d.frame(40, 180, 260, 300, "OUTSIDE · developer")
    d.frame(340, 1440, 1320, 190, "S3 · IAM · bucket không public")
    d.frame(1700, 1440, 1080, 190, "ECR · platform/* · image tag bất biến")
    specs = [
        (
            "dev",
            60,
            270,
            "ngoài AWS",
            "Developer",
            "SSM · không mở SSH",
            "chuẩn bị nguồn / build image",
        ),
        (
            "pg",
            370,
            260,
            "mặc định",
            "PostgreSQL",
            "postgres:15 · :5432",
            "Walmart / warehouse / metadata",
        ),
        (
            "lk",
            710,
            260,
            "catalog / spark / query",
            "Lakekeeper",
            "REST Iceberg · :8181",
            "catalog metadata → PostgreSQL",
        ),
        ("trino", 1050, 260, "query", "Trino", ":8080 · host :8083", "đọc Gold A / warehouse"),
        (
            "spark",
            710,
            530,
            "spark",
            "spark-master",
            "Spark 3.5.3 · :7077",
            "Bronze / Silver / Gold",
        ),
        ("w1", 370, 530, "spark", "spark-worker-1", "Spark 3.5.3", "executor batch"),
        ("w2", 1050, 530, "spark", "spark-worker-2", "Spark 3.5.3", "executor batch"),
        ("air", 1390, 530, "airflow", "Airflow", ":8080 · host :8081", "ml_feature_materialize"),
        (
            "kafka",
            370,
            800,
            "stream / cdc",
            "Kafka",
            ":9092 · host :9094",
            "Return / ml.sku_return_24h",
        ),
        (
            "flink",
            710,
            800,
            "flink",
            "flink-jobmanager",
            "Flink 1.20.3 · :8081",
            "SQL Return 24h / SKU",
        ),
        ("tm", 1050, 800, "flink", "flink-taskmanager", "Flink 1.20.3", "thực thi cửa sổ Return"),
        (
            "push",
            1390,
            800,
            "ml-stream",
            "feast-pusher",
            "Feast 0.66.0",
            "push online + Parquet offline",
        ),
        ("mlflow", 1050, 1070, "ml", "MLflow", ":5000 · host :5050", "production · artifacts S3"),
        ("jenkins", 1390, 1070, "ci", "Jenkins", ":8090 · build image", "CI image ứng dụng"),
        ("vault", 370, 1280, "security", "Vault", ":8200 · EBS", "init / unseal tay"),
        (
            "dh",
            710,
            1280,
            "profile riêng · tùy pha",
            "DataHub GMS",
            ":8080 · host :8089",
            "metadata / lineage",
        ),
        (
            "cdc",
            370,
            1070,
            "cdc · tùy chọn",
            "Debezium Connect",
            ":8083 · host :8084",
            "Postgres WAL → Kafka",
        ),
        (
            "redis",
            1750,
            260,
            "ns feature-store",
            "Redis",
            ":6379 · NodePort :30379",
            "online features",
        ),
        (
            "api",
            2090,
            260,
            "ns feature-store",
            "Feature API · 2 replica",
            ":8000 · /mcp /predict",
            "Pod Identity · S3 chỉ đọc",
        ),
        (
            "serve",
            2430,
            530,
            "ns ml-serving",
            "KServe return-risk",
            ":8080 · RawDeployment",
            "Feast SDK / model trong image",
        ),
        (
            "kfp",
            1750,
            800,
            "ns kubeflow",
            "Kubeflow Pipelines",
            "KFP 2.17.2 · 5 bước",
            "PIT → split → train → eval",
        ),
        (
            "drift",
            2090,
            1070,
            "ns kubeflow",
            "Job mle-drift",
            "python:3.12 · Evidently",
            "log JSON · không tự retrain",
        ),
        (
            "ks",
            2430,
            1280,
            "ns kserve",
            "KServe controller",
            "InferenceService",
            "điều khiển predictor",
        ),
        (
            "raw",
            370,
            1510,
            "S3 · raw bucket",
            "S3 raw",
            "source-exports / JSONL",
            "nguồn đã chuẩn bị",
        ),
        (
            "lake",
            1050,
            1510,
            "S3 · lakehouse bucket",
            "S3 lakehouse",
            "Iceberg / Parquet / artifact",
            "feature-store/registry.db",
        ),
        (
            "ecr",
            2090,
            1510,
            "AWS ECR",
            "ECR repositories",
            "platform/platform-*",
            "train / return-risk / API",
        ),
    ]
    for spec in specs:
        d.box(*spec, support=spec[0] in {"vault", "dh", "ks", "jenkins"})
    edges = [
        ("raw", "spark", "B1", "raw JSONL"),
        ("pg", "spark", "B2", "nguồn Walmart JDBC"),
        ("spark", "lake", "B3", "Bronze / Silver / Gold"),
        ("spark", "lk", "B4", "commit snapshot"),
        ("lk", "spark", "B5", "metadata Iceberg"),
        ("spark", "pg", "B6", "Gold warehouse"),
        ("lake", "trino", "B7", "bảng Gold Iceberg"),
        ("pg", "trino", "B8", "bảng warehouse"),
        ("trino", "dev", "B9", "kết quả SQL qua SSM"),
        ("spark", "lake", "B10", "Parquet feature / PIT"),
        ("lake", "air", "B11", "feature mới nhất"),
        ("air", "redis", "B12", "materialize :30379"),
        ("trino", "kafka", "S1", "phát lại Return Gold A"),
        ("kafka", "flink", "S2", "Return đã duyệt"),
        ("flink", "kafka", "S3", "Return 24h / SKU"),
        ("kafka", "push", "S4", "ml.sku_return_24h"),
        ("push", "redis", "S5", "online qua :30379"),
        ("push", "lake", "S6", "Parquet offline"),
        ("lake", "kfp", "T1", "training_lines / PIT"),
        ("kfp", "mlflow", "T2", "model / metric :5050"),
        ("mlflow", "dev", "T3", "model production qua SSM"),
        ("dev", "ecr", "T4", "CLI build / push image"),
        ("ecr", "serve", "T5", "image mô hình"),
        ("lake", "drift", "M1", "reference / lô mới"),
        ("redis", "api", "B13", "feature SKU"),
        ("api", "serve", "T6", "đơn cần dự đoán"),
        ("redis", "serve", "T7", "feature online"),
        ("air", "spark", "P1", "spark-submit"),
        ("spark", "w1", "P2", "task batch"),
        ("spark", "w2", "P3", "task batch"),
        ("flink", "tm", "P4", "task stream"),
        ("pg", "cdc", "P5", "WAL CDC tùy chọn"),
        ("cdc", "kafka", "P6", "change events"),
    ]
    for edge in edges:
        d.edge(*edge, secondary=edge[2].startswith("P"))
    d.note(
        40,
        1710,
        "Feast là thư viện; S3 chứa registry.db, rag-registry.db, Parquet offline và Iceberg qua Lakekeeper. Không có MinIO trên AWS.",
    )
    d.note(
        40,
        1732,
        "S1: replay_return_events.py đọc Gold qua Trino → Kafka (gap #25); model v1 train khi feature stream còn rỗng. Profile chạy theo pha.",
    )
    d.flows = [
        (
            "B",
            "Batch · Developer",
            "Raw S3 / Walmart → Spark → Iceberg S3 + warehouse; Parquet → Airflow → Redis EKS qua IP riêng.",
        ),
        (
            "S",
            "Stream · Developer",
            "Phát lại Return Gold A qua Trino → Kafka → Flink → Kafka → feast-pusher → Redis + S3.",
        ),
        (
            "T",
            "Train / deploy · ML engineer",
            "KFP → MLflow EC2 → Developer chạy deploy_return_risk.sh build/push → ECR → KServe; API gọi predictor trong cụm.",
        ),
        (
            "M",
            "Drift · ML engineer",
            "Job mle-drift (kubeflow): Evidently đọc S3, ghi tóm tắt log; không có retrain tự động.",
        ),
    ]
    d.note(
        30,
        1940,
        "Quy ước: vòng tròn = bước theo luồng; mũi tên = dữ liệu hoặc chiều gọi; nét đứt = điều phối / CDC phụ.",
    )
    d.note(
        30,
        1962,
        "Hộp nét đứt = hỗ trợ; S3/ECR ngoài VPC. EC2 IAM role; EKS Pod Identity chỉ đọc feature-store/*; image pull dùng role node.",
    )
    return d
