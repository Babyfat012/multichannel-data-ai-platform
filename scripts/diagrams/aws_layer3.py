"""AWS conversation, RAG, observability and CI deployment from repo manifests."""

from common import Diagram


def make():
    """Define actual AWS units and data directions without local-only services."""
    d = Diagram("aws-architecture-layer3", "AWS · LLM Platform / Agents", 3100, 2290)
    d.deployment = "AWS · ap-southeast-2"
    d.frame(320, 90, 2750, 1880, "AWS · ap-southeast-2")
    d.frame(340, 130, 2710, 1620, "VPC · IP riêng EC2 ↔ EKS")
    d.frame(360, 360, 1980, 1360, "EKS · 2 node · tag = namespace", True)
    d.frame(2380, 360, 650, 1360, "EC2 · Docker Compose · 10.42.0.151 · r6i.xlarge")
    d.frame(30, 180, 260, 350, "OUTSIDE · seller / dev")
    d.frame(2380, 1780, 650, 150, "S3 / ECR · ngoài VPC · IAM")
    specs = [
        (
            "seller",
            50,
            270,
            "người dùng",
            "Seller / Developer",
            "HTTPS · trình duyệt",
            "chat / vận hành",
        ),
        ("nlb", 390, 270, "AWS · LoadBalancer", "NLB public", ":80 / :443", "Service ingress EKS"),
        (
            "ing",
            730,
            270,
            "ns ingress",
            "F5 NGINX Ingress",
            "agent. · basic auth · 5r/s",
            "TLS Let's Encrypt · :443",
        ),
        ("ui", 1070, 270, "ns kagent", "kagent UI", ":8080", "chat → coordinator"),
        ("coord", 1410, 270, "ns kagent", "coordinator", "A2A · :8080", "điều phối sub-task"),
        (
            "agw",
            1750,
            270,
            "ns agentgateway",
            "agentgateway",
            ":3000 · metrics :15020",
            "API key / PII / A/B",
        ),
        (
            "llm",
            2090,
            540,
            "ns llm",
            "Qwen · llama.cpp",
            "Qwen2.5-1.5B · :8080",
            "OpenAI API · CPU",
        ),
        ("data", 1070, 540, "ns kagent", "data-agent", "A2A · :8080", "get_sku_features"),
        ("model", 1410, 540, "ns kagent", "model-agent", "A2A · :8080", "score / policy RAG"),
        (
            "api",
            1410,
            810,
            "ns feature-store",
            "Feature API · 2 replica",
            ":8000 · /mcp /chunks",
            "Pod Identity · S3 chỉ đọc",
        ),
        (
            "redis",
            1070,
            810,
            "ns feature-store",
            "Redis",
            ":6379 · NodePort :30379",
            "Feast SKU online",
        ),
        (
            "serve",
            1750,
            810,
            "ns ml-serving",
            "KServe return-risk",
            ":8080 · predictor",
            "Feast SDK / model image",
        ),
        (
            "pgv",
            1410,
            1080,
            "ns rag",
            "pgvector",
            ":5432 · NodePort :30432",
            "policy chunks / vectors",
        ),
        (
            "prom",
            390,
            1080,
            "ns observability",
            "Prometheus",
            "scrape API / gateway",
            "pull exporters EC2 IP riêng",
        ),
        ("keda", 390, 810, "ns keda", "KEDA", "ScaledObject ns kagent", "req/s → replicas"),
        (
            "graf",
            390,
            1350,
            "ns observability",
            "Grafana",
            "dashboards / queries",
            "grafana.<ip>.sslip.io",
        ),
        (
            "fluent",
            730,
            1080,
            "ns observability",
            "Fluent Bit",
            "DaemonSet · log pod",
            "thu log trên node",
        ),
        ("loki", 730, 1350, "ns observability", "Loki", ":3100 · PVC gp3", "log container"),
        ("jaeger", 1750, 1080, "ns observability", "Jaeger", "OTLP :4317", "traces.<ip>.sslip.io"),
        ("lf", 2090, 1080, "ns langfuse", "Langfuse", ":3000", "prompt production / version"),
        (
            "areg",
            1410,
            1350,
            "ns agentregistry",
            "agentregistry",
            "danh bạ agent / MCP",
            "registry.<ip>.sslip.io",
        ),
        (
            "cert",
            730,
            540,
            "ns cert-manager",
            "cert-manager",
            "Let's Encrypt HTTP-01",
            "cấp / gia hạn TLS",
        ),
        (
            "air",
            2410,
            810,
            "airflow",
            "Airflow · ml_rag_ingest",
            ":8080 · host :8081",
            "chunk / fastembed CPU",
        ),
        ("jenkins", 2410, 540, "ci", "Jenkins", ":8090 · test / build", "EKS access entry ci"),
        ("node", 2410, 1080, "mặc định", "node-exporter", ":9100", "metric CPU / disk EC2"),
        ("pgexp", 2750, 1080, "mặc định", "postgres-exporter", ":9187", "metric PG / WAL"),
        ("vault", 2750, 1350, "security", "Vault", ":8200 · EBS", "secret ingress / CI / gateway"),
        (
            "s3",
            2410,
            1650,
            "S3 · raw + lakehouse",
            "S3 buckets",
            "rag/raw · chunks Parquet",
            "feature-store/rag-registry.db",
        ),
        (
            "ecr",
            2750,
            1650,
            "AWS ECR",
            "ECR repositories",
            "platform/* · image:SHA",
            "pull qua role node EKS",
        ),
    ]
    for spec in specs:
        key, x, y, *labels = spec
        d.box(
            key,
            x,
            y if key in {"seller", "nlb"} else y + 180,
            *labels,
            support=key in {"areg", "vault"},
        )
    edges = [
        ("seller", "nlb", "A1", "câu hỏi HTTPS"),
        ("nlb", "ing", "A2", "HTTPS agent."),
        ("ing", "ui", "A3", "chat xác thực"),
        ("ui", "coord", "A4", "task A2A"),
        ("coord", "agw", "A5", "sub-task A2A"),
        ("agw", "data", "A6", "task dữ liệu"),
        ("agw", "model", "A7", "task score / RAG"),
        ("data", "agw", "A8", "MCP / prompt"),
        ("model", "agw", "A9", "MCP / prompt"),
        ("agw", "api", "A10", "tool call /mcp"),
        ("agw", "llm", "A11", "prompt / completion"),
        ("redis", "api", "A12", "feature SKU"),
        ("pgv", "api", "A13", "chunk / vector"),
        ("api", "serve", "A14", "đơn cần dự đoán"),
        ("redis", "serve", "A15", "feature online"),
        ("s3", "api", "A16", "registry S3 chỉ đọc"),
        ("s3", "air", "K1", "tài liệu raw"),
        ("air", "s3", "K2", "chunk / embedding"),
        ("air", "pgv", "K3", "Feast write :30432"),
        ("agw", "prom", "O1", "metric req / token"),
        ("api", "prom", "O2", "metric HTTP / tool"),
        ("node", "prom", "O3", "metric EC2 :9100"),
        ("pgexp", "prom", "O4", "metric PG :9187"),
        ("prom", "keda", "O5", "req/s theo route"),
        ("keda", "data", "O6", "scale replicas"),
        ("keda", "coord", "O7", "scale coordinator"),
        ("prom", "graf", "O8", "series dashboard"),
        ("fluent", "loki", "O9", "log pod"),
        ("loki", "graf", "O10", "log truy vấn"),
        ("agw", "jaeger", "O11", "trace OTLP"),
        ("jaeger", "graf", "O12", "trace truy vấn"),
        ("jenkins", "ecr", "C1", "test / build / push SHA"),
        ("ecr", "api", "C2", "image pull"),
        ("jenkins", "api", "C3", "ci · helm --atomic"),
        ("jenkins", "coord", "C4", "ci · apply / eval"),
        ("lf", "jenkins", "C5", "prompt production sync"),
        ("cert", "ing", "P1", "chứng chỉ TLS"),
    ]
    for edge in edges:
        d.edge(*edge, secondary=edge[2].startswith("P"))
    d.note(
        40,
        2000,
        "Hosts HTTPS: api., agent., grafana., traces., metrics., registry. + <ip>.sslip.io; UI basic auth, API rate limit 50r/s.",
    )
    d.note(
        40,
        2022,
        "Feast / fastembed là thư viện. RAG: EC2 IAM role → S3 → pgvector EKS :30432 qua IP riêng. A/B là route cấu hình, không thêm replica B mặc định.",
    )
    d.flows = [
        (
            "A",
            "Hội thoại · Seller",
            "NLB → NGINX → kagent UI → coordinator → gateway → agents → MCP / Qwen; API → KServe, Redis, pgvector.",
        ),
        (
            "K",
            "RAG ingest · Developer",
            "Airflow ml_rag_ingest đọc raw, ghi chunk + embedding S3 và Feast pgvector online qua NodePort 30432.",
        ),
        (
            "O",
            "Quan sát / scale · Operator",
            "Prometheus pull metric, kể cả EC2 :9100/:9187; KEDA scale agent; Fluent Bit → Loki; OTLP → Jaeger.",
        ),
        (
            "C",
            "CI/CD · Developer",
            "Jenkins EC2 test → build → ECR → EKS qua access entry ci; helm --atomic / eval / prompt sync.",
        ),
    ]
    d.note(
        30,
        2230,
        "Quy ước: mũi tên = dữ liệu; gọi không kèm dữ liệu hướng bên gọi → bên được gọi. Prometheus pull, mũi tên biểu diễn metric trả về.",
    )
    d.note(
        30,
        2252,
        "Vòng tròn = bước theo luồng; nét đứt = cấp TLS phụ; hộp nét đứt = hỗ trợ dùng chung. EC2 quản trị bằng SSM, không mở SSH.",
    )
    return d
