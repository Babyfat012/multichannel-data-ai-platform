"""Feature view RAG: mỗi chunk tài liệu chính sách trả hàng là một entity, kèm embedding 384 chiều.

Pipeline `dags/ml_rag_ingest.py` ghi Parquet offline rồi đẩy vào online store bằng
`write_to_online_store`; Agent tìm bằng `retrieve_online_documents_v2` (cosine).
"""

import os
from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.types import Array, Float32, String

chunk = Entity(
    name="chunk",
    join_keys=["chunk_id"],
    value_type=ValueType.STRING,
    description="Một đoạn tài liệu",
)

policy_chunks = FeatureView(
    name="policy_chunks",
    entities=[chunk],
    ttl=timedelta(0),  # tài liệu không hết hạn; bản mới ghi đè theo chunk_id
    schema=[
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,
            vector_length=384,
            vector_search_metric="cosine",
        ),
        Field(name="text", dtype=String),
        Field(name="doc_id", dtype=String),
        Field(name="heading", dtype=String),
    ],
    source=FileSource(
        name="policy_chunks_source",
        path=os.environ.get("FEAST_RAG_CHUNKS_PATH", "s3://platform/rag/chunks.parquet"),
        timestamp_field="event_timestamp",
        s3_endpoint_override=os.environ.get("FEAST_S3_ENDPOINT_URL"),
    ),
)
