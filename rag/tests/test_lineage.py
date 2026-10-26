"""Test MCP DataHub của pipeline RAG: đúng chuỗi lineage và assertion theo receipt."""

import json

from rag.pipeline import lineage_mcps

RECEIPT = {
    "run_id": "r1",
    "completed_at": "2026-10-21T00:00:00+00:00",
    "checks": {"duplicate_chunk_ids": 0, "probe_wrong_top_doc": 1},
}


def by_aspect(mcps, aspect):
    return [
        (m["entityUrn"], json.loads(m["aspect"]["value"]))
        for m in mcps
        if m["aspectName"] == aspect
    ]


def test_lineage_chain_raw_to_online():
    mcps = lineage_mcps("platform/rag", RECEIPT)
    edges = {
        urn: [u["dataset"] for u in v["upstreams"]] for urn, v in by_aspect(mcps, "upstreamLineage")
    }
    online = (
        "urn:li:dataset:(urn:li:dataPlatform:postgres,rag.public.platform_rag_policy_chunks,PROD)"
    )
    vectors = "urn:li:dataset:(urn:li:dataPlatform:s3,platform/rag/chunks.parquet,PROD)"
    assert edges[online] == [vectors]
    assert len(edges) == 4  # raw, chunks_text, chunks, online


def test_jobs_follow_task_order():
    jobs = by_aspect(lineage_mcps("platform/rag", RECEIPT), "dataJobInputOutput")
    names = [urn.rsplit(",", 1)[1].rstrip(")") for urn, _ in jobs]
    assert names == ["upload", "chunk", "embed", "load", "validate"]
    assert jobs[0][1]["inputDatajobs"] == []
    assert jobs[1][1]["inputDatajobs"] == [jobs[0][0]]


def test_assertion_result_follows_check_value():
    runs = by_aspect(lineage_mcps("platform/rag", RECEIPT), "assertionRunEvent")
    results = sorted(v["result"]["type"] for _, v in runs)
    assert results == ["FAILURE", "SUCCESS"]
