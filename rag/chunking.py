"""Cắt tài liệu Markdown thành chunk cho RAG; thuần Python để test không cần model hay Feast.

Mỗi mục `## ` là một chunk (kèm tiêu đề tài liệu); mục dài hơn `max_chars` bị cắt theo câu với
phần gối `overlap` ký tự. `chunk_id` là băm của nội dung ⇒ chạy lại ra cùng id (ghi đè, không nhân bản).
"""

import functools
import hashlib
import re

MODEL = "BAAI/bge-small-en-v1.5"  # 384 chiều, ONNX chạy CPU qua fastembed


def _split_long(text, max_chars, overlap):
    """Gom câu thành đoạn ≤ max_chars; đoạn sau lặp lại đuôi đoạn trước để không mất ngữ cảnh."""
    sentences = re.split(r"(?<=[.!?])\s+|\n(?=- )", text)
    parts, current = [], ""
    for sentence in sentences:
        if current and len(current) + len(sentence) + 1 > max_chars:
            parts.append(current)
            current = current[-overlap:] + " " + sentence if overlap else sentence
        else:
            current = f"{current} {sentence}".strip()
    if current:
        parts.append(current)
    return parts


def chunk_markdown(doc_id, markdown, max_chars=700, overlap=100):
    """Trả list dict {chunk_id, doc_id, heading, text}; bỏ comment HTML và mục rỗng."""
    markdown = re.sub(r"<!--.*?-->", "", markdown, flags=re.S)
    title_match = re.search(r"^# (.+)$", markdown, flags=re.M)
    title = title_match.group(1).strip() if title_match else doc_id
    chunks = []
    for section in re.split(r"^## ", markdown, flags=re.M)[1:]:
        heading, _, body = section.partition("\n")
        body = " ".join(body.split())
        if not body:
            continue
        for part in _split_long(body, max_chars, overlap):
            text = f"{title} — {heading.strip()}: {part}"
            chunk_id = hashlib.sha256(f"{doc_id}\n{text}".encode()).hexdigest()[:16]
            chunks.append(
                {"chunk_id": chunk_id, "doc_id": doc_id, "heading": heading.strip(), "text": text}
            )
    return chunks


@functools.lru_cache(maxsize=2)
def _model(model_name, cache_dir):
    from fastembed import TextEmbedding

    return TextEmbedding(model_name, cache_dir=cache_dir)


def embed(texts, model_name=MODEL, cache_dir=None):
    """Embedding chuẩn hoá (dùng cosine); model nạp 1 lần mỗi process, fastembed chỉ import khi cần."""
    return [vector.tolist() for vector in _model(model_name, cache_dir).embed(list(texts))]
