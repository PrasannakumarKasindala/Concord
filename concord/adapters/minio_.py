"""
MinIO dead-letter archive. When a record exhausts its retries, we do not drop it
and we do not let it clog the hot path forever. We serialize the whole record,
including its last error and attempt count, to object storage keyed by
aggregate/date/id, and mark it DEAD in the outbox. A human (or a replay job) can
later list the bucket, read the forensic record, fix the root cause, and replay.

MinIO is S3-compatible, so this same adapter points at real S3 in production by
changing the endpoint. Poison messages are exactly the kind of thing you want in
cheap, durable, greppable object storage rather than in your primary database.
"""

from __future__ import annotations

import json
import time

from ..model import OutboxRecord
from ..ports import DeadLetterArchive


class MinioArchive(DeadLetterArchive):
    def __init__(self, endpoint: str, bucket: str,
                 access_key: str, secret_key: str, secure: bool = False) -> None:
        Minio = _import_minio()
        self._client = Minio(endpoint, access_key=access_key,
                             secret_key=secret_key, secure=secure)
        self._bucket = bucket
        if not self._client.bucket_exists(bucket):
            self._client.make_bucket(bucket)

    def archive(self, record: OutboxRecord) -> str:
        import io
        day = time.strftime("%Y/%m/%d", time.gmtime(record.created_at))
        key = f"{record.aggregate}/{day}/{record.id}.json"
        body = json.dumps({
            "id": record.id,
            "aggregate": record.aggregate,
            "key": record.key,
            "payload_b64": record.payload.hex(),
            "attempts": record.attempts,
            "last_error": record.last_error,
            "created_at": record.created_at,
        }, separators=(",", ":")).encode()
        self._client.put_object(self._bucket, key, io.BytesIO(body), len(body),
                                content_type="application/json")
        return f"s3://{self._bucket}/{key}"


def _import_minio():
    try:
        from minio import Minio
        return Minio
    except ImportError as e:
        raise RuntimeError(
            "MinIO adapter needs minio. Install: pip install 'concord[prod]'"
        ) from e
