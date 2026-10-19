"""etcd directory: one linearizable Range rebuilds a cache at revision r.

The cache is only a routing hint. The business group checks bucket epoch and mode.
"""

import base64
import json

from common import canon, http


def b64(s):
    return base64.b64encode(s.encode()).decode()


class EtcdDirectory:
    def __init__(self, config):
        self.endpoints = config["endpoints"]
        self.prefix = config.get("prefix", "mp/routes/")

    def api(self, path, body):
        last = None
        for endpoint in self.endpoints:
            try:
                return http(endpoint + path, body)
            except Exception as e:
                last = e
        raise RuntimeError("etcd directory unavailable") from last

    def read(self):
        p = self.prefix
        r = self.api(
            "/v3/kv/range", {"key": b64(p), "range_end": b64(p[:-1] + chr(ord(p[-1]) + 1))}
        )
        return {
            base64.b64decode(x["key"]).decode()[len(p) :]: json.loads(base64.b64decode(x["value"]))
            for x in r.get("kvs", [])
        }, int(r["header"]["revision"])

    def create(self, bucket, route):
        key = b64(self.prefix + bucket)
        return self.api(
            "/v3/kv/txn",
            {
                "compare": [{"key": key, "target": "VERSION", "result": "EQUAL", "version": "0"}],
                "success": [{"request_put": {"key": key, "value": b64(canon(route))}}],
                "failure": [{"request_range": {"key": key}}],
            },
        )

    def swap(self, bucket, old, new):
        key = b64(self.prefix + bucket)
        return self.api(
            "/v3/kv/txn",
            {
                "compare": [
                    {"key": key, "target": "VALUE", "result": "EQUAL", "value": b64(canon(old))}
                ],
                "success": [{"request_put": {"key": key, "value": b64(canon(new))}}],
                "failure": [{"request_range": {"key": key}}],
            },
        )
