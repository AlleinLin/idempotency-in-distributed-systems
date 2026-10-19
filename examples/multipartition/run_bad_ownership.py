"""Intentionally wrong double ownership: actual Raft groups and HTTP effects.

This path is explicitly unsafe. The bad controller initializes an empty active
destination while the source remains active, then directs the same S to both.
Local uniqueness remains intact, but no migration handoff protects S globally.
"""

import argparse
from pathlib import Path

from common import canon, http, identity, purchase_input
from run_lab import Lab


def main(out):
    lab = Lab(out)
    ok = False
    try:
        lab.initialize()
        for name in ("pay", "relay", "consume"):
            lab.stop(name)
        # Advance the source namespace before the faulty empty destination opens.
        padding = identity(4, "source-padding")
        assert "charge_id" in lab.cl.command(
            "b1",
            "accept",
            s=padding,
            input=purchase_input(),
            input_version="external-v2",
            now_upper=1,
        )
        assert (
            lab.cl.group(
                "G2", "/command", {"kind": "init", "bucket": "b1", "epoch": 2, "stock": {}}
            )["code"]
            == "created"
        )
        s = identity(1, "bad-double-owner")
        lab.cl.group(
            "G2",
            "/command",
            {"kind": "session", "bucket": "b1", "epoch": 2, "s": s, "deadline": 1000},
        )
        requests = []
        for group, epoch in (("G1", 1), ("G2", 2)):
            lab.trace("bad_route_invocation", group=group, epoch=epoch, s=s, input=purchase_input())
            result = lab.cl.group(
                group,
                "/command",
                {
                    "kind": "accept",
                    "bucket": "b1",
                    "epoch": epoch,
                    "s": s,
                    "input": purchase_input(),
                    "input_version": "external-v2",
                    "now_upper": 1,
                },
            )
            lab.trace("bad_route_response", group=group, s=s, result=result)
            requests.append((group, epoch))
        for group, epoch in requests:
            sk = canon(s)
            operation = lab.cl.state(group)["buckets"]["b1"]["operations"][sk]
            claim = lab.cl.group(
                group,
                "/command",
                {
                    "kind": "claim",
                    "bucket": "b1",
                    "epoch": epoch,
                    "s_key": sk,
                    "owner": "bad-protocol-worker",
                },
            )
            assert claim["reservation_ready"] is True
            result = http(
                lab.cfg["proxy"] + "/call",
                {
                    "scope": operation["scope"],
                    "key": operation["key"],
                    "kind": "charge",
                    "input": operation["payment_input"],
                },
            )
            lab.cl.group(
                group,
                "/command",
                {
                    "kind": "observe",
                    "bucket": "b1",
                    "epoch": epoch,
                    "s_key": sk,
                    "owner": "bad-protocol-worker",
                    "work_epoch": claim["work_epoch"],
                    "provider_result": result,
                },
            )
            lab.trace("bad_protocol_external_effect", group=group, s=s, result=result)
        lab.save("bad-final-state")
        ok = True
    finally:
        lab.cleanup()
        (lab.out / "run-status.json").write_text(
            canon({"completed": ok, "intentional_bad_protocol": True}), encoding="utf-8"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    main(Path(parser.parse_args().run_dir))
