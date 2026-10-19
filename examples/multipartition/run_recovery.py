"""Additional real-process scope: session lifecycle, quorum loss, snapshots, refunds."""

import argparse
import traceback
from pathlib import Path

from common import canon, http, identity, purchase_input
from run_lab import Lab, wait_for


def main(out):
    lab = Lab(out)
    ok = False
    try:
        lab.initialize()
        for account in (1, 2, 3):
            s = identity(account, "K1")
            lab.invoke(s)
            lab.paid(s)
            lab.fulfilled(s)
        # A closed session stops new/replayed submissions. Durable existing work
        # remains the recovery worker's responsibility and reaches its result.
        lab.stop("pay")
        s = identity(4, "close-pending")
        lab.invoke(s)
        close = lab.cl.command("b1", "close", s=s)
        assert close["closed"] is True
        assert lab.invoke(s) == {"code": "operation_expired"}
        assert lab.invoke(identity(4, "closed-new")) == {"code": "operation_expired"}
        assert lab.cl.command("b1", "session", s=s, deadline=2000)["code"] == "session_conflict"
        expired = lab.cl.command(
            "b2",
            "accept",
            s=identity(5, "expired"),
            input=purchase_input(),
            input_version="external-v2",
            now_upper=1000,
        )
        assert expired == {"code": "operation_expired"}
        lab.trace("session_lifecycle", closed=close, expired=expired)
        lab.svc("pay", name="pay-after-close")
        lab.paid(s)
        lab.fulfilled(s)
        lab.save("01-session-lifecycle")
        # Entire b2 group and control majority are unavailable. Cached direct b3
        # routing and the b3 payment/message path still make progress.
        for group in ("G2", "control"):
            for j in range(3):
                lab.stop(f"{group}-{j}")
        lab.trace("groups_fully_unavailable", groups=["G2", "control"])
        s = identity(6, "local-availability")
        lab.invoke(s)
        lab.paid(s)
        lab.fulfilled(s)
        lab.trace("unaffected_partition_completed", s=s, original_result=lab.operation(s)["result"])
        for group in ("G2", "control"):
            for j in range(3):
                lab.restart(f"{group}-{j}")
            lab.cl.leaders.pop(group, None)
            wait_for(lambda group=group: lab.cl.state(group), label=group + " restored quorum")
        lab.save("02-local-availability")
        for name in ("pay-after-close", "relay", "consume"):
            lab.stop(name)
        s = identity(7, "bundle")
        lab.invoke(s, purchase_input(bundle=True))
        marker = lab.out / "snapshot-prepared.json"
        lab.svc("coordinate", name="snapshot-coordinate-prepared", pause="prepare", marker=marker)
        wait_for(marker.exists, label="snapshot prepared transaction")
        lab.stop("snapshot-coordinate-prepared")
        before = lab.save("03-before-full-snapshot-restart")
        # Each node produces a real Raft snapshot, then all 12 authority processes
        # exit. SQLite services and JetStream remain independent surviving domains.
        for group in lab.cfg["groups"]:
            minimum = before["groups"][group]["applied_index"]
            for j in range(3):
                name = f"{group}-{j}"
                url = lab.nodes[name]["http"]
                wait_for(
                    lambda url=url, minimum=minimum: (
                        int(http(url + "/status")["stats"]["applied_index"]) >= minimum
                    ),
                    label=name + " applied through checkpoint",
                )
                lab.trace("snapshot_invocation", node=name, url=url)
                result = http(url + "/snapshot", {})
                lab.trace("snapshot_response", node=name, result=result)
                assert result["code"] == "snapshot_complete"
        lab.trace("all_authority_snapshots_persisted")
        for name in lab.nodes:
            lab.stop(name)
        lab.trace("all_authority_processes_stopped")
        for name in lab.nodes:
            lab.restart(name)
        lab.cl.leaders.clear()
        for group in lab.cfg["groups"]:
            wait_for(
                lambda group=group: lab.cl.state(group), label=group + " cold snapshot recovery"
            )
        after = lab.save("04-after-full-snapshot-restart")
        for group in before["groups"]:
            for field in ("buckets", "routes"):
                assert before["groups"][group][field] == after["groups"][group][field]
        lab.svc("pay", name="pay-after-snapshot")
        lab.svc("relay", name="relay-after-snapshot")
        lab.svc("consume", name="consume-after-snapshot")
        lab.svc("coordinate", name="coordinate-after-snapshot")
        lab.paid(s)
        lab.fulfilled(s)
        lab.save("05-transaction-recovered")
        # Unknown refund occupies 2000 of the original charge's 3000 capacity.
        rf_s = identity(1, "RF-K1", action="refund.create.v1", session="RF-E1")
        rf_input = {"source_c": "C:b1:1", "currency": "CNY", "amount_minor": 2000}
        rf = lab.cl.command("b1", "refund_accept", s=rf_s, input=rf_input, now_upper=1)
        assert rf["rf"] == "RF:b1:4"
        pending = http(
            lab.cfg["platform"] + "/call",
            {
                "scope": rf["scope"],
                "key": rf["key"],
                "kind": "refund",
                "input": rf["provider_input"],
                "mode": "pending",
            },
        )
        assert pending["status"] == "pending"
        retry = lab.cl.command("b1", "refund_accept", s=rf_s, input=rf_input, now_upper=1)
        assert retry == rf
        excessive = lab.cl.command(
            "b1",
            "refund_accept",
            s=identity(1, "RF-OVER", action="refund.create.v1", session="RF-E1"),
            input=dict(rf_input, amount_minor=1200),
            now_upper=1,
        )
        assert excessive == {"code": "refund_capacity_exceeded"}
        conflict = lab.cl.command(
            "b1", "refund_accept", s=rf_s, input=dict(rf_input, amount_minor=500), now_upper=1
        )
        assert conflict == {"code": "conflict"}
        lab.trace(
            "refund_unknown_capacity",
            s=rf_s,
            refund=rf,
            pending=pending,
            excessive=excessive,
            conflict=conflict,
        )
        lab.save("06-refund-unknown-capacity")
        http(
            lab.cfg["platform"] + "/resolve",
            {
                "scope": rf["scope"],
                "key": rf["key"],
                "kind": "refund",
                "input": rf["provider_input"],
            },
        )
        marker = lab.out / "refund-provider-paused.json"
        lab.svc("refund", name="refund-before-save", pause="refund_provider", marker=marker)
        wait_for(marker.exists, label="refund actual provider result")
        lab.stop("refund-before-save")
        assert lab.cl.bucket("b1")["refunds"][canon(rf_s)]["result"] is None
        lab.svc("refund", name="refund-after-crash")
        wait_for(
            lambda: lab.cl.bucket("b1")["refunds"][canon(rf_s)]["result"],
            label="refund recovery result",
        )
        second_s = identity(1, "RF-K2", action="refund.create.v1", session="RF-E1")
        second = lab.cl.command(
            "b1", "refund_accept", s=second_s, input=dict(rf_input, amount_minor=1000), now_upper=1
        )
        assert second["rf"] == "RF:b1:5"
        wait_for(
            lambda: lab.cl.bucket("b1")["refunds"][canon(second_s)]["result"],
            label="remaining refund capacity",
        )
        assert lab.cl.command(
            "b1",
            "refund_accept",
            s=identity(1, "RF-ZERO", action="refund.create.v1", session="RF-E1"),
            input=dict(rf_input, amount_minor=1),
            now_upper=1,
        ) == {"code": "refund_capacity_exceeded"}
        lab.save("07-refunds-recovered")
        lab.save("final-state")
        ok = True
    except Exception:
        (lab.out / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        lab.cleanup()
        (lab.out / "run-status.json").write_text(
            canon({"completed": ok, "scope": "recovery-and-lifecycle"}), encoding="utf-8"
        )


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-dir", required=True)
    main(Path(p.parse_args().run_dir))
