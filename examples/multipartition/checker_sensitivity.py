"""Inject evidence mutations in memory. Never modifies original run artifacts."""

import argparse
import copy
import hashlib
import json
from pathlib import Path

import check_run


def main(run):
    original = check_run.load
    final = original(run / "final-state.json")
    key = next(
        k
        for k, o in final["groups"]["G2"]["buckets"]["b1"]["operations"].items()
        if o["s"]["account_id"] == 1 and o["s"]["idempotency_key"] == "K1"
    )

    def op(snapshot):
        return snapshot["groups"]["G2"]["buckets"]["b1"]["operations"][key]

    def alter_amount(s):
        op(s)["payment_input"]["amount_minor"] = 3001

    def alter_result(s):
        op(s)["result"]["payment_result"]["provider_charge_id"] = "X999"

    def alter_key(s):
        op(s)["key"] = "replacement-payment-key"

    def duplicate_effect(s):
        s["platform"]["effects"].append(copy.deepcopy(s["platform"]["effects"][0]))

    def alter_f(s):
        s["consumer"]["inbox"][0]["f"] = 999

    def alter_decision(s):
        s["groups"]["G2"]["buckets"]["b1"]["transactions"]["TX:P:b1:5"]["decision"] = "abort"

    def alter_stock(s):
        s["groups"]["G2"]["buckets"]["b2"]["stock"]["SKU-A"] = 4

    def alter_source(s):
        s["groups"]["G1"]["buckets"]["b1"]["operations"][key]["c"] = "C:changed"

    def dual_owner(s):
        s["groups"]["G1"]["buckets"]["b1"]["mode"] = "active"

    mutations = [
        alter_amount,
        alter_result,
        alter_key,
        duplicate_effect,
        alter_f,
        alter_decision,
        alter_stock,
        alter_source,
        dual_owner,
    ]
    reports = []
    baseline = check_run.check(run)
    assert baseline["passed"]
    for mutate in mutations:
        changed = copy.deepcopy(final)
        mutate(changed)
        check_run.load = lambda path: (
            changed if path == run / "final-state.json" else original(path)
        )
        try:
            report = check_run.check(run)
            failed = [r["check"] for r in report["results"] if not r["passed"]]
            reports.append(
                {
                    "mutation": mutate.__name__,
                    "detected": not report["passed"],
                    "failed_checks": failed,
                }
            )
        finally:
            check_run.load = original
    result = {
        "baseline_passed": True,
        "all_mutations_detected": all(r["detected"] for r in reports),
        "original_evidence_changed": False,
        "mutations": reports,
        "checker_sha256": hashlib.sha256(Path(check_run.__file__).read_bytes()).hexdigest(),
    }
    (run / "checker-sensitivity.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False))
    assert result["all_mutations_detected"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", required=True)
    main(Path(parser.parse_args().run_dir).resolve())
