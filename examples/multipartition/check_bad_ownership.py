"""The intentionally wrong protocol must yield real invariant violations."""

import argparse
import hashlib
import json
from pathlib import Path

import ownership_invariants

parser = argparse.ArgumentParser()
parser.add_argument("--run-dir", required=True)
args = parser.parse_args()
run = Path(args.run_dir).resolve()
snapshot = json.loads((run / "bad-final-state.json").read_text(encoding="utf-8"))
errors = ownership_invariants.violations(snapshot)
expected = {"one-active-owner-per-bucket", "one-binding-per-full-S", "one-charge-effect-per-full-S"}
ledger = snapshot["platform"]
actual = {e["invariant"] for e in errors}
result = {
    "intentional_bad_protocol": True,
    "expected_failure_reproduced": actual == expected,
    "violations": errors,
    "real_platform_effects": ledger["effects"],
    "real_platform_accounts": ledger["accounts"],
    "checker_sha256": {
        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (Path(__file__), Path(ownership_invariants.__file__))
    },
}
(run / "negative-check.json").write_text(
    json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
)
print(json.dumps(result, ensure_ascii=False))
raise SystemExit(0 if result["expected_failure_reproduced"] else 1)
