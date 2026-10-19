"""Ownership safety rules independent of FSM, routing code and runner."""

import collections
import json


def violations(snapshot):
    owners = collections.defaultdict(list)
    bindings = collections.defaultdict(list)
    effects = {(r["scope"], r["key"]): r for r in snapshot["platform"]["effects"]}
    errors = []
    for group, state in snapshot["groups"].items():
        for bid, bucket in state["buckets"].items():
            if bucket["mode"] != "active":
                continue
            owners[bid].append({"group": group, "epoch": bucket["epoch"]})
            for operation in bucket["operations"].values():
                s = json.dumps(
                    operation["s"], ensure_ascii=False, sort_keys=True, separators=(",", ":")
                )
                binding = {
                    "group": group,
                    "bucket": bid,
                    "c": operation["c"],
                    "scope": operation["scope"],
                    "key": operation["key"],
                }
                effect = effects.get((operation["scope"], operation["key"]))
                if effect:
                    binding["provider_effect"] = json.loads(effect["result"])["provider_charge_id"]
                bindings[s].append(binding)
    for bid, choices in owners.items():
        if len(choices) > 1:
            errors.append(
                {"invariant": "one-active-owner-per-bucket", "bucket": bid, "owners": choices}
            )
    for s, choices in bindings.items():
        if len({(v["c"], v["scope"], v["key"]) for v in choices}) > 1:
            errors.append(
                {"invariant": "one-binding-per-full-S", "s": json.loads(s), "bindings": choices}
            )
        external = {v["provider_effect"] for v in choices if "provider_effect" in v}
        if len(external) > 1:
            errors.append(
                {
                    "invariant": "one-charge-effect-per-full-S",
                    "s": json.loads(s),
                    "effects": sorted(external),
                }
            )
    return errors
