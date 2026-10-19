"""Independent finite transition model: 3 refund IDs, amounts 1 or 2, source=3.

Each ID is empty, pending, succeeded or rejected; amount is immutable once bound.
Transitions: first admission, same-input retry, conflicting retry (no mutation),
unknown observation (no mutation), definitive success, definitive rejection.
Success after rejection is disallowed by the terminal-evidence premise.
"""

from collections import deque


def explore(broken=False):
    initial = ((0, 0), (0, 0), (0, 0))
    queue = deque([initial])
    parent = {initial: None}
    moves = {}
    edges = 0
    while queue:
        state = queue.popleft()
        occupancy = sum(amount for status, amount in state if status in (1, 2))
        if occupancy > 3:
            trace = []
            while parent[state] is not None:
                trace.append({"move": moves[state], "state": state})
                state = parent[state]
            return {
                "safe": False,
                "states_seen": len(parent),
                "edges": edges,
                "counterexample": list(reversed(trace)),
            }
        for i, (status, amount) in enumerate(state):
            nexts = []
            if status == 0:
                used = sum(a for st, a in state if st == 2) if broken else occupancy
                for x in (1, 2):
                    if used + x <= 3:
                        nexts.append((f"admit RF{i + 1} amount={x}", (1, x)))
            else:
                nexts.append(
                    (f"same RF{i + 1} retry/unknown/conflict preserves binding", (status, amount))
                )
                if status == 1:
                    nexts.extend(
                        [
                            (f"success RF{i + 1}", (2, amount)),
                            (f"final rejection RF{i + 1}", (3, amount)),
                        ]
                    )
            for label, new in nexts:
                target = list(state)
                target[i] = new
                target = tuple(target)
                edges += 1
                if target not in parent:
                    parent[target] = state
                    moves[target] = label
                    queue.append(target)
    return {"safe": True, "reachable_states": len(parent), "edges": edges, "counterexample": None}


if __name__ == "__main__":
    import json

    print(
        json.dumps(
            {"correct": explore(), "broken_ignores_pending": explore(True)},
            ensure_ascii=False,
            indent=2,
        )
    )
