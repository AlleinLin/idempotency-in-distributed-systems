import argparse
import json
import traceback
from pathlib import Path

from common import Trace
from evidence import capture_sources
from native_brokers import NativeBrokers

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", choices=["kafka", "rabbit"], required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    out = Path(args.out)
    out.mkdir()
    capture_sources(Path(__file__).parent, out)
    trace = Trace(out / "history.jsonl")
    brokers = NativeBrokers(args.broker, out / "brokers", trace)
    try:
        config = brokers.initialize()
        (out / "config.json").write_text(json.dumps(config, indent=2), encoding="utf-8")
        print(json.dumps(config), flush=True)
    except Exception:
        (out / "failure.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        brokers.close()
