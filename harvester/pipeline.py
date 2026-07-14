#!/usr/bin/env python3
"""World Ledger harvester -- Week 1 pipeline proof.

Pulls the three real-time-ish sources named in the spec (GPR spreadsheet,
gold price, TIC foreign Treasury holdings), merges the editorial layer,
computes the trust/tension composites, and writes ledger.json.

Run manually: python harvester/pipeline.py
GitHub Actions cron + sanity-gate hardening is Week 2 work.

COFER (dollar share), WGC gold demand history, and GDELT are not wired
yet -- those metrics come from editorial.json for now and are flagged
"editorial": true in the output, per the spec's harvestable/editorial split.
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
HARVESTER_DIR = Path(__file__).resolve().parent
LEDGER_PATH = ROOT / "ledger.json"
HISTORY_DIR = ROOT / "history"
EDITORIAL_PATH = ROOT / "editorial" / "editorial.json"
BASELINE_PATH = HARVESTER_DIR / "baseline_config.json"

GPR_URL = "https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls"
GOLD_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?range=5d&interval=1d"
TIC_URL = "https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/mfh.txt"

HEADERS = {"User-Agent": "Mozilla/5.0 (world-ledger harvester; personal project)"}
OZ_PER_TONNE = 32150.7466
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}

# Sanity gate: reject a fetched value if it swings more than this vs. the
# last published ledger, and fall back to the stale previous value instead.
MAX_JUMP = {"gpr_index": 2.5, "gold_price_usd_oz": 0.15, "tic_foreign_holdings_usd_bn": 0.20}


def load_previous_ledger():
    if LEDGER_PATH.exists():
        return json.loads(LEDGER_PATH.read_text(encoding="utf-8"))
    return None


def sanity_check(name, new_value, prev_metrics):
    prev = prev_metrics.get(name, {}).get("value") if prev_metrics else None
    if prev in (None, 0) or new_value is None:
        return True
    jump = abs(new_value - prev) / abs(prev)
    if jump > MAX_JUMP[name]:
        print(f"[warn] {name} jumped {jump:.0%} ({prev} -> {new_value}) -- rejecting, marking stale", file=sys.stderr)
        return False
    return True


def fetch_gpr(prev_metrics):
    """Real source. Also derives the 2000-2015 baseline min/max from the same file."""
    try:
        resp = requests.get(GPR_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        tmp = HARVESTER_DIR / "_gpr_cache.xls"
        tmp.write_bytes(resp.content)
        df = pd.read_excel(tmp, sheet_name=0)
        tmp.unlink(missing_ok=True)

        df["month"] = pd.to_datetime(df["month"])
        df = df.dropna(subset=["GPR"]).sort_values("month")

        latest = df.iloc[-1]
        value, as_of = float(latest["GPR"]), latest["month"].strftime("%Y-%m")
        trailing_12m = float(df[df["month"] > latest["month"] - pd.DateOffset(months=12)]["GPR"].mean())

        baseline = df[(df["month"] >= "2000-01-01") & (df["month"] <= "2015-12-31")]["GPR"]

        if not sanity_check("gpr_index", value, prev_metrics):
            raise ValueError("failed sanity gate")

        return {
            "gpr_index": {"value": round(value, 2), "as_of": as_of, "source": "Caldara & Iacoviello GPR", "stale": False},
            "gpr_index_12m_avg": round(trailing_12m, 2),
            "gpr_baseline_min": round(float(baseline.min()), 2),
            "gpr_baseline_max": round(float(baseline.max()), 2),
        }
    except Exception as e:
        print(f"[warn] GPR fetch failed: {e}", file=sys.stderr)
        if prev_metrics and "gpr_index" in prev_metrics:
            m = dict(prev_metrics["gpr_index"])
            m["stale"] = True
            return {"gpr_index": m, "gpr_index_12m_avg": None, "gpr_baseline_min": None, "gpr_baseline_max": None}
        raise


def fetch_gold(prev_metrics):
    try:
        resp = requests.get(GOLD_URL, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        result = resp.json()["chart"]["result"][0]
        value = float(result["meta"]["regularMarketPrice"])
        as_of = datetime.fromtimestamp(result["meta"]["regularMarketTime"], tz=timezone.utc).strftime("%Y-%m-%d")

        if not sanity_check("gold_price_usd_oz", value, prev_metrics):
            raise ValueError("failed sanity gate")

        return {"value": round(value, 2), "as_of": as_of, "source": "Yahoo Finance COMEX GC=F", "stale": False}
    except Exception as e:
        print(f"[warn] gold price fetch failed: {e}", file=sys.stderr)
        if prev_metrics and "gold_price_usd_oz" in prev_metrics:
            m = dict(prev_metrics["gold_price_usd_oz"])
            m["stale"] = True
            return m
        raise


def fetch_tic(prev_metrics):
    """TIC 'Major Foreign Holders' text table. Known trap: this legacy URL is
    human-maintained and can drift far behind (it does not track a fixed
    cadence like the others) -- that's real, not a bug, so we surface as_of
    plainly and let the client decide whether it's too stale to trust."""
    try:
        resp = requests.get(TIC_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        lines = resp.text.splitlines()

        year_line_idx = next(i for i, l in enumerate(lines) if l.strip().startswith("Country") and re.search(r"\d{4}", l))
        month_line = lines[year_line_idx - 1]
        year_line = lines[year_line_idx]
        total_line = next(l for l in lines if l.strip().startswith("Grand Total"))

        month_tok = month_line.split()[0]
        year_tok = year_line.split()[1]
        value_tok = total_line.replace("Grand Total", "").split()[0]

        as_of = f"{year_tok}-{MONTHS[month_tok]:02d}"
        value = float(value_tok)

        if not sanity_check("tic_foreign_holdings_usd_bn", value, prev_metrics):
            raise ValueError("failed sanity gate")

        return {"value": value, "as_of": as_of, "source": "US Treasury TIC, Major Foreign Holders (grand total, all holders)", "stale": False}
    except Exception as e:
        print(f"[warn] TIC fetch failed: {e}", file=sys.stderr)
        if prev_metrics and "tic_foreign_holdings_usd_bn" in prev_metrics:
            m = dict(prev_metrics["tic_foreign_holdings_usd_bn"])
            m["stale"] = True
            return m
        raise


def norm(x, lo, hi):
    if x is None:
        return None
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def tension_state(t):
    if t < 25:
        return "Calm"
    if t < 50:
        return "Strain"
    if t < 75:
        return "Fracture"
    return "Rupture"


def main():
    prev = load_previous_ledger()
    prev_metrics = prev["metrics"] if prev else {}

    editorial = json.loads(EDITORIAL_PATH.read_text(encoding="utf-8"))
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))

    gpr = fetch_gpr(prev_metrics)
    gold = fetch_gold(prev_metrics)
    tic = fetch_tic(prev_metrics)

    gpr_baseline_min = gpr["gpr_baseline_min"] if gpr["gpr_baseline_min"] is not None else None
    gpr_baseline_max = gpr["gpr_baseline_max"] if gpr["gpr_baseline_max"] is not None else None

    ed = editorial["metrics"]
    total_gold_tonnes = ed["total_official_gold_tonnes"]["value"]
    gold_usd_bn = total_gold_tonnes * gold["value"] * OZ_PER_TONNE / 1e9
    treasuries_usd_bn = tic["value"]
    treasuries_vs_gold_ratio = treasuries_usd_bn / gold_usd_bn if gold_usd_bn else None

    # ---- trust ----
    n_dollar = norm(ed["dollar_share_gold_incl"]["value"],
                     baseline["dollar_share_gold_incl"]["min"], baseline["dollar_share_gold_incl"]["max"])
    n_ratio = norm(treasuries_vs_gold_ratio,
                    baseline["treasuries_vs_gold_ratio"]["min"], baseline["treasuries_vs_gold_ratio"]["max"])
    n_gold_buy = norm(ed["cb_gold_tonnes_4q"]["value"],
                       baseline["cb_gold_purchases_trailing_4q_tonnes"]["min"],
                       baseline["cb_gold_purchases_trailing_4q_tonnes"]["max"])
    n_gpr_12m = norm(gpr["gpr_index_12m_avg"], gpr_baseline_min, gpr_baseline_max) if gpr_baseline_min is not None else None

    trust_terms = [(0.45, n_dollar), (0.25, n_ratio), (0.20, (1 - n_gold_buy) if n_gold_buy is not None else None),
                   (0.10, (1 - n_gpr_12m) if n_gpr_12m is not None else None)]
    trust_weight = sum(w for w, v in trust_terms if v is not None)
    trust = round(100 * sum(w * v for w, v in trust_terms if v is not None) / trust_weight, 1) if trust_weight else None

    # ---- tension ---- (GDELT term is v2 -- not fetched yet; weights renormalised over what we have)
    n_gpr_monthly = norm(gpr["gpr_index"]["value"], gpr_baseline_min, gpr_baseline_max) if gpr_baseline_min is not None else None
    n_penalty = norm(ed["cross_bloc_penalty"]["value"], baseline["cross_bloc_penalty"]["min"], baseline["cross_bloc_penalty"]["max"])

    tension_terms = [(0.55, n_gpr_monthly), (0.20, n_penalty)]
    tension_weight = sum(w for w, v in tension_terms if v is not None)
    tension = round(100 * sum(w * v for w, v in tension_terms if v is not None) / tension_weight, 1) if tension_weight else None

    prev_trust = prev["composites"]["trust"]["value"] if prev else None
    prev_tension = prev["composites"]["tension"]["value"] if prev else None

    ledger = {
        "sealed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 1,
        "composites": {
            "trust": {"value": trust, "delta": None if prev_trust is None else round(trust - prev_trust, 1)},
            "tension": {"value": tension, "state": tension_state(tension),
                        "delta": None if prev_tension is None else round(tension - prev_tension, 1),
                        "note": "GDELT term pending (v2); weights renormalised over GPR + cross-bloc penalty"},
        },
        "metrics": {
            "gpr_index": gpr["gpr_index"],
            "gpr_index_12m_avg": {"value": gpr["gpr_index_12m_avg"], "as_of": gpr["gpr_index"]["as_of"], "source": "Caldara & Iacoviello GPR"},
            "gold_price_usd_oz": gold,
            "tic_foreign_holdings_usd_bn": tic,
            "gold_vs_treasuries_usd_bn": {"gold": round(gold_usd_bn, 1), "treasuries": round(treasuries_usd_bn, 1),
                                           "as_of": tic["as_of"], "note": f"gold = editorial {total_gold_tonnes}t x live price"},
            "dollar_share_gold_incl": {**ed["dollar_share_gold_incl"], "editorial": True},
            "cb_gold_tonnes_4q": {**ed["cb_gold_tonnes_4q"], "editorial": True},
            "cross_bloc_penalty": {**ed["cross_bloc_penalty"], "editorial": True},
        },
        "blocs": editorial["blocs"],
        "dispatch": editorial["dispatch"],
        "scenarios": editorial["scenarios"],
    }

    LEDGER_PATH.write_text(json.dumps(ledger, indent=2, ensure_ascii=False), encoding="utf-8")
    HISTORY_DIR.mkdir(exist_ok=True)
    month_filename = f"{datetime.now(timezone.utc).strftime('%Y-%m')}.json"
    month_file = HISTORY_DIR / month_filename
    month_file.write_text(json.dumps(ledger, indent=2, ensure_ascii=False), encoding="utf-8")

    # client fetches this manifest to discover which history/*.json files exist
    # (a static site has no directory listing) -- keep it de-duped and sorted
    index_path = HISTORY_DIR / "index.json"
    existing = json.loads(index_path.read_text(encoding="utf-8"))["files"] if index_path.exists() else []
    files = sorted(set(existing) | {month_filename})
    index_path.write_text(json.dumps({"files": files}, indent=2), encoding="utf-8")

    print(f"wrote {LEDGER_PATH} -- trust={trust} tension={tension} ({tension_state(tension)})")


if __name__ == "__main__":
    main()
