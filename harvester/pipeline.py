#!/usr/bin/env python3
"""World Ledger harvester.

Pulls the real-time-ish sources named in the spec (GPR spreadsheet, gold
price, TIC foreign Treasury holdings, IMF COFER dollar share), merges the
editorial layer, computes the trust/tension composites, and writes
ledger.json.

Run manually: python harvester/pipeline.py
Runs daily via GitHub Actions (.github/workflows/harvest.yml).

WGC gold demand history and GDELT are not wired yet -- those metrics come
from editorial.json for now and are flagged "editorial": true in the
output, per the spec's harvestable/editorial split.

COFER request shape (IMF SDMX 3.0, saved per the spec's "budget a full
evening, save the exact request URL" advice -- dimension order is
COUNTRY.INDICATOR.FXR_CURRENCY.TYPE_OF_TRANSFORMATION.FREQUENCY):
  https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/COFER/+/G001.AFXRA.CI_USD.SHRO_PT.Q
    -> World allocated FX reserves, USD share of allocated, quarterly
  https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/COFER/+/G001.AFXRA.CI_T.NV_USD.Q
    -> World allocated FX reserves, all currencies, nominal USD, quarterly
COFER's "dollar share" excludes gold by definition (it's a reserves-composition
series, not including monetary gold) -- the gold-inclusive figure the spec
formula wants is derived here from these two series plus live gold price.
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
COFER_BASE = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/COFER/+/G001.AFXRA"
COFER_SHARE_URL = f"{COFER_BASE}.CI_USD.SHRO_PT.Q"
COFER_TOTAL_URL = f"{COFER_BASE}.CI_T.NV_USD.Q"

HEADERS = {"User-Agent": "Mozilla/5.0 (world-ledger harvester; personal project)"}
OZ_PER_TONNE = 32150.7466
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}

# Sanity gate: reject a fetched value if it swings more than this vs. the
# last published ledger, and fall back to the stale previous value instead.
MAX_JUMP = {
    "gpr_index": 2.5, "gold_price_usd_oz": 0.15, "tic_foreign_holdings_usd_bn": 0.20,
    "cofer_usd_share_of_allocated": 0.15, "cofer_allocated_total_usd_bn": 0.15,
}


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


def _latest_cofer_obs(url):
    """SDMX 3.0 JSON: observations are indexed by position into the shared
    TIME_PERIOD value list, not keyed by period -- resolve the mapping."""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    payload = resp.json()["data"]
    struct = payload["structures"][0]
    periods = [v["value"] for v in struct["dimensions"]["observation"][0]["values"]]
    series = next(iter(payload["dataSets"][0]["series"].values()))
    obs = series["observations"]
    last_idx = max(obs.keys(), key=int)
    return float(obs[last_idx][0]), periods[int(last_idx)]


def fetch_cofer(prev_metrics):
    """IMF COFER: World allocated FX reserves, USD share + total nominal value.
    Both series share the same quarterly cadence and as_of, so one failure
    (e.g. schema drift on one series but not the other) still falls back
    cleanly to the previous ledger's pair."""
    try:
        share, as_of_share = _latest_cofer_obs(COFER_SHARE_URL)
        total_usd_bn, as_of_total = _latest_cofer_obs(COFER_TOTAL_URL)
        if as_of_share != as_of_total:
            raise ValueError(f"COFER series out of sync: share={as_of_share} total={as_of_total}")
        share_frac = share / 100.0
        total_usd_bn = total_usd_bn / 1e9

        if not sanity_check("cofer_usd_share_of_allocated", share_frac, prev_metrics):
            raise ValueError("failed sanity gate: cofer share")
        if not sanity_check("cofer_allocated_total_usd_bn", total_usd_bn, prev_metrics):
            raise ValueError("failed sanity gate: cofer total")

        return {
            "cofer_usd_share_of_allocated": {"value": round(share_frac, 4), "as_of": as_of_share,
                                              "source": "IMF COFER (allocated reserves, USD share)", "stale": False},
            "cofer_allocated_total_usd_bn": {"value": round(total_usd_bn, 1), "as_of": as_of_total,
                                              "source": "IMF COFER (allocated reserves, all currencies, nominal USD)", "stale": False},
        }
    except Exception as e:
        print(f"[warn] COFER fetch failed: {e}", file=sys.stderr)
        if prev_metrics and "cofer_usd_share_of_allocated" in prev_metrics:
            share_m = dict(prev_metrics["cofer_usd_share_of_allocated"]); share_m["stale"] = True
            total_m = dict(prev_metrics["cofer_allocated_total_usd_bn"]); total_m["stale"] = True
            return {"cofer_usd_share_of_allocated": share_m, "cofer_allocated_total_usd_bn": total_m}
        return None  # no COFER ever fetched yet -- caller falls back to editorial


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
    cofer = fetch_cofer(prev_metrics)

    gpr_baseline_min = gpr["gpr_baseline_min"] if gpr["gpr_baseline_min"] is not None else None
    gpr_baseline_max = gpr["gpr_baseline_max"] if gpr["gpr_baseline_max"] is not None else None

    ed = editorial["metrics"]
    total_gold_tonnes = ed["total_official_gold_tonnes"]["value"]
    gold_usd_bn = total_gold_tonnes * gold["value"] * OZ_PER_TONNE / 1e9
    treasuries_usd_bn = tic["value"]
    treasuries_vs_gold_ratio = treasuries_usd_bn / gold_usd_bn if gold_usd_bn else None

    # dollar share, gold-inclusive: COFER gives allocated-FX-only dollar share
    # and total (excludes monetary gold by definition) -- fold gold in here.
    # Falls back to the editorial estimate if COFER couldn't be fetched at all.
    if cofer is not None:
        cofer_total = cofer["cofer_allocated_total_usd_bn"]["value"]
        cofer_share = cofer["cofer_usd_share_of_allocated"]["value"]
        dollar_reserves_usd_bn = cofer_total * cofer_share
        dollar_share_gold_incl = {
            "value": round(dollar_reserves_usd_bn / (cofer_total + gold_usd_bn), 4),
            "as_of": cofer["cofer_usd_share_of_allocated"]["as_of"],
            "source": "IMF COFER (allocated dollar share + total) + editorial gold tonnage x live price",
            "stale": cofer["cofer_usd_share_of_allocated"].get("stale", False),
        }
    else:
        dollar_reserves_usd_bn = None
        dollar_share_gold_incl = {**ed["dollar_share_gold_incl"], "editorial": True}

    # ---- trust ----
    n_dollar = norm(dollar_share_gold_incl["value"],
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
            "dollar_share_gold_incl": dollar_share_gold_incl,
            "cb_gold_tonnes_4q": {**ed["cb_gold_tonnes_4q"], "editorial": True},
            "cross_bloc_penalty": {**ed["cross_bloc_penalty"], "editorial": True},
            **({
                "cofer_usd_share_of_allocated": cofer["cofer_usd_share_of_allocated"],
                "cofer_allocated_total_usd_bn": cofer["cofer_allocated_total_usd_bn"],
            } if cofer is not None else {}),
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
