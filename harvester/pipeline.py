#!/usr/bin/env python3
"""World Ledger harvester.

Pulls the real-time-ish sources named in the spec (GPR spreadsheet, gold
price, TIC foreign Treasury holdings, IMF COFER dollar share, GDELT tone),
merges the editorial layer, computes the trust/tension composites, and
writes ledger.json.

Run manually: python harvester/pipeline.py
Runs daily via GitHub Actions (.github/workflows/harvest.yml).

WGC gold demand history is not wired yet -- that metric comes from
editorial.json for now and is flagged "editorial": true in the output,
per the spec's harvestable/editorial split.

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

GDELT request shape (DOC 2.0 API, no auth, rate-limited to ~1 req/5s --
this harvester makes exactly one call per run so that's never an issue):
  https://api.gdeltproject.org/api/v2/doc/doc?query=(sanctions OR tariff OR
    "trade war" OR geopolitical OR conflict)&mode=timelinetone&format=json&timespan=30d
    -> daily average tone (-100..+100, negative = negative coverage) for the
       query over the trailing 30 days; we average the daily points into
       one gdelt_conflict_tone_30d figure, per the spec's "always aggregate
       over windows, never react to single events" warning.

Observer Station (the view from the nonaligned South) request shapes:
  https://api.frankfurter.app/{30-days-ago}..?from=USD&to=ZAR
    -> daily USD/ZAR rate series (ECB reference rates); latest value + a
       30-day realised volatility (stdev of daily log returns).
  https://comtradeapi.un.org/public/v1/preview/C/A/HS?reporterCode=710&
    period={year}&cmdCode=TOTAL&flowCode=X
    -> South Africa's (reporterCode 710) exports by partner, no API key
       needed on the free "preview" tier. Filter to motCode=0 (all modes
       combined, isAggregate=true) to get one row per partner rather than
       a mode-of-transport breakdown -- the unfiltered response can hit
       the free tier's 500-record/call cap and silently truncate.
  https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json
    -> static reference mapping Comtrade's numeric partnerCode to ISO3,
       used with blocs.json to classify each partner as west/east/
       africa/other for the export-split composite.
  https://custom.resbank.co.za/SarbWebApi/WebIndicators/CurrentMarketRates
    -> SARB's live policy (repo) rate, among other market rates. No clean
       API exists for gold/FX reserves (published as monthly PDF notices
       only) -- that figure stays editorial, same treatment as WGC gold
       demand.
"""
import json
import math
import re
import statistics
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
HARVESTER_DIR = Path(__file__).resolve().parent
LEDGER_PATH = ROOT / "ledger.json"
HISTORY_DIR = ROOT / "history"
EDITORIAL_PATH = ROOT / "editorial" / "editorial.json"
BASELINE_PATH = HARVESTER_DIR / "baseline_config.json"
BLOCS_PATH = HARVESTER_DIR / "blocs.json"

GPR_URL = "https://www.matteoiacoviello.com/gpr_files/data_gpr_export.xls"
GOLD_URL = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?range=5d&interval=1d"
TIC_URL = "https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/mfh.txt"
COFER_BASE = "https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/COFER/+/G001.AFXRA"
COFER_SHARE_URL = f"{COFER_BASE}.CI_USD.SHRO_PT.Q"
COFER_TOTAL_URL = f"{COFER_BASE}.CI_T.NV_USD.Q"
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_QUERY = '(sanctions OR tariff OR "trade war" OR geopolitical OR conflict)'
FX_URL = "https://api.frankfurter.app"
COMTRADE_URL = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"
COMTRADE_PARTNERS_URL = "https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json"
SA_REPORTER_CODE = "710"
SARB_URL = "https://custom.resbank.co.za/SarbWebApi/WebIndicators/CurrentMarketRates"

# UN-recognised African states (ISO3) -- a fixed geographic fact, unlike the
# west/east bloc alignment in blocs.json which is an editorial judgment call.
AFRICA_ISO3 = {
    "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
    "COM", "COG", "COD", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
    "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
    "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
    "STP", "SEN", "SYC", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO",
    "TUN", "UGA", "ZMB", "ZWE",
}

HEADERS = {"User-Agent": "Mozilla/5.0 (world-ledger harvester; personal project)"}
OZ_PER_TONNE = 32150.7466
MONTHS = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)}

# Sanity gate: reject a fetched value if it swings more than this vs. the
# last published ledger, and fall back to the stale previous value instead.
MAX_JUMP = {
    "gpr_index": 2.5, "gold_price_usd_oz": 0.15, "tic_foreign_holdings_usd_bn": 0.20,
    "cofer_usd_share_of_allocated": 0.15, "cofer_allocated_total_usd_bn": 0.15,
    "usdzar_rate": 0.15, "sarb_repo_rate": 0.30,
}
# GDELT tone oscillates close to zero, where a percentage-jump gate is
# unstable (small denominators inflate trivial swings) -- use an absolute
# swing threshold instead, on the -100..+100 tone scale.
GDELT_MAX_ABS_JUMP = 4.0


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


def fetch_gdelt(prev_metrics):
    """GDELT DOC 2.0 timelinetone: daily average tone of conflict/tariff/
    sanctions coverage, aggregated over the trailing 30 days. No live feed
    existed for this in v1 -- optional, degrades to the tension composite's
    existing renormalised weights (GPR + penalty only) if unavailable."""
    try:
        resp = requests.get(GDELT_URL, headers=HEADERS, timeout=30, params={
            "query": GDELT_QUERY, "mode": "timelinetone", "format": "json", "timespan": "30d",
        })
        resp.raise_for_status()
        points = resp.json()["timeline"][0]["data"]
        if not points:
            raise ValueError("empty GDELT timeline")
        tone_30d = sum(p["value"] for p in points) / len(points)
        as_of = points[-1]["date"][:8]
        as_of = f"{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}"

        prev = prev_metrics.get("gdelt_conflict_tone_30d", {}).get("value") if prev_metrics else None
        if prev is not None and abs(tone_30d - prev) > GDELT_MAX_ABS_JUMP:
            print(f"[warn] gdelt_conflict_tone_30d jumped {prev} -> {tone_30d} -- rejecting, marking stale", file=sys.stderr)
            raise ValueError("failed sanity gate")

        return {"value": round(tone_30d, 3), "as_of": as_of,
                "source": f"GDELT DOC 2.0 (30d avg tone, query: {GDELT_QUERY})", "stale": False}
    except Exception as e:
        print(f"[warn] GDELT fetch failed: {e}", file=sys.stderr)
        if prev_metrics and "gdelt_conflict_tone_30d" in prev_metrics:
            m = dict(prev_metrics["gdelt_conflict_tone_30d"])
            m["stale"] = True
            return m
        return None  # no GDELT ever fetched yet -- tension falls back to renormalised weights


def fetch_fx(prev_observer):
    """USD/ZAR via frankfurter.app: latest rate + 30-day realised volatility
    (stdev of daily log returns). Takes the previous ledger's own "observer"
    sub-dict directly rather than the metrics dict the other sanity checks
    use -- Observer Station lives at the ledger's top level, not in metrics."""
    prev = (prev_observer or {}).get("usdzar", {})
    try:
        start = (datetime.now(timezone.utc) - timedelta(days=35)).strftime("%Y-%m-%d")
        resp = requests.get(f"{FX_URL}/{start}..", headers=HEADERS, timeout=20,
                             params={"from": "USD", "to": "ZAR"})
        resp.raise_for_status()
        rates = resp.json()["rates"]
        dates = sorted(rates.keys())
        series = [rates[d]["ZAR"] for d in dates]
        latest_rate, as_of = series[-1], dates[-1]

        prev_rate = prev.get("rate")
        if prev_rate and abs(latest_rate - prev_rate) / abs(prev_rate) > MAX_JUMP["usdzar_rate"]:
            print(f"[warn] usdzar_rate jumped {prev_rate} -> {latest_rate} -- rejecting, marking stale", file=sys.stderr)
            raise ValueError("failed sanity gate")

        log_returns = [math.log(series[i] / series[i - 1]) for i in range(1, len(series)) if series[i - 1]]
        vol_30d = round(statistics.pstdev(log_returns), 4) if len(log_returns) > 1 else None

        return {"rate": round(latest_rate, 4), "vol_30d": vol_30d, "as_of": as_of,
                "source": "frankfurter.app (ECB reference rates)", "stale": False}
    except Exception as e:
        print(f"[warn] FX fetch failed: {e}", file=sys.stderr)
        if prev:
            return {**prev, "stale": True}
        return None  # no FX ever fetched yet


def fetch_comtrade(prev_observer, blocs):
    """South Africa's exports by partner (UN Comtrade free "preview" tier,
    no key needed), classified into west/east/africa/other via blocs.json +
    AFRICA_ISO3. Comtrade releases lag real time -- try the current year,
    fall back a year or two if that year has no data published yet."""
    prev = (prev_observer or {}).get("export_split", {})
    try:
        partners_resp = requests.get(COMTRADE_PARTNERS_URL, headers=HEADERS, timeout=30)
        partners_resp.raise_for_status()
        code_to_iso3 = {str(r["PartnerCode"]): r.get("PartnerCodeIsoAlpha3")
                        for r in partners_resp.json()["results"]}

        this_year = datetime.now(timezone.utc).year
        rows, used_year = None, None
        for year in (this_year, this_year - 1, this_year - 2):
            resp = requests.get(COMTRADE_URL, headers=HEADERS, timeout=30, params={
                "reporterCode": SA_REPORTER_CODE, "period": str(year),
                "cmdCode": "TOTAL", "flowCode": "X",
            })
            resp.raise_for_status()
            # motCode 0 = all modes of transport combined (isAggregate) --
            # one row per partner. The unfiltered response mixes in a
            # per-mode-of-transport breakdown and can hit the free tier's
            # 500-record cap.
            candidate = [r for r in resp.json().get("data", [])
                         if r.get("motCode") == 0 and r.get("partnerCode") != 0]
            if candidate:
                rows, used_year = candidate, year
                break
        if not rows:
            raise ValueError("no Comtrade data for recent years")

        totals = {"west": 0.0, "east": 0.0, "africa": 0.0, "other": 0.0}
        grand_total = 0.0
        for row in rows:
            value = row.get("primaryValue") or 0.0
            iso3 = code_to_iso3.get(str(row["partnerCode"]))
            grand_total += value
            if iso3 in blocs.get("west", []):
                totals["west"] += value
            elif iso3 in blocs.get("east", []):
                totals["east"] += value
            elif iso3 in AFRICA_ISO3:
                totals["africa"] += value
            else:
                totals["other"] += value
        if not grand_total:
            raise ValueError("zero grand total")

        shares = {k: round(v / grand_total, 4) for k, v in totals.items()}
        shares.update({"as_of": str(used_year),
                        "source": "UN Comtrade (South Africa exports by partner, annual)",
                        "stale": False})
        return shares
    except Exception as e:
        print(f"[warn] Comtrade fetch failed: {e}", file=sys.stderr)
        if prev:
            return {**prev, "stale": True}
        return None  # no Comtrade data ever fetched yet


def fetch_sarb(prev_observer):
    """SARB policy (repo) rate -- live via WebIndicators/CurrentMarketRates.
    Gold/FX reserves have no equivalent clean API (monthly PDF notices
    only), so that figure is merged in from editorial.json by the caller."""
    prev = (prev_observer or {}).get("sarb", {})
    try:
        resp = requests.get(SARB_URL, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        repo_row = next(r for r in resp.json() if r.get("Name") == "SARB Policy Rate")
        repo_rate, as_of = repo_row["Value"] / 100, repo_row["Date"]

        prev_repo = prev.get("repo")
        if prev_repo and abs(repo_rate - prev_repo) / abs(prev_repo) > MAX_JUMP["sarb_repo_rate"]:
            print(f"[warn] sarb_repo_rate jumped {prev_repo} -> {repo_rate} -- rejecting, marking stale", file=sys.stderr)
            raise ValueError("failed sanity gate")

        return {"repo": round(repo_rate, 4), "repo_as_of": as_of,
                "source": "SARB WebIndicators (CurrentMarketRates)", "stale": False}
    except Exception as e:
        print(f"[warn] SARB fetch failed: {e}", file=sys.stderr)
        if prev:
            return {**prev, "stale": True}
        return None  # no SARB rate ever fetched yet


def norm(x, lo, hi):
    if x is None:
        return None
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def composite_from_terms(terms):
    """terms: list of (id: str, raw_weight: float, oriented_norm: float|None) --
    oriented_norm is already inversion-adjusted by the caller (e.g. `1 - norm(...)`)
    so higher always means "more of the composite" here.
    Returns (value: float|None rounded to 1dp, components: list[dict]) where each
    dict is {id, weight (post-renormalization, 4dp), norm (4dp), contribution (2dp)}
    for terms that had a non-None value this run."""
    available = [(cid, w, v) for cid, w, v in terms if v is not None]
    weight_sum = sum(w for _, w, v in available)
    if not weight_sum:
        return None, []
    value = round(100 * sum(w * v for _, w, v in available) / weight_sum, 1)
    components = []
    for cid, w, v in available:
        eff_w = w / weight_sum
        components.append({
            "id": cid,
            "weight": round(eff_w, 4),
            "norm": round(v, 4),
            "contribution": round(100 * eff_w * v, 2),
        })
    return value, components


def with_delta_contrib(components, prev_components):
    """Attach delta_contrib (2dp) vs. the previous ledger's same-id component's
    contribution. None if that id wasn't present previously (first run after this
    ships, or the term simply wasn't fetchable last run)."""
    prev_by_id = {c["id"]: c["contribution"] for c in (prev_components or [])}
    return [
        {**c, "delta_contrib": (round(c["contribution"] - prev_by_id[c["id"]], 2)
                                 if c["id"] in prev_by_id else None)}
        for c in components
    ]


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
    prev_observer = prev.get("observer", {}) if prev else {}

    editorial = json.loads(EDITORIAL_PATH.read_text(encoding="utf-8"))
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    blocs = json.loads(BLOCS_PATH.read_text(encoding="utf-8"))

    gpr = fetch_gpr(prev_metrics)
    gold = fetch_gold(prev_metrics)
    tic = fetch_tic(prev_metrics)
    cofer = fetch_cofer(prev_metrics)
    gdelt = fetch_gdelt(prev_metrics)
    fx = fetch_fx(prev_observer)
    export_split = fetch_comtrade(prev_observer, blocs)
    sarb_live = fetch_sarb(prev_observer)

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

    trust_terms = [
        ("dollar_share", 0.45, n_dollar),
        ("treasuries_ratio", 0.25, n_ratio),
        ("gold_buying", 0.20, (1 - n_gold_buy) if n_gold_buy is not None else None),
        ("gpr_12m", 0.10, (1 - n_gpr_12m) if n_gpr_12m is not None else None),
    ]
    trust, trust_components = composite_from_terms(trust_terms)

    # ---- tension ---- (weights renormalise over whichever terms are available this run)
    n_gpr_monthly = norm(gpr["gpr_index"]["value"], gpr_baseline_min, gpr_baseline_max) if gpr_baseline_min is not None else None
    n_penalty = norm(ed["cross_bloc_penalty"]["value"], baseline["cross_bloc_penalty"]["min"], baseline["cross_bloc_penalty"]["max"])
    # GDELT tone: more negative = more conflict coverage = more tension, so
    # invert after normalising (norm maps low-tone/high-conflict toward 0).
    n_gdelt = None
    if gdelt is not None:
        n_gdelt_raw = norm(gdelt["value"], baseline["gdelt_conflict_tone_30d"]["min"], baseline["gdelt_conflict_tone_30d"]["max"])
        n_gdelt = 1 - n_gdelt_raw if n_gdelt_raw is not None else None

    tension_terms = [
        ("gpr_monthly", 0.55, n_gpr_monthly),
        ("gdelt", 0.25, n_gdelt),
        ("cross_bloc_penalty", 0.20, n_penalty),
    ]
    tension, tension_components = composite_from_terms(tension_terms)

    prev_trust = prev["composites"]["trust"]["value"] if prev else None
    prev_tension = prev["composites"]["tension"]["value"] if prev else None
    prev_trust_components = prev["composites"]["trust"].get("components", []) if prev else []
    prev_tension_components = prev["composites"]["tension"].get("components", []) if prev else []
    trust_components = with_delta_contrib(trust_components, prev_trust_components)
    tension_components = with_delta_contrib(tension_components, prev_tension_components)

    # ---- Observer Station: the view from the nonaligned South ----
    ed_observer = editorial.get("observer", {})
    observer = {}
    if fx is not None:
        observer["usdzar"] = fx
    if export_split is not None:
        observer["export_split"] = export_split
    sarb_block = dict(sarb_live) if sarb_live is not None else {}
    reserves = ed_observer.get("reserves_usd_bn")
    if reserves is not None:
        sarb_block["reserves_usd_bn"] = reserves["value"]
        sarb_block["reserves_as_of"] = reserves["as_of"]
        sarb_block["reserves_source"] = reserves["source"]
        sarb_block["reserves_editorial"] = True
    if sarb_block:
        observer["sarb"] = sarb_block
    if ed_observer.get("status"):
        observer["status"] = ed_observer["status"]

    ledger = {
        "sealed_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "schema_version": 1,
        "composites": {
            "trust": {"value": trust, "delta": None if prev_trust is None else round(trust - prev_trust, 1),
                      "components": trust_components},
            "tension": {"value": tension, "state": tension_state(tension),
                        "delta": None if prev_tension is None else round(tension - prev_tension, 1),
                        "note": "weights: 0.55 GPR + 0.25 GDELT + 0.20 cross-bloc penalty"
                                if gdelt is not None else
                                "GDELT unavailable this run; weights renormalised over GPR + cross-bloc penalty",
                        "components": tension_components},
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
            **({"gdelt_conflict_tone_30d": gdelt} if gdelt is not None else {}),
        },
        "blocs": editorial["blocs"],
        "dispatch": editorial["dispatch"],
        "scenarios": editorial["scenarios"],
        "observer": observer,
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
