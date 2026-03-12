import os
import time
import json
import requests
import schedule
import sqlite3
import numpy as np
import math
from datetime import datetime, timedelta, timezone
from math import exp, lgamma, log

# ==========================================
# V5.13 EUROPEAN QUANT FUND
# ==========================================
# CAMBIOS VS V5.12:
#   1. ARQUITECTURA: xG via fetch_team_xg por fecha (portado de V6.5)
#      - Elimina dependencia de /predictions (promedios de temporada completa)
#      - El único método que funciona en Free tier sin bloqueo
#      - Cache compartida _DATE_FIXTURES_CACHE: 1 req por fecha sirve
#        para todos los equipos de todas las ligas ese día
#   2. ARQUITECTURA: sync_request_counter al arrancar (portado de V6.5)
#      - Sincroniza contador interno con la API real
#      - Evita desfases entre deploys repetidos
#   3. ARQUITECTURA: xg_result_log + ingest_results_into_xg_cache (V6.5)
#      - Acumula historial FT real usando fechas ya en memoria (0 req extra)
#      - Elimina necesidad de llamar /predictions por partido
#   4. ARQUITECTURA: capture_midday_lines (V6.5)
#      - Snapshot intermedio D-1 → mediodía → cierre
#      - Más datos de movimiento de línea para análisis futuro
#   5. ARQUITECTURA: weekly_xg_cache (V6.5 adaptado)
#      - Pre-caché last10 todos los equipos los lunes
#      - Budget máximo 44 req — nunca superar
#   6. ARQUITECTURA: startup diagnostics (V6.5)
#      - Verifica API, plan, requests disponibles y acceso a ligas al arrancar
#      - Pre-llena _DATE_FIXTURES_CACHE con fechas futuras (reutilizadas por scan)
#   7. LIGA: Championship eliminado (ID 40)
#      - Los ~12 req liberados van al warmup de xG cache
#      - Brasileirao y Liga MX cubiertos por V6.5 en servicio separado
#   8. CONSERVADO: LIQUIDITY_TIERS por liga (V5.12)
#   9. CONSERVADO: league_advanced_factors + goal_std por liga (V5.12)
#      - xG negbinom calibrado por liga (Bundesliga más abierta que Serie A)
#  10. CONSERVADO: portfolio engine con LCP por liga (V5.12)
#      - El LCP europeo divide por liga, no solo por total (mejor diversificación)
#  11. CONSERVADO: todas las correcciones V5.10→V5.12
#      - Kill-switch granular, anti-duplicado fixture, sanity 0.18, etc.
#  12. FIX: clear_date_cache en run_daily_scan solo borra fechas pasadas
#      - Preserva D+0/D+1/D+2 pre-cargadas por _startup_diagnostics
#      - Ahorra 2-3 req por scan (crítico en Free tier 100/día)

LIVE_TRADING = False

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY   = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

# Diagnóstico de DB al arrancar
print(f"  📂 DB_DIR={DB_DIR} | DB_PATH={DB_PATH}")
print(f"  📂 DB existe: {os.path.exists(DB_PATH)} | Tamaño: {os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0} bytes")
try:
    _chk = sqlite3.connect(DB_PATH)
    _xg  = _chk.execute("SELECT COUNT(*) FROM team_xg_cache").fetchone()[0] \
           if _chk.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='team_xg_cache'").fetchone() \
           else "tabla no existe"
    _pk  = _chk.execute("SELECT COUNT(*) FROM picks_log").fetchone()[0] \
           if _chk.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='picks_log'").fetchone() \
           else "tabla no existe"
    _chk.close()
    print(f"  📂 team_xg_cache={_xg} registros | picks_log={_pk} registros")
except Exception as _e:
    print(f"  📂 DB check error: {_e}")

# ── Horarios ────────────────────────────────────────────────────────────────
RUN_TIME_SCAN       = "09:00"   # D-1: cuotas líquidas, ~30h antes del KO
RUN_TIME_MIDDAY_CLV = "13:00"   # snapshot intermedio (antes del KO europeo ~15:00 UK)
RUN_TIME_XG_CACHE   = "08:00"   # lunes: pre-caché xG todos los equipos
RUN_TIME_INGEST     = "04:00"   # actualización league_advanced_factors

# ── Ligas objetivo — Championship (40) eliminado en V5.13 ───────────────────
TARGET_LEAGUES = {
    39:  "🇬🇧 PREMIER",
    140: "🇪🇸 LA LIGA",
    135: "🇮🇹 SERIE A",
    78:  "🇩🇪 BUNDESLIGA",
    61:  "🇫🇷 LIGUE 1",
    2:   "🏆 CHAMPIONS",
    3:   "🏆 EUROPA",
    88:  "🇳🇱 EREDIVISIE",
    94:  "🇵🇹 PRIMEIRA",
}

LIQUIDITY_TIERS = {
    "🇬🇧 PREMIER":    1.00,
    "🏆 CHAMPIONS":   1.00,
    "🇪🇸 LA LIGA":    1.00,
    "🇮🇹 SERIE A":    1.00,
    "🇩🇪 BUNDESLIGA": 1.00,
    "🏆 EUROPA":      0.85,
    "🇫🇷 LIGUE 1":    0.85,
    "🇳🇱 EREDIVISIE": 0.75,
    "🇵🇹 PRIMEIRA":   0.75,
}

XG_STD_BY_LEAGUE = {
    "🇬🇧 PREMIER":    1.48,
    "🇪🇸 LA LIGA":    1.38,
    "🇮🇹 SERIE A":    1.35,
    "🇩🇪 BUNDESLIGA": 1.55,
    "🇫🇷 LIGUE 1":    1.40,
    "🏆 CHAMPIONS":   1.45,
    "🏆 EUROPA":      1.42,
    "🇳🇱 EREDIVISIE": 1.58,
    "🇵🇹 PRIMEIRA":   1.38,
}

PACE_BY_LEAGUE = {
    "🇬🇧 PREMIER":    1.10,
    "🇪🇸 LA LIGA":    0.99,
    "🇮🇹 SERIE A":    1.01,
    "🇩🇪 BUNDESLIGA": 1.13,
    "🇫🇷 LIGUE 1":    1.00,
    "🏆 CHAMPIONS":   1.06,
    "🏆 EUROPA":      1.04,
    "🇳🇱 EREDIVISIE": 1.17,
    "🇵🇹 PRIMEIRA":   1.02,
}

MAX_DAILY_HEAT          = 0.10
TARGET_DAILY_VOLATILITY = 0.05
MIN_EV_THRESHOLD        = 0.015
MAX_EV_THRESHOLD        = 0.15
MAX_PICKS_PER_FIXTURE   = 1
XG_DECAY_FACTOR         = 0.85
XG_CACHE_TTL_HOURS      = 20
MAX_FIXTURES_PER_SCAN   = 40
MAX_DAYS_BACK_XG        = 90

VOLATILITY_BUCKETS = {"OVER": 0.85, "UNDER": 0.85, "BTTS": 0.90, "1X2": 1.25}

_DATE_FIXTURES_CACHE: dict = {}


# ==========================================
# DATABASE
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, league TEXT,
        home_team TEXT, away_team TEXT,
        market TEXT, selection TEXT, selection_key TEXT,
        odd_open REAL, prob_model REAL, ev_open REAL, stake_pct REAL,
        xg_home REAL, xg_away REAL, xg_total REAL,
        pick_time DATETIME, kickoff_time DATETIME,
        clv_captured INTEGER DEFAULT 0,
        urs REAL DEFAULT 0.0,
        model_gap REAL DEFAULT 0.0,
        xg_source TEXT DEFAULT 'predictions'
    )""")
    for col, defn in [
        ("urs",        "REAL DEFAULT 0.0"),
        ("model_gap",  "REAL DEFAULT 0.0"),
        ("xg_source",  "TEXT DEFAULT 'predictions'"),
    ]:
        try:
            c.execute(f"ALTER TABLE picks_log ADD COLUMN {col} {defn}")
        except:
            pass

    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, market TEXT, selection_key TEXT,
        odd_close REAL, implied_prob_close REAL, capture_time DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS decision_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, match TEXT, market TEXT,
        odd REAL, ev REAL, reason TEXT, timestamp DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS request_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, count INTEGER
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS team_xg_cache (
        team_id INTEGER PRIMARY KEY,
        team_name TEXT,
        gf_series TEXT,
        ga_series TEXT,
        xg_for REAL,
        xg_against REAL,
        confidence TEXT,
        depth INTEGER DEFAULT 6,
        updated_at DATETIME
    )""")
    try:
        c.execute("ALTER TABLE team_xg_cache ADD COLUMN depth INTEGER DEFAULT 6")
    except:
        pass

    c.execute("""CREATE TABLE IF NOT EXISTS line_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER,
        home_team TEXT, away_team TEXT, kickoff_time TEXT,
        market TEXT, selection TEXT,
        odd_snapshot REAL, odd_open REAL,
        captured_at DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS xg_result_log (
        fixture_id INTEGER,
        team_id    INTEGER,
        ingested_at TEXT,
        PRIMARY KEY (fixture_id, team_id)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS league_advanced_factors (
        league TEXT PRIMARY KEY,
        shots_avg REAL, shots_on_target_avg REAL,
        goals_per_shot REAL, goals_per_sot REAL,
        goal_std REAL, matches INTEGER,
        window_days INTEGER, last_updated DATETIME
    )""")

    c.execute("SELECT COUNT(*) FROM league_advanced_factors")
    if c.fetchone()[0] == 0:
        baselines = [
            ("🇬🇧 PREMIER",    26.5, 9.2, 0.110, 0.32, 1.48),
            ("🇪🇸 LA LIGA",    23.8, 8.1, 0.098, 0.29, 1.38),
            ("🇮🇹 SERIE A",    24.2, 8.3, 0.102, 0.30, 1.35),
            ("🇩🇪 BUNDESLIGA", 27.1, 9.5, 0.115, 0.33, 1.55),
            ("🇫🇷 LIGUE 1",    24.0, 8.4, 0.100, 0.30, 1.40),
            ("🏆 CHAMPIONS",   25.5, 9.0, 0.108, 0.31, 1.45),
            ("🏆 EUROPA",      25.0, 8.8, 0.105, 0.30, 1.42),
            ("🇳🇱 EREDIVISIE", 28.0,10.0, 0.118, 0.34, 1.58),
            ("🇵🇹 PRIMEIRA",   24.5, 8.5, 0.101, 0.30, 1.38),
        ]
        now = datetime.now(timezone.utc).isoformat()
        for row in baselines:
            c.execute(
                "INSERT INTO league_advanced_factors VALUES (?,?,?,?,?,?,?,?,?)",
                (row[0], row[1], row[2], row[3], row[4], row[5], 100, 30, now)
            )

    c.execute("SELECT id, selection_key FROM picks_log WHERE clv_captured = 1")
    for pid, skey in c.fetchall():
        if skey and skey.split("|")[-1].replace(".", "", 1).isdigit():
            c.execute("DELETE FROM closing_lines WHERE selection_key = ?", (skey,))
            c.execute("UPDATE picks_log SET clv_captured = -1 WHERE id = ?", (pid,))

    conn.commit()
    conn.close()


def log_rejection(fixture_id, match, market, odd, ev, reason):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute(
            "INSERT INTO decision_log VALUES (NULL,?,?,?,?,?,?,?)",
            (fixture_id, match, market, odd, ev, reason,
             datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
    except:
        pass


def track_requests(n=1):
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn  = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT count FROM request_log WHERE date=?", (today,))
        row = c.fetchone()
        if row:
            if n > 0:
                c.execute("UPDATE request_log SET count=count+? WHERE date=?", (n, today))
        else:
            c.execute("INSERT INTO request_log VALUES (NULL,?,?)", (today, max(n, 0)))
        conn.commit()
        c.execute("SELECT count FROM request_log WHERE date=?", (today,))
        total = c.fetchone()[0]
        conn.close()
        return total
    except:
        return 0


def sync_request_counter(headers):
    try:
        r    = requests.get("https://v3.football.api-sports.io/status",
                            headers=headers, timeout=10)
        raw  = r.json()
        resp = raw if isinstance(raw, dict) else {}
        data = resp.get("response", {})
        if isinstance(data, list):
            data = data[0] if data else {}
        current = data.get("requests", {}).get("current", None)
        if current is None:
            return
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        conn  = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("DELETE FROM request_log WHERE date=?", (today,))
        c.execute("INSERT INTO request_log VALUES (NULL,?,?)", (today, int(current)))
        conn.commit()
        conn.close()
        print(f"  📡 Contador sincronizado con API: {current}/100 requests hoy")
    except Exception as e:
        print(f"  ⚠️  sync_request_counter error: {e}")


# ==========================================
# URS ENGINE
# ==========================================

def get_avg_clv_by_market(market, lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if market:
            c.execute("""SELECT AVG((p.odd_open - c.odd_close)/p.odd_open)
                         FROM picks_log p JOIN closing_lines c
                           ON p.fixture_id=c.fixture_id AND p.market=c.market
                              AND p.selection_key=c.selection_key
                         WHERE p.market=? AND p.clv_captured=1
                         ORDER BY p.id DESC LIMIT ?""", (market, lookback))
        else:
            c.execute("""SELECT AVG((p.odd_open - c.odd_close)/p.odd_open)
                         FROM picks_log p JOIN closing_lines c
                           ON p.fixture_id=c.fixture_id AND p.market=c.market
                              AND p.selection_key=c.selection_key
                         WHERE p.clv_captured=1
                         ORDER BY p.id DESC LIMIT ?""", (lookback,))
        res = c.fetchone()[0]
        conn.close()
        return float(res) if res else 0.0
    except:
        return 0.0


def get_clv_sharpe():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""SELECT (p.odd_open - c.odd_close)/p.odd_open
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.clv_captured=1
                     ORDER BY p.id DESC LIMIT 50""")
        clvs = [row[0] for row in c.fetchall()]
        conn.close()
        if len(clvs) < 10:
            return 0.0
        mean_clv, std_clv = np.mean(clvs), np.std(clvs, ddof=1)
        return mean_clv / std_clv if std_clv != 0 else 0.0
    except:
        return 0.0


def score_sharpe(s):
    if s < -0.5:   return 0.10
    elif s < 0.0:  return 0.30
    elif s < 0.5:  return 0.50
    elif s < 1.0:  return 0.75
    elif s < 1.5:  return 0.90
    else:          return 1.00


def score_ev_gcs(ev):
    if ev < 0.03:   return 0.20
    elif ev < 0.05: return 0.40
    elif ev < 0.08: return 0.60
    elif ev < 0.12: return 0.80
    else:           return 1.00


def score_odd(odd):
    if odd < 1.20:    return 0.10
    elif odd < 1.40:  return 0.50
    elif odd <= 3.00: return 1.00
    elif odd <= 4.00: return 0.70
    else:             return 0.30


def calculate_urs(ev, odd, league_name):
    sharpe = get_clv_sharpe()
    liq    = LIQUIDITY_TIERS.get(league_name, 0.70)
    w      = {"sharpe": 0.35, "ev": 0.30, "liquidity": 0.20, "odd": 0.15}
    urs    = (w["sharpe"]    * score_sharpe(sharpe) +
              w["ev"]        * score_ev_gcs(ev) +
              w["liquidity"] * liq +
              w["odd"]       * score_odd(odd))
    return max(0.10, min(urs, 1.00))


def get_kelly_and_urs(ev, odd, market, league_name):
    avg_clv_market = get_avg_clv_by_market(market)
    avg_clv_global = get_avg_clv_by_market(None)
    if avg_clv_market < -0.015:
        return 0.0, 0.0, f"KILL_SWITCH_{market}"
    if avg_clv_global < -0.025:
        return 0.0, 0.0, "KILL_SWITCH_GLOBAL"
    base_kelly = max(0.0, min(ev / (odd - 1), 0.05))
    if -0.015 <= avg_clv_market < 0.005:
        base_kelly *= 0.25
    urs = calculate_urs(ev, odd, league_name)
    return base_kelly * urs, urs, None


# ==========================================
# PORTFOLIO ENGINE
# ==========================================

def apply_portfolio_risk_engine(preliminary_picks):
    if not preliminary_picks:
        return [], {}

    league_counts = {}
    for p in preliminary_picks:
        league_counts[p["l_name"]] = league_counts.get(p["l_name"], 0) + 1

    port_var = 0.0
    for p in preliminary_picks:
        if p["odd"] <= 1.01:
            p["adj_stake"] = 0
            p["lcp_applied"] = 0
            continue
        lcp       = 1.0 / math.sqrt(league_counts[p["l_name"]])
        adj_stake = p["base_stake"] * lcp
        beta      = VOLATILITY_BUCKETS.get(p["mkt"], 1.00)
        var_i     = beta * p["prob"] * (1 - p["prob"]) * (p["odd"] ** 2)
        port_var += (adj_stake ** 2) * var_i
        p["adj_stake"]   = adj_stake
        p["lcp_applied"] = lcp

    port_vol = math.sqrt(port_var) if port_var > 0 else 0.0001
    damper   = min(1.0, TARGET_DAILY_VOLATILITY / port_vol)
    total    = 0.0
    for p in preliminary_picks:
        p["final_stake"] = p.get("adj_stake", 0) * damper
        total += p["final_stake"]

    scale = min(1.0, MAX_DAILY_HEAT / total) if total > 0 else 1.0
    for p in preliminary_picks:
        p["final_stake"] = max(0.0, min(p["final_stake"] * scale, 0.05))

    preliminary_picks = [p for p in preliminary_picks if p["final_stake"] >= 0.005]

    meta = {
        "port_vol":   port_vol,
        "damper":     damper,
        "final_heat": sum(p["final_stake"] for p in preliminary_picks),
    }
    return preliminary_picks, meta


# ==========================================
# MATH ENGINE
# ==========================================

def _poisson_pmf(mu, k):
    if mu <= 0 or k < 0:
        return 0.0
    try:
        return exp(-mu + k * log(mu) - lgamma(k + 1))
    except:
        return 0.0


def _weighted_avg(values, decay=XG_DECAY_FACTOR):
    if not values:
        return 0.0
    w = [decay ** i for i in range(len(values))]
    return sum(v * wi for v, wi in zip(values, w)) / sum(w)


def _form_factor(gf_series):
    if len(gf_series) < 6:
        return 1.0
    recent   = _weighted_avg(gf_series[:3])
    previous = _weighted_avg(gf_series[3:6])
    if previous < 0.1:
        return 1.0
    return max(0.85, min(recent / previous, 1.15))


def negbin_pmf(mu, var, k):
    if mu <= 0:
        return 0.0
    if var <= mu * 1.01:
        return _poisson_pmf(mu, k)
    r = mu ** 2 / (var - mu)
    p = r / (r + mu)
    try:
        return exp(lgamma(k+r) - lgamma(r) - lgamma(k+1) + r*log(p) + k*log(1-p))
    except:
        return 0.0


def calc_over_under(xg_total, line=2.5, league_name=None):
    std     = XG_STD_BY_LEAGUE.get(league_name, 1.45) if league_name else 1.45
    var     = max(std ** 2, xg_total)
    p_under = sum(negbin_pmf(xg_total, var, k) for k in range(int(np.floor(line)) + 1))
    return 1 - p_under, p_under


def bivariate_poisson_1x2(xg_home, xg_away, max_goals=10):
    if not (0.4 <= xg_home <= 4.0) or not (0.4 <= xg_away <= 4.0):
        return None
    p_home = p_draw = p_away = 0.0
    for i in range(max_goals + 1):
        for j in range(max_goals + 1):
            prob = _poisson_pmf(xg_home, i) * _poisson_pmf(xg_away, j)
            if i > j:    p_home += prob
            elif i == j: p_draw += prob
            else:        p_away += prob
    total = p_home + p_draw + p_away
    if total < 0.95:
        return None
    p_h, p_d, p_a = p_home/total, p_draw/total, p_away/total
    if not (0.08 <= p_d <= 0.55):
        return None
    return p_h, p_d, p_a


def calc_btts(xg_home, xg_away):
    if not (0.4 <= xg_home <= 4.0) or not (0.4 <= xg_away <= 4.0):
        return None, None
    p_yes = (1 - exp(-xg_home)) * (1 - exp(-xg_away))
    p_no  = 1 - p_yes
    if not (0.20 <= p_yes <= 0.90):
        return None, None
    return round(p_yes, 4), round(p_no, 4)


# ==========================================
# XG ENGINE
# ==========================================

def _get_fixtures_for_date(d, headers):
    if d not in _DATE_FIXTURES_CACHE:
        try:
            r = requests.get(
                "https://v3.football.api-sports.io/fixtures",
                headers=headers,
                params={"date": d},
                timeout=10
            )
            _DATE_FIXTURES_CACHE[d] = r.json().get("response", [])
            time.sleep(0.3)
        except:
            _DATE_FIXTURES_CACHE[d] = []
    return _DATE_FIXTURES_CACHE[d]


def clear_date_cache():
    _DATE_FIXTURES_CACHE.clear()


def clear_past_dates_only():
    """
    FIX v5.13: solo borra fechas pasadas del cache.
    Preserva D+0/D+1/D+2 pre-cargadas por _startup_diagnostics,
    ahorrando 2-3 req por scan en Free tier.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    past  = [d for d in list(_DATE_FIXTURES_CACHE.keys()) if d < today]
    for d in past:
        _DATE_FIXTURES_CACHE.pop(d, None)
    if past:
        print(f"  🧹 Cache: {len(past)} fechas pasadas eliminadas, "
              f"{len(_DATE_FIXTURES_CACHE)} futuras preservadas")


def fetch_team_xg(team_id, headers, league_id=None, use_cache=True, depth=6):
    if use_cache:
        try:
            conn_c = sqlite3.connect(DB_PATH)
            cc = conn_c.cursor()
            cc.execute(
                "SELECT gf_series, ga_series, xg_for, xg_against, confidence, updated_at "
                "FROM team_xg_cache WHERE team_id=?", (team_id,)
            )
            row = conn_c.fetchone()
            conn_c.close()
            if row:
                updated = datetime.fromisoformat(row[5])
                age_h   = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
                if age_h < XG_CACHE_TTL_HOURS:
                    gf = json.loads(row[0])
                    ga = json.loads(row[1])
                    print(f"    xG [{team_id}] CACHE HIT age={age_h:.1f}h conf={row[4]}")
                    return float(row[2]), float(row[3]), row[4], gf, ga, True
        except:
            pass

    gf_series = []
    ga_series = []
    days_searched = 0

    def _extract_goals(fixtures, tid, strict_league):
        gf, ga = [], []
        for fix in fixtures:
            if strict_league and league_id and fix["league"]["id"] != league_id:
                continue
            if fix["fixture"]["status"]["short"] != "FT":
                continue
            h_id    = fix["teams"]["home"]["id"]
            a_id    = fix["teams"]["away"]["id"]
            h_goals = fix["goals"]["home"]
            a_goals = fix["goals"]["away"]
            if h_goals is None or a_goals is None:
                continue
            if h_id == tid:
                gf.append(h_goals); ga.append(a_goals)
            elif a_id == tid:
                gf.append(a_goals); ga.append(h_goals)
        return gf, ga

    for days_back in range(1, MAX_DAYS_BACK_XG + 1):
        if len(gf_series) >= depth:
            break
        d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        already_cached = d in _DATE_FIXTURES_CACHE
        all_day = _get_fixtures_for_date(d, headers)
        if not already_cached:
            days_searched += 1
        gf_day, ga_day = _extract_goals(all_day, team_id, strict_league=True)
        gf_series.extend(gf_day)
        ga_series.extend(ga_day)

    # Fallback: aceptar cualquier liga (Champions, Copa) para tener forma del equipo
    if len(gf_series) < 2:
        gf_any, ga_any = [], []
        for days_back in range(1, MAX_DAYS_BACK_XG + 1):
            if len(gf_any) >= depth:
                break
            d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
            all_day = _get_fixtures_for_date(d, headers)
            gf_d, ga_d = _extract_goals(all_day, team_id, strict_league=False)
            gf_any.extend(gf_d)
            ga_any.extend(ga_d)
        if len(gf_any) > len(gf_series):
            print(f"    xG [{team_id}] fallback multi-liga: {len(gf_any)} partidos")
            gf_series, ga_series = gf_any, ga_any

    if not gf_series:
        print(f"    xG [{team_id}] sin partidos en {MAX_DAYS_BACK_XG} días — DEFAULT 1.3/1.3 LOW")
        return 1.3, 1.3, "LOW", [], [], False

    xg_for     = _weighted_avg(gf_series)
    xg_against = _weighted_avg(ga_series)
    confidence = "HIGH" if len(gf_series) >= max(4, depth // 2) else "MED"
    print(f"    xG [{team_id}] {len(gf_series)} partidos en {days_searched} días "
          f"— xG={xg_for:.2f}/{xg_against:.2f} {confidence}")

    try:
        conn_c = sqlite3.connect(DB_PATH)
        cc = conn_c.cursor()
        cc.execute("""INSERT OR REPLACE INTO team_xg_cache
            (team_id, gf_series, ga_series, xg_for, xg_against, confidence, updated_at, depth)
            VALUES (?,?,?,?,?,?,?,?)""",
            (team_id, json.dumps(gf_series), json.dumps(ga_series),
             xg_for, xg_against, confidence,
             datetime.now(timezone.utc).isoformat(), depth))
        conn_c.commit()
        conn_c.close()
    except:
        pass

    return xg_for, xg_against, confidence, gf_series, ga_series, False


def build_xg_match(home_id, away_id, h_inj, a_inj, league_id, league_name, headers, depth=6):
    h_xgf, h_xga, h_conf, h_gf, h_ga, h_cached = fetch_team_xg(
        home_id, headers, league_id=league_id, depth=depth
    )
    if not h_cached:
        time.sleep(2.0)

    a_xgf, a_xga, a_conf, a_gf, a_ga, a_cached = fetch_team_xg(
        away_id, headers, league_id=league_id, depth=depth
    )

    xh = (h_xgf + a_xga) / 2
    xa = (a_xgf + h_xga) / 2

    xh *= _form_factor(h_gf)
    xa *= _form_factor(a_gf)

    xh *= (1 - min(h_inj * 0.015, 0.08))
    xa *= (1 - min(a_inj * 0.015, 0.08))

    pace = PACE_BY_LEAGUE.get(league_name, 1.0)
    xh *= pace
    xa *= pace

    xh = max(0.6, min(xh, 3.5))
    xa = max(0.6, min(xa, 3.5))

    conf = "HIGH" if (h_conf == "HIGH" and a_conf == "HIGH") else \
           "MED"  if (h_conf != "LOW"  and a_conf != "LOW")  else "LOW"

    xg_src = f"last{depth} (H:{len(h_gf)}pts, A:{len(a_gf)}pts)"
    return xh, xa, xh + xa, conf, xg_src


def ingest_results_into_xg_cache(headers):
    ingested = 0
    dates_to_check = []

    for d in sorted(_DATE_FIXTURES_CACHE.keys(), reverse=True)[:7]:
        dates_to_check.append((d, False))

    for days_back in range(1, 4):
        d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        if d not in _DATE_FIXTURES_CACHE:
            dates_to_check.append((d, True))

    try:
        conn = sqlite3.connect(DB_PATH)
        cc   = conn.cursor()
        for d, needs_fetch in dates_to_check:
            if needs_fetch:
                try:
                    r = requests.get(
                        "https://v3.football.api-sports.io/fixtures",
                        headers=headers, params={"date": d}, timeout=10
                    )
                    fixtures = r.json().get("response", [])
                    _DATE_FIXTURES_CACHE[d] = fixtures
                except:
                    continue
            else:
                fixtures = _DATE_FIXTURES_CACHE.get(d, [])

            for fix in fixtures:
                if fix["fixture"]["status"]["short"] != "FT":
                    continue
                if fix["league"]["id"] not in TARGET_LEAGUES:
                    continue
                h_id    = fix["teams"]["home"]["id"]
                a_id    = fix["teams"]["away"]["id"]
                h_goals = fix["goals"]["home"]
                a_goals = fix["goals"]["away"]
                if h_goals is None or a_goals is None:
                    continue
                fid = fix["fixture"]["id"]

                for team_id, gf, ga in [(h_id, h_goals, a_goals), (a_id, a_goals, h_goals)]:
                    cc.execute(
                        "SELECT 1 FROM xg_result_log WHERE fixture_id=? AND team_id=?",
                        (fid, team_id)
                    )
                    if cc.fetchone():
                        continue

                    cc.execute(
                        "SELECT gf_series, ga_series FROM team_xg_cache WHERE team_id=?",
                        (team_id,)
                    )
                    row = cc.fetchone()
                    gf_series = json.loads(row[0]) if row else []
                    ga_series = json.loads(row[1]) if row else []

                    gf_series = [gf] + gf_series[:9]
                    ga_series = [ga] + ga_series[:9]

                    xg_for     = _weighted_avg(gf_series)
                    xg_against = _weighted_avg(ga_series)
                    confidence = "HIGH" if len(gf_series) >= 4 else "MED"

                    cc.execute("""INSERT OR REPLACE INTO team_xg_cache
                        (team_id, gf_series, ga_series, xg_for, xg_against,
                         confidence, updated_at, depth)
                        VALUES (?,?,?,?,?,?,?,?)""",
                        (team_id, json.dumps(gf_series), json.dumps(ga_series),
                         xg_for, xg_against, confidence,
                         datetime.now(timezone.utc).isoformat(), len(gf_series)))
                    cc.execute(
                        "INSERT OR IGNORE INTO xg_result_log VALUES (?,?,?)",
                        (fid, team_id, datetime.now(timezone.utc).isoformat())
                    )
                    ingested += 1

        conn.commit()
        conn.close()
        if ingested > 0:
            print(f"  📥 xG ingest: {ingested} resultados FT añadidos a cache")
    except Exception as e:
        print(f"  ⚠️  ingest_results error: {e}")


# ==========================================
# VALIDACIONES
# ==========================================

def validate_xg(xh, xa, bets):
    home_odd = away_odd = over_odd = under_odd = None
    for b in bets:
        if b["id"] == 1:
            for v in b["values"]:
                try:
                    if v["value"] == "Home": home_odd = float(v["odd"])
                    if v["value"] == "Away": away_odd = float(v["odd"])
                except:
                    pass
        elif b["id"] == 5:
            for v in b["values"]:
                try:
                    if v["value"] == "Over 2.5":  over_odd  = float(v["odd"])
                    if v["value"] == "Under 2.5": under_odd = float(v["odd"])
                except:
                    pass

    if home_odd and away_odd:
        min_odd  = min(home_odd, away_odd)
        xg_ratio = max(xh, xa) / min(xh, xa) if min(xh, xa) > 0 else 1.0
        if min_odd < 1.40 and xg_ratio < 1.50:
            return False, f"XG_DEFAULT_DETECTED (ratio={xg_ratio:.2f})"
        if min_odd < 1.65 and xg_ratio < 1.20:
            return False, f"XG_FLAT_ON_FAVOURITE (ratio={xg_ratio:.2f})"
        if 1.30 <= xh <= 1.50 and 1.30 <= xa <= 1.50 and min_odd < 1.60:
            return False, f"XG_LIKELY_DEFAULT (xh={xh:.2f}, xa={xa:.2f}, fav={min_odd:.2f})"

    if over_odd and under_odd:
        p_under_mkt = 1 / (under_odd * 1.07)
        if p_under_mkt > 0.01:
            xg_implied = -2.5 * math.log(p_under_mkt)
            gap        = abs((xh + xa) - xg_implied)
            if gap > 1.8:
                return False, f"XG_TOTAL_INCONSISTENT (model={xh+xa:.2f}, mkt={xg_implied:.2f})"

    return True, None


def sanity_check(p_true, mkt, odd):
    VIG = {"OVER": 1.07, "UNDER": 1.07, "1X2": 1.05, "BTTS": 1.06}
    gap = abs(p_true - 1 / (odd * VIG.get(mkt, 1.06)))
    if gap > 0.18:
        return False, f"XG_SANITY_FAIL (gap={gap:.2f}, p={p_true:.2f})"
    return True, None


# ==========================================
# PRICING ENGINE
# ==========================================

def build_market_probs(bets, xh, xa, h_n, a_n, conf, league_name):
    probs = []
    po, pu   = calc_over_under(xh + xa, league_name=league_name)
    p_by, pn = calc_btts(xh, xa) if conf != "LOW" else (None, None)
    poisson  = bivariate_poisson_1x2(xh, xa) if conf != "LOW" else None

    if poisson:
        p_h, p_d, p_a = poisson
        p_1x2 = {"Home": p_h, "Draw": p_d, "Away": p_a}
        names  = {"Home": f"Gana {h_n}", "Draw": "Empate", "Away": f"Gana {a_n}"}
    else:
        p_1x2 = {}

    for b in bets:
        if b["id"] == 1:
            for v in b["values"]:
                if v["value"] not in p_1x2:
                    continue
                odd       = float(v["odd"])
                p_true    = p_1x2[v["value"]]
                p_implied = 1 / (odd * 1.05)
                probs.append({
                    "mkt": "1X2", "pick": names[v["value"]],
                    "odd": odd, "prob": p_true,
                    "bid": b["id"], "val": v["value"],
                    "model_gap": round(p_true - p_implied, 4)
                })

        elif b["id"] == 5:
            for v in b["values"]:
                if v["value"] not in ("Over 2.5", "Under 2.5"):
                    continue
                is_over   = "Over" in v["value"]
                mkt_type  = "OVER" if is_over else "UNDER"
                p_true    = po if is_over else pu
                odd       = float(v["odd"])
                p_implied = 1 / (odd * 1.07)
                probs.append({
                    "mkt": mkt_type, "pick": f"{v['value']} Goles",
                    "odd": odd, "prob": p_true,
                    "bid": b["id"], "val": v["value"],
                    "model_gap": round(p_true - p_implied, 4)
                })

        elif b["id"] == 8 and p_by is not None:
            for v in b["values"]:
                if v["value"] not in ("Yes", "No"):
                    continue
                p_true    = p_by if v["value"] == "Yes" else pn
                odd       = float(v["odd"])
                p_implied = 1 / (odd * 1.06)
                probs.append({
                    "mkt": "BTTS", "pick": f"Ambos Marcan: {v['value']}",
                    "odd": odd, "prob": p_true,
                    "bid": b["id"], "val": v["value"],
                    "model_gap": round(p_true - p_implied, 4)
                })

    return probs


# ==========================================
# MAIN BOT
# ==========================================

class QuantFundEuropean:
    def __init__(self):
        init_db()
        self.headers = {"x-apisports-key": API_SPORTS_KEY}
        sync_request_counter(self.headers)
        api_ok, plan_info, req_info, access_ok, access_detail = self._startup_diagnostics()
        mode = "🔴 LIVE" if LIVE_TRADING else "🟡 DRY-RUN"
        self.send_msg(
            f"🌍 <b>EUROPEAN QUANT FUND V5.13</b>\n"
            f"Estado: {mode}\n\n"
            f"{'✅' if api_ok else '❌'} API: {plan_info}\n"
            f"📡 Requests hoy: {req_info}\n"
            f"{'✅' if access_ok else '❌'} Ligas: {access_detail}"
        )

    def _startup_diagnostics(self):
        try:
            r        = requests.get("https://v3.football.api-sports.io/status",
                                    headers=self.headers, timeout=10)
            track_requests(1)
            raw      = r.json()
            resp     = raw if isinstance(raw, dict) else {}
            data     = resp.get("response", {})
            if isinstance(data, list):
                data = data[0] if data else {}
            sub     = data.get("subscription", {})
            reqs    = data.get("requests", {})
            plan    = sub.get("plan", "Unknown")
            active  = sub.get("active", False)
            current = reqs.get("current", "?")
            limit   = reqs.get("limit_day", "?")
            plan_info = f"{plan} ({'activo' if active else '⚠️ INACTIVO'})"
            req_info  = f"{current}/{limit}"
            if not active:
                return False, plan_info, req_info, False, "suscripción inactiva"
        except Exception as e:
            return False, "error de conexión", "?/?", False, str(e)

        try:
            league_found = set()
            for d_off in range(5):
                d = (datetime.now() + timedelta(days=d_off)).strftime("%Y-%m-%d")
                r = requests.get("https://v3.football.api-sports.io/fixtures",
                                 headers=self.headers, params={"date": d}, timeout=10)
                track_requests(1)
                fixtures = r.json().get("response", [])
                # FIX v5.13: poblar cache — run_daily_scan reutiliza sin req extra
                _DATE_FIXTURES_CACHE[d] = fixtures
                for fix in fixtures:
                    lid = fix["league"]["id"]
                    if lid in TARGET_LEAGUES:
                        league_found.add(lid)
                if len(league_found) >= 4:
                    break
                time.sleep(0.5)

            names_found = [TARGET_LEAGUES[lid].split()[-1] for lid in league_found]
            detail = (f"{len(league_found)}/9 ligas con fixtures próximos "
                      f"({', '.join(names_found[:4])}{'...' if len(names_found)>4 else ''})")
            return True, plan_info, req_info, len(league_found) > 0, detail
        except Exception as e:
            return True, plan_info, req_info, False, str(e)

    def send_msg(self, text):
        if not TELEGRAM_TOKEN:
            print("⚠️  TELEGRAM_TOKEN vacío")
            return
        try:
            r = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10
            )
            if not r.ok:
                print(f"⚠️  Telegram {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"⚠️  Telegram error: {e}")

    def _fetch_and_store_odds(self, c, fid, mkt, skey, now, mark_captured=True):
        res = requests.get(
            f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8",
            headers=self.headers, timeout=10
        ).json()
        track_requests(1)
        found = False
        if res.get("response"):
            for b in res["response"][0]["bookmakers"][0]["bets"]:
                for v in b["values"]:
                    if f"{b['id']}|{v['value']}" != skey:
                        continue
                    odd_val = float(v["odd"])
                    c.execute(
                        "SELECT COUNT(*) FROM closing_lines "
                        "WHERE fixture_id=? AND market=? AND selection_key=?",
                        (fid, mkt, skey)
                    )
                    exists = c.fetchone()[0] > 0
                    if mark_captured:
                        if exists:
                            c.execute(
                                "UPDATE closing_lines SET odd_close=?, implied_prob_close=?, "
                                "capture_time=? WHERE fixture_id=? AND market=? AND selection_key=?",
                                (odd_val, 1/odd_val, now.isoformat(), fid, mkt, skey)
                            )
                        else:
                            c.execute(
                                "INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?)",
                                (fid, mkt, skey, odd_val, 1/odd_val, now.isoformat())
                            )
                    else:
                        if not exists:
                            c.execute(
                                "INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?)",
                                (fid, mkt, skey, odd_val, 1/odd_val, now.isoformat())
                            )
                    found = True
                    break
        return found

    def capture_midday_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            c    = conn.cursor()
            now  = datetime.now(timezone.utc)
            c.execute(
                "SELECT id, fixture_id, market, selection_key, kickoff_time "
                "FROM picks_log WHERE clv_captured = 0"
            )
            for pid, fid, mkt, skey, ko in c.fetchall():
                mins = (datetime.fromisoformat(ko) - now).total_seconds() / 60.0
                if 120.0 <= mins <= 360.0:
                    self._fetch_and_store_odds(c, fid, mkt, skey, now, mark_captured=False)
                    time.sleep(2.0)
            conn.commit()
            conn.close()
        except:
            pass

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            c    = conn.cursor()
            now  = datetime.now(timezone.utc)
            c.execute(
                "SELECT id, fixture_id, market, selection_key, kickoff_time "
                "FROM picks_log WHERE clv_captured = 0"
            )
            for pid, fid, mkt, skey, ko in c.fetchall():
                mins = (datetime.fromisoformat(ko) - now).total_seconds() / 60.0
                if mins <= 60.0:
                    found = self._fetch_and_store_odds(
                        c, fid, mkt, skey, now, mark_captured=True
                    )
                    time.sleep(2.0)
                    c.execute(
                        "UPDATE picks_log SET clv_captured=? WHERE id=?",
                        (1 if found else -1, pid)
                    )
            conn.commit()
            conn.close()
        except:
            pass

    def weekly_xg_cache(self):
        clear_date_cache()
        BUDGET_MAX  = 44
        req_inicio  = track_requests(0)

        def reqs_gastados():
            return track_requests(0) - req_inicio

        try:
            teams_by_league = {lid: {} for lid in TARGET_LEAGUES}
            ligas_completas = set()

            for days_back in range(1, 16):
                if reqs_gastados() >= BUDGET_MAX:
                    break
                if len(ligas_completas) == len(TARGET_LEAGUES):
                    break
                d = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
                try:
                    r = requests.get(
                        "https://v3.football.api-sports.io/fixtures",
                        headers=self.headers, params={"date": d}, timeout=10
                    )
                    track_requests(1)
                    for fix in r.json().get("response", []):
                        lid = fix["league"]["id"]
                        if lid not in TARGET_LEAGUES:
                            continue
                        if fix["fixture"]["status"]["short"] != "FT":
                            continue
                        teams_by_league[lid][fix["teams"]["home"]["id"]] = fix["teams"]["home"]["name"]
                        teams_by_league[lid][fix["teams"]["away"]["id"]] = fix["teams"]["away"]["name"]
                        if len(teams_by_league[lid]) >= 18:
                            ligas_completas.add(lid)
                    time.sleep(0.5)
                except:
                    pass

            for lid, lname in TARGET_LEAGUES.items():
                n = len(teams_by_league[lid])
                sample = list(teams_by_league[lid].values())[:6]
                print(f"  {lname}: {n} equipos → {', '.join(sample)}{'...' if n>6 else ''}")

            total_cached = total_skipped = 0
            for lid, lname in TARGET_LEAGUES.items():
                for team_id, team_name in teams_by_league[lid].items():
                    if reqs_gastados() >= BUDGET_MAX:
                        print("  ⚠️  Budget máximo alcanzado — parando cache")
                        break
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cc   = conn.cursor()
                        cc.execute(
                            "SELECT updated_at, depth FROM team_xg_cache WHERE team_id=?",
                            (team_id,)
                        )
                        row = cc.fetchone()
                        conn.close()
                        if row:
                            age = (datetime.now(timezone.utc) -
                                   datetime.fromisoformat(row[0])).total_seconds() / 3600
                            if age < XG_CACHE_TTL_HOURS and (row[1] or 0) >= 10:
                                total_skipped += 1
                                continue
                    except:
                        pass

                    fetch_team_xg(team_id, self.headers, league_id=lid, use_cache=False, depth=10)
                    total_cached += 1
                    time.sleep(1.5)

                    try:
                        conn = sqlite3.connect(DB_PATH)
                        cc   = conn.cursor()
                        cc.execute(
                            "UPDATE team_xg_cache SET team_name=?, depth=10 WHERE team_id=?",
                            (team_name, team_id)
                        )
                        conn.commit()
                        conn.close()
                    except:
                        pass

            req_total = reqs_gastados()
            self.send_msg(
                f"🔄 <b>xG Cache V5.13 actualizada</b>\n"
                f"Equipos cacheados: {total_cached} | Saltados: {total_skipped}\n"
                f"📡 Requests warmup: {req_total}/{BUDGET_MAX} máx"
            )
        except Exception as e:
            self.send_msg(f"⚠️ weekly_xg_cache error: {e}")

    def update_league_advanced_factors(self):
        today   = datetime.now(timezone.utc)
        weekday = today.weekday()
        schedule_ligas = {
            0: [39, 94], 1: [140, 88], 2: [135],
            3: [78],     4: [61],      5: [2],  6: [3]
        }
        if weekday not in schedule_ligas:
            return
        for league_id in schedule_ligas[weekday]:
            league_name = TARGET_LEAGUES.get(league_id)
            if not league_name:
                continue
            season = today.year if today.month >= 8 else today.year - 1
            try:
                teams = requests.get(
                    "https://v3.football.api-sports.io/teams",
                    headers=self.headers,
                    params={"league": league_id, "season": season},
                    timeout=15
                ).json().get("response", [])
                track_requests(1)
            except:
                continue

            t_shots = t_sot = t_goals = 0
            match_counts = []
            for t in teams:
                time.sleep(1.1)
                try:
                    stats = requests.get(
                        "https://v3.football.api-sports.io/teams/statistics",
                        headers=self.headers,
                        params={"league": league_id, "season": season,
                                "team": t["team"]["id"]},
                        timeout=15
                    ).json().get("response")
                    track_requests(1)
                    sh  = stats["shots"].get("total", 0)
                    sot = stats["shots"].get("on", 0)
                    gls = stats["goals"]["for"]["total"].get("total", 0)
                    m   = stats["fixtures"]["played"].get("total", 0)
                    if sh and gls and m > 0:
                        t_shots += sh
                        t_sot   += (sot or 0)
                        t_goals += gls
                        match_counts.append(m)
                except:
                    continue

            m_total = sum(match_counts) / 2
            if m_total > 0:
                sh_avg    = t_shots / m_total
                sot_avg   = t_sot   / m_total
                gps       = t_goals / t_shots if t_shots else 0
                gsot      = t_goals / max(t_sot, 1)
                std_proxy = np.sqrt(gps * sh_avg)
                conn = sqlite3.connect(DB_PATH)
                c    = conn.cursor()
                c.execute("""INSERT INTO league_advanced_factors
                    (league, shots_avg, shots_on_target_avg, goals_per_shot,
                     goals_per_sot, goal_std, matches, window_days, last_updated)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(league) DO UPDATE SET
                        shots_avg=excluded.shots_avg,
                        shots_on_target_avg=excluded.shots_on_target_avg,
                        goal_std=excluded.goal_std,
                        last_updated=excluded.last_updated""",
                    (league_name, round(sh_avg, 2), round(sot_avg, 2),
                     round(gps, 4), round(gsot, 4), round(std_proxy, 3),
                     int(m_total), 30, today.isoformat()))
                conn.commit()
                conn.close()

    def run_daily_scan(self):
        now_utc = datetime.now(timezone.utc)

        # FIX v5.13: solo borrar fechas pasadas — preservar D+0/D+1/D+2 del diagnóstico
        clear_past_dates_only()

        matches_raw = []
        for days_ahead in [0, 1, 2]:
            d = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
            # Si ya está en cache (del diagnóstico), no gasta req extra
            already = d in _DATE_FIXTURES_CACHE
            fixtures_day = _get_fixtures_for_date(d, self.headers)
            if not already:
                track_requests(1)
            matches_raw.extend([
                f for f in fixtures_day
                if f["league"]["id"] in TARGET_LEAGUES
            ])

        def hours_away(m):
            try:
                ko = datetime.fromisoformat(m["fixture"]["date"].replace("Z", "+00:00"))
                return (ko - now_utc).total_seconds() / 3600
            except:
                return 999

        matches = [m for m in matches_raw if hours_away(m) <= 48]
        matches.sort(key=hours_away)
        matches = matches[:MAX_FIXTURES_PER_SCAN]

        if not matches:
            self.send_msg(
                "🔇 <b>European V5.13:</b> Sin partidos en los próximos 48h.\n"
                f"📡 Requests: {track_requests(0)}/100"
            )
            ingest_results_into_xg_cache(self.headers)
            return

        liga_counts = {}
        for m in matches:
            ln = TARGET_LEAGUES[m["league"]["id"]]
            liga_counts[ln] = liga_counts.get(ln, 0) + 1

        req_est = len(matches) * 2  # odds + injuries (xG usa cache en su mayoría)
        self.send_msg(
            f"🔍 <b>European V5.13 — Scan D-1</b>\n"
            + "\n".join(f"  {ln}: {n}" for ln, n in sorted(liga_counts.items()))
            + f"\n📡 Requests estimados: ~{req_est}/100"
        )

        already_picked_today = set()
        try:
            conn_check = sqlite3.connect(DB_PATH)
            cc         = conn_check.cursor()
            today_str  = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            cc.execute(
                "SELECT fixture_id FROM picks_log WHERE pick_time >= ?", (today_str,)
            )
            already_picked_today = {row[0] for row in cc.fetchall()}
            conn_check.close()
        except:
            pass

        preliminary_picks = []

        for m in matches:
            fid    = m["fixture"]["id"]
            h_n    = m["teams"]["home"]["name"]
            a_n    = m["teams"]["away"]["name"]
            h_id   = m["teams"]["home"]["id"]
            a_id   = m["teams"]["away"]["id"]
            ko     = m["fixture"]["date"]
            lid    = m["league"]["id"]
            l_name = TARGET_LEAGUES[lid]
            label  = f"{h_n} vs {a_n} ({l_name})"
            print(f"\n  ── {label} (fid={fid}) ──")

            if fid in already_picked_today:
                print(f"     ⏭️  Ya procesado hoy — skip")
                continue

            time.sleep(3.0)

            try:
                odds_res = requests.get(
                    "https://v3.football.api-sports.io/odds",
                    headers=self.headers,
                    params={"fixture": fid, "bookmaker": 8}, timeout=10
                ).json().get("response", [])
                track_requests(1)
            except:
                continue
            if not odds_res:
                continue
            bets = odds_res[0]["bookmakers"][0]["bets"]

            try:
                inj_res = requests.get(
                    "https://v3.football.api-sports.io/injuries",
                    headers=self.headers,
                    params={"fixture": fid}, timeout=10
                ).json().get("response", [])
                track_requests(1)
                hinj = sum(1 for i in inj_res if i["team"]["id"] == h_id)
                ainj = sum(1 for i in inj_res if i["team"]["id"] == a_id)
                print(f"     Lesionados: {h_n}={hinj} {a_n}={ainj}")
            except:
                hinj = ainj = 0

            xh, xa, xt, conf, xg_src = build_xg_match(
                h_id, a_id, hinj, ainj, lid, l_name, self.headers, depth=6
            )
            print(f"     xG: {h_n}={xh:.2f} {a_n}={xa:.2f} total={xt:.2f} "
                  f"conf={conf} src={xg_src} req={track_requests(0)}/100")

            ok, reason = validate_xg(xh, xa, bets)
            if not ok:
                log_rejection(fid, label, "ALL", 0.0, 0.0, reason)
                print(f"     ❌ {reason}")
                continue

            if conf == "LOW":
                log_rejection(fid, label, "ALL", 0.0, 0.0, "XG_LOW_SKIP")
                print(f"     ❌ xG LOW — skip")
                continue

            probs = build_market_probs(bets, xh, xa, h_n, a_n, conf, l_name)

            candidates = []
            for item in probs:
                ev = (item["prob"] * item["odd"]) - 1

                ok2, fail = sanity_check(item["prob"], item["mkt"], item["odd"])
                if not ok2:
                    log_rejection(fid, label, item["mkt"], item["odd"], ev, fail)
                    continue
                if ev < MIN_EV_THRESHOLD:
                    log_rejection(fid, label, item["mkt"], item["odd"], ev, "LOW_EV")
                    continue
                if ev > MAX_EV_THRESHOLD:
                    log_rejection(fid, label, item["mkt"], item["odd"], ev, "EV_ALUCINATION")
                    continue

                kelly, urs, rej = get_kelly_and_urs(ev, item["odd"], item["mkt"], l_name)
                if kelly == 0.0:
                    log_rejection(fid, label, item["mkt"], item["odd"], ev, rej)
                    continue

                print(f"     ✅ CANDIDATO: {item['mkt']} @{item['odd']:.2f} "
                      f"EV={ev*100:.1f}% URS={urs:.2f}")
                candidates.append({
                    **item, "ev": ev, "base_stake": kelly, "urs": urs,
                    "fid": fid, "h_n": h_n, "a_n": a_n, "ko": ko,
                    "l_name": l_name, "conf": conf, "xg_src": xg_src,
                    "xh": xh, "xa": xa, "xt": xt,
                })

            if not candidates:
                continue

            candidates.sort(key=lambda x: x["ev"] * x["urs"], reverse=True)
            preliminary_picks.append(candidates[0])

        final, meta = apply_portfolio_risk_engine(preliminary_picks)

        try:
            if final:
                conn = sqlite3.connect(DB_PATH)
                c    = conn.cursor()
                reports = [
                    f"📊 <b>European V5.13 — Portfolio:</b>\n"
                    f"Picks: {len(final)} | Vol: {meta['port_vol']*100:.2f}%\n"
                    f"Heat: {meta['final_heat']*100:.2f}% | Damper: {meta['damper']:.2f}x\n"
                    f"📡 Requests: {track_requests(0)}/100"
                ]
                for p in final:
                    op_stake = p["final_stake"] if LIVE_TRADING else 0.0
                    c.execute("""INSERT INTO picks_log
                        (fixture_id, league, home_team, away_team, market, selection,
                         selection_key, odd_open, prob_model, ev_open, stake_pct,
                         xg_home, xg_away, xg_total, pick_time, kickoff_time,
                         urs, model_gap, xg_source)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                        (p["fid"], p["l_name"], p["h_n"], p["a_n"], p["mkt"], p["pick"],
                         f"{p['bid']}|{p['val']}", p["odd"], p["prob"], p["ev"], op_stake,
                         p["xh"], p["xa"], p["xt"],
                         datetime.now(timezone.utc).isoformat(), p["ko"],
                         p["urs"], p["model_gap"], p["xg_src"])
                    )
                    conf_icon  = "✅" if p["conf"] == "HIGH" else "⚠️ MED"
                    gap_str    = f"+{p['model_gap']*100:.1f}%" if p["model_gap"] >= 0 else f"{p['model_gap']*100:.1f}%"
                    reports.append(
                        f"⚽ {p['h_n']} vs {p['a_n']} | {p['l_name']}\n"
                        f"🟡 [DRY-RUN] [{p['mkt']}]: {p['pick']}\n"
                        f"📊 Cuota: @{p['odd']} | EV: +{p['ev']*100:.1f}%\n"
                        f"📉 URS: {p['urs']:.2f} | LCP: {p['lcp_applied']:.2f}\n"
                        f"🔬 Gap: {gap_str} | xG: {p['xh']:.1f}-{p['xa']:.1f} {conf_icon}\n"
                        f"📈 Fuente: {p['xg_src']}\n"
                        f"🎯 Stake: {p['final_stake']*100:.2f}%"
                    )
                conn.commit()
                conn.close()
                self.send_msg("\n\n".join(reports))
            else:
                self.send_msg(
                    f"🔇 <b>European V5.13:</b> Sin picks válidos hoy.\n"
                    f"📡 Requests: {track_requests(0)}/100"
                )
        except Exception as e:
            print(f"run_daily_scan error: {e}")
            import traceback; traceback.print_exc()
        finally:
            ingest_results_into_xg_cache(self.headers)


if __name__ == "__main__":
    bot = QuantFundEuropean()

    schedule.every().day.at(RUN_TIME_SCAN).do(bot.run_daily_scan)
    schedule.every().day.at(RUN_TIME_MIDDAY_CLV).do(bot.capture_midday_lines)
    schedule.every(30).minutes.do(bot.capture_closing_lines)
    schedule.every().monday.at(RUN_TIME_XG_CACHE).do(bot.weekly_xg_cache)
    schedule.every().day.at(RUN_TIME_INGEST).do(bot.update_league_advanced_factors)

    try:
        from burn_in_evaluator import print_burn_in_report
        print_burn_in_report(DB_PATH)
    except Exception as e:
        print(f"Burn-in no disponible: {e}")

    try:
        print("\n🕵️  MORGUE:")
        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()
        c.execute("SELECT reason, COUNT(*) FROM decision_log "
                  "GROUP BY reason ORDER BY COUNT(*) DESC LIMIT 10")
        for r in c.fetchall():
            print(f"  ❌ {r[0]}: {r[1]}")
        conn.close()
    except:
        pass

    try:
        print("\n⏳ CLV:")
        conn = sqlite3.connect(DB_PATH)
        c    = conn.cursor()
        c.execute("""SELECT ((p.odd_open-c.odd_close)/p.odd_open)*100, p.market
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id
                          AND p.market=c.market
                          AND p.selection_key=c.selection_key
                     WHERE p.clv_captured=1""")
        picks = c.fetchall()
        conn.close()
        if picks:
            clvs  = [p[0] for p in picks]
            beats = sum(1 for v in clvs if v > 0)
            print(f"  N={len(picks)} | Beat={beats}/{len(picks)} ({beats/len(picks)*100:.0f}%) "
                  f"| CLV_avg={sum(clvs)/len(clvs):.2f}%")
            mkts = {}
            for clv, mkt in picks:
                mkts.setdefault(mkt, []).append(clv)
            for mkt, vals in sorted(mkts.items()):
                beat_m = sum(1 for v in vals if v > 0)
                print(f"    {mkt:<8} N={len(vals)} CLV={sum(vals)/len(vals):.2f}% "
                      f"Beat={beat_m}/{len(vals)}")
        else:
            print("  Sin CLVs aún.")
    except:
        pass

    cache_count = 0
    try:
        conn_check  = sqlite3.connect(DB_PATH)
        cc          = conn_check.cursor()
        cc.execute("SELECT COUNT(*) FROM team_xg_cache")
        cache_count = cc.fetchone()[0]
        conn_check.close()
        print(f"  📦 Cache xG al arrancar: {cache_count} equipos")
    except Exception as e:
        print(f"  Cache check error: {e}")

    reqs_disponibles = 100 - track_requests(0)
    print(f"  📡 Requests disponibles: {reqs_disponibles}/100")

    if cache_count == 0 and reqs_disponibles >= 50:
        print("  ⚠️  Cache vacía + budget OK — ejecutando warmup...")
        bot.send_msg(
            "⏳ <b>Primera vez detectada</b>\n"
            "Calentando cache xG (9 ligas europeas)...\n"
            "Esto toma ~3 minutos. El scan arranca después."
        )
        bot.weekly_xg_cache()
        reqs_post = 100 - track_requests(0)
        if reqs_post >= 30:
            print(f"  ✅ Warmup OK — {reqs_post} req restantes — arrancando scan")
            bot.run_daily_scan()
        else:
            bot.send_msg(
                f"✅ <b>Warmup completado</b>\n"
                f"⏰ Solo {reqs_post} req restantes — scan diferido.\n"
                f"Arranca mañana a las 09:00 UTC."
            )
    elif cache_count == 0 and reqs_disponibles < 50:
        bot.send_msg(
            f"⚠️ <b>Cache vacía, sin budget hoy</b>\n"
            f"Solo {reqs_disponibles} req disponibles.\n"
            f"Warmup automático el lunes 08:00 UTC."
        )
        bot.run_daily_scan()
    else:
        print(f"  ✅ Cache OK ({cache_count} equipos) — scan directo")
        bot.run_daily_scan()

    while True:
        schedule.run_pending()
        time.sleep(60)
