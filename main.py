import os
import time
import requests
import schedule
import sqlite3
import numpy as np
import math
from datetime import datetime, timedelta, timezone
from math import exp, lgamma, log

# ==========================================
# V5.10 QUANT FUND
# ==========================================
# CORRECCIONES VS V5.9:
#   1. Detección de xG "default": si xh y xa son demasiado similares
#      en un partido desequilibrado (cuotas asimétricas), se rechaza
#      el partido completo antes de calcular Poisson.
#   2. xG mínimo subido de 0.5 a 0.6 para evitar distribuciones
#      demasiado concentradas en 0 goles.
#   3. Validación de que xG refleja el desequilibrio real del partido:
#      si la cuota favorita es < 1.50 pero xg_ratio < 1.3, datos corruptos.
#   4. EV máximo bajado de 20% a 15% — los EVs de 19% eran sospechosos.
#   5. Logging más detallado de por qué se rechaza cada partido.
#   6. xG default detection: si ambos xG están entre 1.35-1.45
#      (zona de default) Y las cuotas son asimétricas, rechazar.

LIVE_TRADING = False

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY   = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

RUN_TIME_SCAN   = "02:50"
RUN_TIME_INGEST = "04:00"

MAX_DAILY_HEAT          = 0.10
TARGET_DAILY_VOLATILITY = 0.05
MIN_EV_THRESHOLD        = 0.015
MAX_EV_THRESHOLD        = 0.15   # bajado de 0.20 — EVs > 15% son sospechosos
MAX_PICKS_PER_FIXTURE   = 1
XG_DECAY_FACTOR         = 0.85

VOLATILITY_BUCKETS = {"OVER": 0.85, "UNDER": 0.85, "BTTS": 0.90, "1X2": 1.25}

TARGET_LEAGUES = {
    39: '🇬🇧 PREMIER', 140: '🇪🇸 LA LIGA', 135: '🇮🇹 SERIE A',
    78: '🇩🇪 BUNDESLIGA', 61: '🇫🇷 LIGUE 1', 2: '🏆 CHAMPIONS', 3: '🏆 EUROPA',
    88: '🇳🇱 EREDIVISIE', 94: '🇵🇹 PRIMEIRA', 40: '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP'
}

LIQUIDITY_TIERS = {
    '🇬🇧 PREMIER': 1.00, '🏆 CHAMPIONS': 1.00, '🇪🇸 LA LIGA': 1.00,
    '🇮🇹 SERIE A': 1.00, '🇩🇪 BUNDESLIGA': 1.00,
    '🏆 EUROPA': 0.85, '🇫🇷 LIGUE 1': 0.85, '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP': 0.85,
    '🇳🇱 EREDIVISIE': 0.75, '🇵🇹 PRIMEIRA': 0.75
}

LEAGUE_UPDATE_SCHEDULE = {
    0: [39, 94], 1: [140, 88], 2: [135, 40],
    3: [78], 4: [61], 5: [2], 6: [3]
}

# ==========================================
# DATABASE
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, league TEXT,
        home_team TEXT, away_team TEXT, market TEXT, selection TEXT, selection_key TEXT,
        odd_open REAL, prob_model REAL, ev_open REAL, stake_pct REAL,
        xg_home REAL, xg_away REAL, xg_total REAL,
        pick_time DATETIME, kickoff_time DATETIME,
        clv_captured INTEGER DEFAULT 0,
        urs REAL DEFAULT 0.0,
        model_gap REAL DEFAULT 0.0
    )""")
    for col, defn in [("urs", "REAL DEFAULT 0.0"), ("model_gap", "REAL DEFAULT 0.0")]:
        try: c.execute(f"ALTER TABLE picks_log ADD COLUMN {col} {defn}")
        except: pass

    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, market TEXT,
        selection_key TEXT, odd_close REAL, implied_prob_close REAL, capture_time DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS league_advanced_factors (
        league TEXT PRIMARY KEY, shots_avg REAL, shots_on_target_avg REAL,
        goals_per_shot REAL, goals_per_sot REAL, goal_std REAL,
        matches INTEGER, window_days INTEGER, last_updated DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS decision_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, match TEXT,
        market TEXT, odd REAL, ev REAL, reason TEXT, timestamp DATETIME
    )""")

    # Auto-heal CLVs corruptos
    c.execute("SELECT id, selection_key FROM picks_log WHERE clv_captured = 1")
    for pid, skey in c.fetchall():
        if skey and skey.split('|')[-1].replace('.', '', 1).isdigit():
            c.execute("DELETE FROM closing_lines WHERE selection_key = ?", (skey,))
            c.execute("UPDATE picks_log SET clv_captured = -1 WHERE id = ?", (pid,))

    # Auto-seed
    c.execute("SELECT COUNT(*) FROM league_advanced_factors")
    if c.fetchone()[0] == 0:
        baselines = [
            ('🇬🇧 PREMIER', 26.5, 9.2, 0.110, 0.32, 1.45),
            ('🇪🇸 LA LIGA', 23.8, 8.1, 0.098, 0.29, 1.35),
            ('🇮🇹 SERIE A', 24.2, 8.3, 0.102, 0.30, 1.38),
            ('🇩🇪 BUNDESLIGA', 27.1, 9.5, 0.115, 0.33, 1.52),
            ('🇫🇷 LIGUE 1', 24.0, 8.4, 0.100, 0.30, 1.36),
            ('🏆 CHAMPIONS', 25.5, 9.0, 0.108, 0.31, 1.42),
            ('🏆 EUROPA', 25.0, 8.8, 0.105, 0.30, 1.40),
            ('🇳🇱 EREDIVISIE', 28.0, 10.0, 0.118, 0.34, 1.55),
            ('🇵🇹 PRIMEIRA', 24.5, 8.5, 0.101, 0.30, 1.37),
            ('🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP', 23.5, 8.0, 0.095, 0.28, 1.32)
        ]
        now = datetime.now(timezone.utc).isoformat()
        for row in baselines:
            c.execute("INSERT INTO league_advanced_factors VALUES (?,?,?,?,?,?,?,?,?)",
                      (row[0], row[1], row[2], row[3], row[4], row[5], 100, 30, now))
    conn.commit(); conn.close()


def log_rejection(fixture_id, match, market, odd, ev, reason):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute(
            "INSERT INTO decision_log (fixture_id, match, market, odd, ev, reason, timestamp) VALUES (?,?,?,?,?,?,?)",
            (fixture_id, match, market, odd, ev, reason, datetime.now(timezone.utc).isoformat())
        )
        conn.commit(); conn.close()
    except: pass


# ==========================================
# URS ENGINE
# ==========================================

def get_avg_clv_by_market(market, lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
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
        res = c.fetchone()[0]; conn.close()
        return float(res) if res else 0.0
    except: return 0.0

def get_clv_sharpe():
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""SELECT (p.odd_open - c.odd_close)/p.odd_open
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.clv_captured=1
                     ORDER BY p.id DESC LIMIT 50""")
        clvs = [row[0] for row in c.fetchall()]; conn.close()
        if len(clvs) < 10: return 0.0
        mean_clv, std_clv = np.mean(clvs), np.std(clvs, ddof=1)
        return mean_clv / std_clv if std_clv != 0 else 0.0
    except: return 0.0

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

def score_liquidity_and_odd(odd, league_name):
    liq = LIQUIDITY_TIERS.get(league_name, 0.40)
    if odd < 1.20:    odd_score = 0.10
    elif odd < 1.40:  odd_score = 0.50
    elif odd <= 3.00: odd_score = 1.00
    elif odd <= 4.00: odd_score = 0.70
    else:             odd_score = 0.30
    return liq, odd_score

def calculate_unified_risk_score(sharpe, ev, league_name, odd):
    w = {"sharpe": 0.35, "ev_gcs": 0.30, "liquidity": 0.20, "odd_quality": 0.15}
    liq, odd_score = score_liquidity_and_odd(odd, league_name)
    urs = (w["sharpe"]      * score_sharpe(sharpe)) \
        + (w["ev_gcs"]      * score_ev_gcs(ev)) \
        + (w["liquidity"]   * liq) \
        + (w["odd_quality"] * odd_score)
    return max(0.10, min(urs, 1.00))

def get_base_kelly_and_urs(ev, odds, market, league_name):
    # Kill-switch granular por mercado
    avg_clv_market = get_avg_clv_by_market(market)
    avg_clv_global = get_avg_clv_by_market(None)  # global
    if avg_clv_market < -0.015:
        return 0.0, 0.0, f"KILL_SWITCH_{market}"
    if avg_clv_global < -0.025:
        return 0.0, 0.0, "KILL_SWITCH_GLOBAL"
    # FIX v5.11: max(0.0) en lugar de max(0.001) — respetar Kelly cuando hay poca convicción
    base_kelly = max(0.0, min(ev / (odds - 1), 0.05))
    if -0.015 <= avg_clv_market < 0.005: base_kelly *= 0.25
    urs = calculate_unified_risk_score(get_clv_sharpe(), ev, league_name, odds)
    return base_kelly * urs, urs, None


# ==========================================
# PORTFOLIO RISK ENGINE
# ==========================================

def apply_portfolio_risk_engine(preliminary_picks):
    if not preliminary_picks: return [], {}
    league_counts = {}
    for p in preliminary_picks:
        league_counts[p['l_name']] = league_counts.get(p['l_name'], 0) + 1

    port_var = 0.0
    for p in preliminary_picks:
        if p['odd'] <= 1.01:
            p['adj_stake'] = 0; p['lcp_applied'] = 0; continue
        lcp       = 1.0 / math.sqrt(league_counts[p['l_name']])
        adj_stake = p['base_stake'] * lcp
        beta      = VOLATILITY_BUCKETS.get(p['mkt'], 1.00)
        var_i     = beta * p['prob'] * (1.0 - p['prob']) * (p['odd'] ** 2)
        port_var += (adj_stake ** 2) * var_i
        p['adj_stake']   = adj_stake
        p['lcp_applied'] = lcp

    port_vol = math.sqrt(port_var) if port_var > 0 else 0.0001
    damper   = min(1.0, TARGET_DAILY_VOLATILITY / port_vol)

    total_heat = 0.0
    for p in preliminary_picks:
        p['final_stake'] = p['adj_stake'] * damper
        total_heat      += p['final_stake']

    heat_scale = 1.0
    if total_heat > MAX_DAILY_HEAT:
        heat_scale = MAX_DAILY_HEAT / total_heat
    for p in preliminary_picks:
        p['final_stake'] *= heat_scale
        p['final_stake']  = max(0.001, min(p['final_stake'], 0.05))

    meta = {
        'port_vol': port_vol, 'port_var': port_var, 'damper': damper,
        'heat_scale': heat_scale,
        'final_heat': sum(p['final_stake'] for p in preliminary_picks)
    }
    return preliminary_picks, meta


# ==========================================
# MATH ENGINE V5.10
# ==========================================

def get_league_factors(league_name):
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT shots_avg, goal_std FROM league_advanced_factors WHERE league = ?", (league_name,))
    row = c.fetchone(); conn.close()
    return {"pace": max(0.85, min(row[0]/24.0, 1.20)), "std": row[1]} if row else {"pace": 1.0, "std": None}


def _weighted_average(values, decay=XG_DECAY_FACTOR):
    if not values: return 0.0
    weights = [decay ** i for i in range(len(values))]
    return sum(v * w for v, w in zip(values, weights)) / sum(weights)


def validate_xg_consistency(xh, xa, bets):
    """
    V5.10: Detecta cuando el xG es el default (1.4/1.4) en un partido
    claramente desequilibrado. Si las cuotas dicen que un equipo es
    gran favorito pero los xG son casi iguales, los datos son corruptos.

    Retorna (True, None) si el xG es consistente con el mercado.
    Retorna (False, reason) si hay inconsistencia detectada.
    """
def validate_xg_consistency(xh, xa, bets):
    """
    V5.10: Detecta xG inválido comparando contra cuotas del mercado.

    Validaciones:
    1. Si hay gran favorito 1X2, el xG debe reflejarlo.
    2. Si el xG total es inconsistente con las cuotas Over/Under, rechazar.
       Ejemplo: xG total = 4.2 pero Over 2.5 paga @2.50 (implica solo 38%).
       Eso significa que el mercado ve un partido cerrado pero nuestro xG ve goleada.
    """
    home_odd = away_odd = None
    over_odd = under_odd = None

    for b in bets:
        if b['id'] == 1:
            for v in b['values']:
                try:
                    if v['value'] == 'Home': home_odd = float(v['odd'])
                    if v['value'] == 'Away': away_odd = float(v['odd'])
                except: pass
        elif b['id'] == 5:
            for v in b['values']:
                try:
                    if v['value'] == 'Over 2.5':  over_odd  = float(v['odd'])
                    if v['value'] == 'Under 2.5': under_odd = float(v['odd'])
                except: pass

    # ── Validación 1X2: xG debe reflejar el favorito ────────
    if home_odd and away_odd:
        min_odd = min(home_odd, away_odd)
        xg_ratio = max(xh, xa) / min(xh, xa) if min(xh, xa) > 0 else 1.0

        if min_odd < 1.40 and xg_ratio < 1.50:
            return False, f"XG_DEFAULT_DETECTED (min_odd={min_odd:.2f}, xg_ratio={xg_ratio:.2f}, xh={xh:.2f}, xa={xa:.2f})"

        elif min_odd < 1.65 and xg_ratio < 1.20:
            return False, f"XG_FLAT_ON_FAVOURITE (min_odd={min_odd:.2f}, xg_ratio={xg_ratio:.2f})"

        if 1.30 <= xh <= 1.50 and 1.30 <= xa <= 1.50 and min_odd < 1.60:
            return False, f"XG_LIKELY_DEFAULT (xh={xh:.2f}, xa={xa:.2f}, favourite_odd={min_odd:.2f})"

    # ── Validación Over/Under: xG total coherente con mercado ──
    if over_odd and under_odd:
        # Probabilidad implícita Over 2.5 del mercado (sin vig)
        p_over_market  = 1 / (over_odd  * 1.07)
        p_under_market = 1 / (under_odd * 1.07)

        # xG total esperado según el mercado:
        # Si Over paga @1.50 (67%), el mercado espera un partido con ~2.8-3.2 goles
        # Si Under paga @1.50 (67%), el mercado espera ~1.5-2.0 goles
        # Usamos la relación empírica: xG_market_implied ≈ -2.5 * ln(p_under_market)
        import math as _math
        if p_under_market > 0.01:
            xg_market_implied = -2.5 * _math.log(p_under_market)
            xg_total = xh + xa

            # Si nuestro xG difiere más de 1.8 goles del implícito del mercado,
            # los datos son sospechosos
            gap_xg = abs(xg_total - xg_market_implied)
            if gap_xg > 1.8:
                return False, (
                    f"XG_TOTAL_INCONSISTENT "
                    f"(xg_model={xg_total:.2f}, xg_market={xg_market_implied:.2f}, "
                    f"gap={gap_xg:.2f}, over_odd={over_odd})"
                )

    return True, None


def synthetic_xg_model_v510(preds, h_inj, a_inj, league_pace):
    """
    V5.10: xG con decay exponencial y detección de datos faltantes.
    Retorna también un flag de confianza en el xG calculado.
    """
    xh, xa = 1.4, 1.4
    xh_loaded = xa_loaded = False

    try:
        h_gf_avg = float(preds['teams']['home']['league']['goals']['for']['average']['home'])
        h_ga_avg = float(preds['teams']['away']['league']['goals']['against']['average']['away'])
        if h_gf_avg > 0 and h_ga_avg > 0:
            xh = (h_gf_avg + h_ga_avg) / 2
            xh_loaded = True
    except: pass

    try:
        a_gf_avg = float(preds['teams']['away']['league']['goals']['for']['average']['away'])
        a_ga_avg = float(preds['teams']['home']['league']['goals']['against']['average']['home'])
        if a_gf_avg > 0 and a_ga_avg > 0:
            xa = (a_gf_avg + a_ga_avg) / 2
            xa_loaded = True
    except: pass

    # Decay exponencial sobre forma reciente
    form_scores = {'W': 1.0, 'D': 0.5, 'L': 0.0}
    fh_str = preds['teams']['home'].get('league', {}).get('form', '')[-6:]
    fa_str = preds['teams']['away'].get('league', {}).get('form', '')[-6:]

    if fh_str:
        fh_series  = [form_scores.get(c, 0.5) for c in reversed(fh_str)]
        fh_weighted = _weighted_average(fh_series)
        fh_mult    = max(0.85, min(0.85 + fh_weighted * 0.30, 1.15))
    else:
        fh_mult = 1.0

    if fa_str:
        fa_series  = [form_scores.get(c, 0.5) for c in reversed(fa_str)]
        fa_weighted = _weighted_average(fa_series)
        fa_mult    = max(0.85, min(0.85 + fa_weighted * 0.30, 1.15))
    else:
        fa_mult = 1.0

    xh *= fh_mult * (1 - min(h_inj * 0.015, 0.08)) * league_pace
    xa *= fa_mult * (1 - min(a_inj * 0.015, 0.08)) * league_pace

    xh = max(0.6, min(xh, 3.5))
    xa = max(0.6, min(xa, 3.5))

    # Confianza: alta solo si ambos xG se cargaron de la API
    confidence = "HIGH" if (xh_loaded and xa_loaded) else "LOW"

    return xh, xa, xh + xa, confidence


def negbin_prob(mu, var, k):
    if mu <= 0: return 0.0
    if var <= mu * 1.01:
        return exp(k * log(mu) - mu - lgamma(k + 1))
    r = (mu ** 2) / (var - mu); p = r / (r + mu)
    return exp(lgamma(k+r) - lgamma(r) - lgamma(k+1) + r*log(p) + k*log(1-p)) \
           if r > 0 else exp(k * log(mu) - mu - lgamma(k + 1))

def calc_over_under_prob_adj(xg_total, line, league_std):
    var     = max(league_std ** 2, xg_total) if league_std else xg_total
    p_under = sum(negbin_prob(xg_total, var, k) for k in range(int(np.floor(line)) + 1))
    return 1 - p_under, p_under


def _poisson_pmf(mu, k):
    """Función de masa de probabilidad de Poisson. Segura contra overflow."""
    if mu <= 0 or k < 0: return 0.0
    try: return exp(-mu + k * log(mu) - lgamma(k + 1))
    except (ValueError, OverflowError): return 0.0


def bivariate_poisson_1x2(xg_home, xg_away, max_goals=10):
    """
    Poisson bivariado independiente para 1X2.
    Retorna None si los inputs son inválidos.
    """
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
    if total < 0.95: return None

    p_h = p_home / total
    p_d = p_draw / total
    p_a = p_away / total

    if not (0.08 <= p_d <= 0.55): return None

    return p_h, p_d, p_a


def calc_btts_prob(xg_home, xg_away):
    """
    BTTS (Both Teams To Score) usando Poisson independiente.

    P(home scores) = 1 - P(home_goals = 0) = 1 - e^(-xg_home)
    P(away scores) = 1 - P(away_goals = 0) = 1 - e^(-xg_away)
    P(BTTS Yes)    = P(home scores) * P(away scores)

    Matemáticamente exacto bajo independencia de Poisson.
    Solo válido si ambos xG tienen confianza HIGH.
    """
    if not (0.4 <= xg_home <= 4.0) or not (0.4 <= xg_away <= 4.0):
        return None, None

    p_home_scores = 1 - exp(-xg_home)
    p_away_scores = 1 - exp(-xg_away)
    p_btts_yes    = p_home_scores * p_away_scores
    p_btts_no     = 1 - p_btts_yes

    # Sanity: BTTS Yes entre 20-90% para ser realista como señal apostable
    if not (0.20 <= p_btts_yes <= 0.90):
        return None, None

    return round(p_btts_yes, 4), round(p_btts_no, 4)


# ==========================================
# V5.10: PURE PRICING ENGINE
# ==========================================

def build_market_probs_v510(bets, xh, xa, xt, league_std, h_n, a_n, conf="HIGH"):
    m_probs = []

    # Pre-calcular probabilidades del modelo
    poisson_result = bivariate_poisson_1x2(xh, xa)
    if poisson_result is not None:
        p_home, p_draw, p_away = poisson_result
        p_1x2  = {'Home': p_home, 'Draw': p_draw, 'Away': p_away}
        names  = {'Home': f"Gana {h_n}", 'Draw': "Empate", 'Away': f"Gana {a_n}"}
    else:
        p_1x2 = {}

    # BTTS solo con confianza HIGH — necesita xG confiables para ambos equipos
    p_btts_yes, p_btts_no = None, None
    if conf == "HIGH":
        p_btts_yes, p_btts_no = calc_btts_prob(xh, xa)

    for b in bets:

        # ── 1X2 ─────────────────────────────────────────────
        if b['id'] == 1:
            for v in b['values']:
                if v['value'] not in p_1x2: continue
                odd       = float(v['odd'])
                p_true    = p_1x2[v['value']]
                p_implied = 1 / (odd * 1.05)
                m_probs.append({
                    "mkt": "1X2", "pick": names[v['value']],
                    "odd": odd, "prob": p_true,
                    "bid": b['id'], "val": v['value'],
                    "p_implied": p_implied,
                    "model_gap": round(p_true - p_implied, 4)
                })

        # ── OVER / UNDER 2.5 ────────────────────────────────
        elif b['id'] == 5:
            po, pu = calc_over_under_prob_adj(xt, 2.5, league_std)
            for v in b['values']:
                if v['value'] not in ('Over 2.5', 'Under 2.5'): continue
                mkt_type  = "OVER" if 'Over' in v['value'] else "UNDER"
                p_true    = po if mkt_type == "OVER" else pu
                odd       = float(v['odd'])
                p_implied = 1 / (odd * 1.07)
                m_probs.append({
                    "mkt": mkt_type, "pick": f"{v['value']} Goles",
                    "odd": odd, "prob": p_true,
                    "bid": b['id'], "val": v['value'],
                    "p_implied": p_implied,
                    "model_gap": round(p_true - p_implied, 4)
                })

        # ── BTTS (Both Teams To Score) — bet ID 8 en api-football ──
        elif b['id'] == 8 and p_btts_yes is not None:
            for v in b['values']:
                if v['value'] not in ('Yes', 'No'): continue
                p_true    = p_btts_yes if v['value'] == 'Yes' else p_btts_no
                odd       = float(v['odd'])
                p_implied = 1 / (odd * 1.06)
                m_probs.append({
                    "mkt": "BTTS", "pick": f"Ambos Marcan: {v['value']}",
                    "odd": odd, "prob": p_true,
                    "bid": b['id'], "val": v['value'],
                    "p_implied": p_implied,
                    "model_gap": round(p_true - p_implied, 4)
                })

    return m_probs


def sanity_check_prob(p_true, mkt_type, odd):
    """Circuit breaker: mercado como auditor, no como co-autor."""
    VIG       = {"OVER": 1.07, "UNDER": 1.07, "1X2": 1.05, "BTTS": 1.06}
    vig       = VIG.get(mkt_type, 1.06)
    p_implied = 1 / (odd * vig)
    gap       = abs(p_true - p_implied)
    if gap > 0.25:
        return False, f"XG_SANITY_FAIL (gap={gap:.2f}, p_model={p_true:.2f}, p_impl={p_implied:.2f})"
    return True, None


# ==========================================
# MAIN BOT
# ==========================================

class QuantFundNode:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        mode = "🔴 LIVE TRADING" if LIVE_TRADING else "🟡 DRY-RUN MODE (Batch Burn-in)"
        self.send_msg(
            f"🛡️ <b>QUANT FUND V5.11 DEPLOYED</b>\n"
            f"Estado: {mode}\n"
            f"Fix: selección ev×urs · kill-switch granular · anti-duplicados CLV"
        )

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        try:
            requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"},
                timeout=10
            )
        except: pass

    def update_league_advanced_factors(self):
        today   = datetime.now(timezone.utc)
        weekday = today.weekday()
        if weekday not in LEAGUE_UPDATE_SCHEDULE: return
        for league_id in LEAGUE_UPDATE_SCHEDULE[weekday]:
            league_name = TARGET_LEAGUES.get(league_id)
            if not league_name: continue
            season = today.year if today.month >= 8 else today.year - 1
            try:
                teams = requests.get(
                    f"https://v3.football.api-sports.io/teams?league={league_id}&season={season}",
                    headers=self.headers, timeout=15
                ).json().get("response", [])
            except: continue

            t_shots = t_sot = t_goals = 0
            match_counts = []
            for t in teams:
                time.sleep(1.1)
                try:
                    stats = requests.get(
                        f"https://v3.football.api-sports.io/teams/statistics"
                        f"?league={league_id}&season={season}&team={t['team']['id']}",
                        headers=self.headers, timeout=15
                    ).json().get("response")
                    sh  = stats['shots'].get('total', 0)
                    sot = stats['shots'].get('on', 0)
                    gls = stats['goals']['for']['total'].get('total', 0)
                    m   = stats['fixtures']['played'].get('total', 0)
                    if sh and gls and m > 0:
                        t_shots += sh; t_sot += (sot or 0)
                        t_goals += gls; match_counts.append(m)
                except: continue

            m_total = sum(match_counts) / 2
            if m_total > 0:
                sh_avg    = t_shots / m_total
                sot_avg   = t_sot   / m_total
                gps       = t_goals / t_shots
                gsot      = t_goals / max(t_sot, 1)
                std_proxy = np.sqrt(gps * sh_avg)
                conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                c.execute("""
                    INSERT INTO league_advanced_factors
                        (league, shots_avg, shots_on_target_avg, goals_per_shot,
                         goals_per_sot, goal_std, matches, window_days, last_updated)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(league) DO UPDATE SET
                        shots_avg=excluded.shots_avg,
                        shots_on_target_avg=excluded.shots_on_target_avg,
                        goal_std=excluded.goal_std,
                        last_updated=excluded.last_updated
                """, (league_name, round(sh_avg,2), round(sot_avg,2), round(gps,4),
                      round(gsot,4), round(std_proxy,3), int(m_total), 30, today.isoformat()))
                conn.commit(); conn.close()

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            now  = datetime.now(timezone.utc)
            c.execute("SELECT id, fixture_id, market, selection_key, kickoff_time FROM picks_log WHERE clv_captured = 0")
            for pid, fid, mkt, skey, ko in c.fetchall():
                mins_to_ko = (datetime.fromisoformat(ko) - now).total_seconds() / 60.0
                if mins_to_ko <= 60.0:
                    res = requests.get(
                        f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8",
                        headers=self.headers
                    ).json()
                    found = False
                    if res.get('response'):
                        for b in res['response'][0]['bookmakers'][0]['bets']:
                            for v in b['values']:
                                if f"{b['id']}|{v['value']}" == skey:
                                    # FIX v5.11: verificar duplicado antes de INSERT
                                    c.execute(
                                        "SELECT COUNT(*) FROM closing_lines WHERE fixture_id=? AND market=? AND selection_key=?",
                                        (fid, mkt, skey)
                                    )
                                    if c.fetchone()[0] == 0:
                                        c.execute(
                                            "INSERT INTO closing_lines (fixture_id, market, selection_key, odd_close, implied_prob_close, capture_time) VALUES (?,?,?,?,?,?)",
                                            (fid, mkt, skey, float(v['odd']), 1/float(v['odd']), now.isoformat())
                                        )
                                    found = True; break
                    c.execute("UPDATE picks_log SET clv_captured = ? WHERE id = ?",
                              (1 if found else -1, pid))
            conn.commit(); conn.close()
        except: pass

    def run_daily_scan(self):
        today    = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        matches  = []
        for d in [today, tomorrow]:
            try:
                matches.extend([
                    f for f in requests.get(
                        f"https://v3.football.api-sports.io/fixtures?date={d}",
                        headers=self.headers
                    ).json().get('response', [])
                    if f['league']['id'] in TARGET_LEAGUES
                ])
            except: pass

        preliminary_picks  = []
        fixture_pick_count = {}

        for m in matches[:40]:
            fid         = m['fixture']['id']
            h_n         = m['teams']['home']['name']
            a_n         = m['teams']['away']['name']
            l_name      = TARGET_LEAGUES[m['league']['id']]
            ko          = m['fixture']['date']
            match_label = f"{h_n} vs {a_n}"
            time.sleep(6.1)

            try:
                odds_res  = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json().get('response', [])
                preds_res = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}",      headers=self.headers).json().get('response', [])
                inj_res   = requests.get(f"https://v3.football.api-sports.io/injuries?fixture={fid}",         headers=self.headers).json().get('response', [])
            except: continue

            if not odds_res or not preds_res: continue

            bets  = odds_res[0]['bookmakers'][0]['bets']
            preds = preds_res[0]
            hinj  = sum(1 for i in inj_res if i['team']['id'] == m['teams']['home']['id'])
            ainj  = sum(1 for i in inj_res if i['team']['id'] == m['teams']['away']['id'])

            factors             = get_league_factors(l_name)
            xh, xa, xt, conf   = synthetic_xg_model_v510(preds, hinj, ainj, factors['pace'])

            # V5.10: Validar que el xG es consistente con el desequilibrio real del partido
            xg_valid, xg_reason = validate_xg_consistency(xh, xa, bets)
            if not xg_valid:
                log_rejection(fid, match_label, 'ALL', 0.0, 0.0, xg_reason)
                continue

            # Si la confianza en el xG es baja (datos de API incompletos),
            # solo procesar Over/Under, no 1X2
            if conf == "LOW":
                log_rejection(fid, match_label, '1X2', 0.0, 0.0, "XG_LOW_CONFIDENCE_SKIP_1X2")

            m_probs = build_market_probs_v510(bets, xh, xa, xt, factors['std'], h_n, a_n, conf=conf)

            # Si confianza baja, filtrar 1X2 de los candidatos
            if conf == "LOW":
                m_probs = [p for p in m_probs if p['mkt'] != '1X2']

            if not any(p['mkt'] == '1X2' for p in m_probs) and conf == "HIGH":
                log_rejection(fid, match_label, '1X2', 0.0, 0.0,
                              f"POISSON_XG_INVALID (xh={xh:.2f}, xa={xa:.2f})")

            candidates = []
            for item in m_probs:
                ev = (item['prob'] * item['odd']) - 1

                ok, fail_reason = sanity_check_prob(item['prob'], item['mkt'], item['odd'])
                if not ok:
                    log_rejection(fid, match_label, item['mkt'], item['odd'], ev, fail_reason)
                    continue

                if ev < MIN_EV_THRESHOLD:
                    log_rejection(fid, match_label, item['mkt'], item['odd'], ev, "LOW_EV")
                    continue
                if ev > MAX_EV_THRESHOLD:
                    log_rejection(fid, match_label, item['mkt'], item['odd'], ev, "EV_ALUCINATION")
                    continue

                base_stake, urs_score, rejection_reason = get_base_kelly_and_urs(
                    ev, item['odd'], item['mkt'], l_name
                )
                if base_stake == 0.0:
                    log_rejection(fid, match_label, item['mkt'], item['odd'], ev, rejection_reason)
                    continue

                candidates.append({**item, 'ev': ev, 'base_stake': base_stake, 'urs_score': urs_score})

            if not candidates: continue

            # FIX v5.11: ordenar por ev*urs (valor ajustado al riesgo)
            # El URS existe para ser criterio de selección — ignorarlo aquí lo anulaba
            candidates.sort(key=lambda x: x['ev'] * x['urs_score'], reverse=True)
            for pick in candidates[:MAX_PICKS_PER_FIXTURE]:
                if fixture_pick_count.get(fid, 0) >= MAX_PICKS_PER_FIXTURE:
                    break
                fixture_pick_count[fid] = fixture_pick_count.get(fid, 0) + 1
                preliminary_picks.append({
                    'fid': fid, 'l_name': l_name, 'h_n': h_n, 'a_n': a_n,
                    'mkt': pick['mkt'], 'pick': pick['pick'],
                    'skey': f"{pick['bid']}|{pick['val']}",
                    'odd': pick['odd'], 'prob': pick['prob'], 'ev': pick['ev'],
                    'model_gap': pick['model_gap'], 'conf': conf,
                    'base_stake': pick['base_stake'], 'urs_score': pick['urs_score'],
                    'ko': ko, 'xh': xh, 'xa': xa, 'xt': xt
                })

        # --- PORTFOLIO RISK ENGINE ---
        final_picks, port_meta = apply_portfolio_risk_engine(preliminary_picks)

        # FIX v5.11: filtrar picks con stake tan baja que no tiene sentido reportar
        final_picks = [p for p in final_picks if p.get('final_stake', 0) >= 0.005]

        if final_picks:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            reports = [
                f"📊 <b>Portfolio Metrics (V5.10):</b>\n"
                f"Vol Proyectada: {port_meta['port_vol']*100:.2f}%\n"
                f"Damper: {port_meta['damper']:.2f}x\n"
                f"Heat Total: {port_meta['final_heat']*100:.2f}%\n"
                f"Picks: {len(final_picks)}"
            ]
            for p in final_picks:
                op_stake = p['final_stake'] if LIVE_TRADING else 0.0
                c.execute("""
                    INSERT INTO picks_log
                        (fixture_id, league, home_team, away_team, market, selection,
                         selection_key, odd_open, prob_model, ev_open, stake_pct,
                         xg_home, xg_away, xg_total, pick_time, kickoff_time, urs, model_gap)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p['fid'], p['l_name'], p['h_n'], p['a_n'], p['mkt'], p['pick'],
                     p['skey'], p['odd'], p['prob'], p['ev'], op_stake,
                     p['xh'], p['xa'], p['xt'],
                     datetime.now(timezone.utc).isoformat(), p['ko'],
                     p['urs_score'], p['model_gap'])
                )
                prefix     = "🟡 [DRY-RUN]" if not LIVE_TRADING else ("💰" if op_stake > 0 else "⚠️")
                disp_stake = p['final_stake'] if not LIVE_TRADING else op_stake
                gap_str    = f"+{p['model_gap']*100:.1f}%" if p['model_gap'] >= 0 else f"{p['model_gap']*100:.1f}%"
                conf_icon  = "✅" if p['conf'] == "HIGH" else "⚠️"

                reports.append(
                    f"⚽ {p['h_n']} vs {p['a_n']} | {p['l_name']}\n"
                    f"{prefix} [{p['mkt']}]: {p['pick']}\n"
                    f"📊 Cuota: @{p['odd']} | EV: +{p['ev']*100:.1f}%\n"
                    f"📉 URS: {p['urs_score']:.2f} | LCP: {p['lcp_applied']:.2f}\n"
                    f"🔬 Gap: {gap_str} | xG: {p['xh']:.1f}-{p['xa']:.1f} {conf_icon}\n"
                    f"🎯 Stake: {disp_stake*100:.2f}%"
                )
            conn.commit(); conn.close()
            self.send_msg("\n\n".join(reports))
        else:
            self.send_msg("🔇 <b>Scan V5.10 completado.</b> Sin picks válidos hoy.")


if __name__ == "__main__":
    bot = QuantFundNode()

    schedule.every().day.at(RUN_TIME_SCAN).do(bot.run_daily_scan)
    schedule.every().day.at(RUN_TIME_INGEST).do(bot.update_league_advanced_factors)
    schedule.every(30).minutes.do(bot.capture_closing_lines)

    # --- BURN-IN EVALUATOR ---
    try:
        from burn_in_evaluator import print_burn_in_report
        print_burn_in_report(DB_PATH)
    except Exception as e:
        print(f"Burn-in evaluator no disponible: {e}")

    # --- AUDITOR DE LA MORGUE ---
    try:
        print("\n🕵️  AUDITORÍA DE RECHAZOS (LA MORGUE)")
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT reason, COUNT(*) FROM decision_log GROUP BY reason ORDER BY COUNT(*) DESC")
        for r in c.fetchall():
            print(f"  ❌ {r[0]}: {r[1]}")
        c.execute("SELECT match, market, odd, ev, reason, timestamp FROM decision_log ORDER BY id DESC LIMIT 10")
        for row in c.fetchall():
            match, market, odd, ev, reason, ts = row
            print(f"  [{ts[:20]}] {match} | {market} @{odd:.2f} | EV:{ev*100:.1f}% -> {reason}")
        conn.close()
        print()
    except Exception as e:
        print(f"Error Morgue: {e}")

    # --- AUDITOR DE CLV ---
    try:
        print("⏳ Auditoría de CLV...")
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""
            SELECT ((p.odd_open - c.odd_close) / p.odd_open) * 100,
                   p.model_gap * 100, p.market
            FROM picks_log p
            JOIN closing_lines c ON p.fixture_id=c.fixture_id
                AND p.market=c.market AND p.selection_key=c.selection_key
            WHERE p.clv_captured = 1
        """)
        picks = c.fetchall(); conn.close()
        if not picks:
            print("  Sin CLVs capturados aún.\n")
        else:
            clvs    = [p[0] for p in picks]
            beats   = sum(1 for v in clvs if v > 0)
            avg_clv = sum(clvs) / len(clvs)
            avg_gap = sum(p[1] for p in picks) / len(picks)
            print(f"  Picks validados : {len(picks)}")
            print(f"  Beat Rate       : {beats}/{len(picks)} ({beats/len(picks)*100:.1f}%)")
            print(f"  CLV Promedio    : {avg_clv:.2f}%")
            print(f"  Model Gap avg   : {avg_gap:.2f}%\n")
            mkts = {}
            for clv, gap, mkt in picks:
                if mkt not in mkts: mkts[mkt] = []
                mkts[mkt].append(clv)
            for mkt, vals in mkts.items():
                print(f"  {mkt:<8} N={len(vals)} CLV_avg={sum(vals)/len(vals):.2f}%")
            print()
    except Exception as e:
        print(f"Error CLV: {e}")

    bot.run_daily_scan()

    while True:
        schedule.run_pending()
        time.sleep(60)
