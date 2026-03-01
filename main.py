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
# V5.8 QUANT FUND CONFIGURATION
# ==========================================

LIVE_TRADING = False  # SHADOW MODE: Burn-in required

TELEGRAM_TOKEN  = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY  = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

RUN_TIME_SCAN   = "02:50"
RUN_TIME_INGEST = "04:00"

MAX_DAILY_HEAT        = 0.10
TARGET_DAILY_VOLATILITY = 0.05

VOLATILITY_BUCKETS = {"OVER": 0.85, "UNDER": 0.85, "BTTS": 1.00, "1X2": 1.25}

TARGET_LEAGUES = {
    39: '🇬🇧 PREMIER', 140: '🇪🇸 LA LIGA', 135: '🇮🇹 SERIE A',
    78: '🇩🇪 BUNDESLIGA', 61: '🇫🇷 LIGUE 1', 2: '🏆 CHAMPIONS', 3: '🏆 EUROPA',
    88: '🇳🇱 EREDIVISIE', 94: '🇵🇹 PRIMEIRA', 40: '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP'
}

LIQUIDITY_TIERS = {
    '🇬🇧 PREMIER': 1.00, '🏆 CHAMPIONS': 1.00, '🇪🇸 LA LIGA': 1.00, '🇮🇹 SERIE A': 1.00, '🇩🇪 BUNDESLIGA': 1.00,
    '🏆 EUROPA': 0.85, '🇫🇷 LIGUE 1': 0.85, '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP': 0.85,
    '🇳🇱 EREDIVISIE': 0.75, '🇵🇹 PRIMEIRA': 0.75
}

LEAGUE_UPDATE_SCHEDULE = {0: [39, 94], 1: [140, 88], 2: [135, 40], 3: [78], 4: [61], 5: [2], 6: [3]}


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
    # Migraciones seguras
    for col, defn in [("urs", "REAL DEFAULT 0.0"), ("model_gap", "REAL DEFAULT 0.0")]:
        try: c.execute(f"ALTER TABLE picks_log ADD COLUMN {col} {defn}")
        except: pass

    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, market TEXT, selection_key TEXT,
        odd_close REAL, implied_prob_close REAL, capture_time DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS league_advanced_factors (
        league TEXT PRIMARY KEY, shots_avg REAL, shots_on_target_avg REAL, goals_per_shot REAL,
        goals_per_sot REAL, goal_std REAL, matches INTEGER, window_days INTEGER, last_updated DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS decision_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, match TEXT, market TEXT,
        odd REAL, ev REAL, reason TEXT, timestamp DATETIME
    )""")

    # Auto-heal: limpiar CLVs corruptos donde la llave era una cuota numérica
    c.execute("SELECT id, selection_key FROM picks_log WHERE clv_captured = 1")
    for pid, skey in c.fetchall():
        if skey and skey.split('|')[-1].replace('.', '', 1).isdigit():
            c.execute("DELETE FROM closing_lines WHERE selection_key = ?", (skey,))
            c.execute("UPDATE picks_log SET clv_captured = -1 WHERE id = ?", (pid,))

    # Auto-seed con baselines si la tabla está vacía
    c.execute("SELECT COUNT(*) FROM league_advanced_factors")
    if c.fetchone()[0] == 0:
        baselines = [
            ('🇬🇧 PREMIER', 26.5, 9.2, 0.110, 0.32, 1.45), ('🇪🇸 LA LIGA', 23.8, 8.1, 0.098, 0.29, 1.35),
            ('🇮🇹 SERIE A', 24.2, 8.3, 0.102, 0.30, 1.38), ('🇩🇪 BUNDESLIGA', 27.1, 9.5, 0.115, 0.33, 1.52),
            ('🇫🇷 LIGUE 1', 24.0, 8.4, 0.100, 0.30, 1.36), ('🏆 CHAMPIONS', 25.5, 9.0, 0.108, 0.31, 1.42),
            ('🏆 EUROPA', 25.0, 8.8, 0.105, 0.30, 1.40), ('🇳🇱 EREDIVISIE', 28.0, 10.0, 0.118, 0.34, 1.55),
            ('🇵🇹 PRIMEIRA', 24.5, 8.5, 0.101, 0.30, 1.37), ('🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP', 23.5, 8.0, 0.095, 0.28, 1.32)
        ]
        now = datetime.now(timezone.utc).isoformat()
        for row in baselines:
            c.execute("INSERT INTO league_advanced_factors VALUES (?,?,?,?,?,?,?,?,?)",
                      (row[0], row[1], row[2], row[3], row[4], row[5], 100, 30, now))
    conn.commit(); conn.close()


def log_rejection(fixture_id, match, market, odd, ev, reason):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("INSERT INTO decision_log (fixture_id, match, market, odd, ev, reason, timestamp) VALUES (?,?,?,?,?,?,?)",
                  (fixture_id, match, market, odd, ev, reason, datetime.now(timezone.utc).isoformat()))
        conn.commit(); conn.close()
    except: pass


# ==========================================
# URS ENGINE
# ==========================================

def get_avg_clv_by_market(market, lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""SELECT AVG((p.odd_open - c.odd_close)/p.odd_open)
                     FROM picks_log p JOIN closing_lines c
                       ON p.fixture_id=c.fixture_id AND p.market=c.market AND p.selection_key=c.selection_key
                     WHERE p.market=? AND p.clv_captured=1
                     ORDER BY p.id DESC LIMIT ?""", (market, lookback))
        res = c.fetchone()[0]; conn.close()
        return float(res) if res else 0.0
    except: return 0.0

def get_clv_sharpe():
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""SELECT (p.odd_open - c.odd_close)/p.odd_open
                     FROM picks_log p JOIN closing_lines c ON p.fixture_id=c.fixture_id AND p.clv_captured=1
                     ORDER BY p.id DESC LIMIT 50""")
        clvs = [row[0] for row in c.fetchall()]; conn.close()
        if len(clvs) < 10: return 0.0
        mean_clv, std_clv = np.mean(clvs), np.std(clvs, ddof=1)
        return mean_clv / std_clv if std_clv != 0 else 0.0
    except: return 0.0

def score_sharpe(s):
    if s < -0.5: return 0.10
    elif s < 0.0: return 0.30
    elif s < 0.5: return 0.50
    elif s < 1.0: return 0.75
    elif s < 1.5: return 0.90
    else: return 1.00

def score_ev_gcs(ev):
    if ev < 0.03: return 0.20
    elif ev < 0.05: return 0.40
    elif ev < 0.08: return 0.60
    elif ev < 0.12: return 0.80
    else: return 1.00

def score_market_always_win(odd):
    if 1.40 <= odd <= 1.60: return 1.00
    elif 1.60 < odd <= 2.10: return 0.80
    elif odd < 1.40: return 0.10
    else: return 0.30

def calculate_unified_risk_score(sharpe, ev, league_name, odd):
    w = {"sharpe": 0.35, "ev_gcs": 0.30, "liquidity": 0.20, "market": 0.15}
    urs = (w["sharpe"]    * score_sharpe(sharpe)) \
        + (w["ev_gcs"]    * score_ev_gcs(ev)) \
        + (w["liquidity"] * LIQUIDITY_TIERS.get(league_name, 0.40)) \
        + (w["market"]    * score_market_always_win(odd))
    return max(0.10, min(urs, 1.00))

def get_base_kelly_and_urs(ev, odds, market, league_name):
    avg_clv = get_avg_clv_by_market(market)
    if avg_clv < -0.015: return 0.0, 0.0, "KILL_SWITCH_ACTIVE"
    base_kelly = max(0.0, min(ev / (odds - 1), 0.05))
    if -0.015 <= avg_clv < 0.005: base_kelly *= 0.25
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
        p['adj_stake']  = adj_stake
        p['lcp_applied'] = lcp

    port_vol = math.sqrt(port_var) if port_var > 0 else 0.0001
    damper   = min(1.0, TARGET_DAILY_VOLATILITY / port_vol)

    total_heat = 0.0
    for p in preliminary_picks:
        p['final_stake'] = p['adj_stake'] * damper
        total_heat += p['final_stake']

    heat_scale = 1.0
    if total_heat > MAX_DAILY_HEAT:
        heat_scale = MAX_DAILY_HEAT / total_heat
    for p in preliminary_picks:
        p['final_stake'] *= heat_scale
        p['final_stake']  = max(0.001, min(p['final_stake'], 0.05))

    meta = {
        'port_vol': port_vol, 'port_var': port_var,
        'damper': damper, 'heat_scale': heat_scale,
        'final_heat': sum(p['final_stake'] for p in preliminary_picks)
    }
    return preliminary_picks, meta


# ==========================================
# MATH ENGINE
# ==========================================

def get_league_factors(league_name):
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT shots_avg, goal_std FROM league_advanced_factors WHERE league = ?", (league_name,))
    row = c.fetchone(); conn.close()
    return {"pace": max(0.85, min(row[0]/24.0, 1.20)), "std": row[1]} if row else {"pace": 1.0, "std": None}

def synthetic_xg_model(preds, h_inj, a_inj, league_pace):
    xh, xa = 1.4, 1.4
    try: xh = (float(preds['teams']['home']['league']['goals']['for']['average']['home'])
               + float(preds['teams']['away']['league']['goals']['against']['average']['away'])) / 2
    except: pass
    try: xa = (float(preds['teams']['away']['league']['goals']['for']['average']['away'])
               + float(preds['teams']['home']['league']['goals']['against']['average']['home'])) / 2
    except: pass
    fh_str = preds['teams']['home'].get('league', {}).get('form', 'WWDLD')[-5:]
    fa_str = preds['teams']['away'].get('league', {}).get('form', 'WWDLD')[-5:]
    fh = fh_str.count('W')*3 + fh_str.count('D')
    fa = fa_str.count('W')*3 + fa_str.count('D')
    xh *= max(0.85, min((7 if fh==0 else fh)/10, 1.15)) * (1 - min(h_inj*0.015, 0.08)) * league_pace
    xa *= max(0.85, min((7 if fa==0 else fa)/10, 1.15)) * (1 - min(a_inj*0.015, 0.08)) * league_pace
    return max(0.5, min(xh, 3.5)), max(0.5, min(xa, 3.5)), xh + xa

def negbin_prob(mu, var, k):
    if mu <= 0: return 0.0
    if var <= mu * 1.01: return exp(k * log(mu) - mu - lgamma(k + 1))
    r = (mu ** 2) / (var - mu); p = r / (r + mu)
    return exp(lgamma(k+r) - lgamma(r) - lgamma(k+1) + r*log(p) + k*log(1-p)) if r > 0 \
           else exp(k * log(mu) - mu - lgamma(k + 1))

def calc_over_under_prob_adj(xg_total, line, league_std):
    var     = max(league_std ** 2, xg_total) if league_std else xg_total
    p_under = sum(negbin_prob(xg_total, var, k) for k in range(int(np.floor(line)) + 1))
    return 1 - p_under, p_under


# ==========================================
# V5.8: PURE PRICING ENGINE
# ==========================================

def build_market_probs_v58(bets, preds, xh, xa, xt, league_std, h_n, a_n):
    """
    Señal 100% independiente del precio del casino.
    p_implied y model_gap son campos de DIAGNÓSTICO, nunca afectan p_true.
    """
    m_probs = []
    for b in bets:

        # ── 1X2 ─────────────────────────────────────────────
        if b['id'] == 1:
            api_keys  = {'Home': 'home', 'Away': 'away', 'Draw': 'draw'}
            raw       = {}
            total_raw = 0.0
            for val_key, api_key in api_keys.items():
                try:
                    pct = float(preds['predictions']['percent'][api_key].replace('%', '')) / 100
                except Exception:
                    pct = 1/3
                raw[val_key] = pct
                total_raw   += pct
            for val_key in raw:
                raw[val_key] = raw[val_key] / total_raw if total_raw > 0 else 1/3

            for v in b['values']:
                if v['value'] not in raw: continue
                odd       = float(v['odd'])
                p_true    = raw[v['value']]
                p_implied = 1 / (odd * 1.05)
                name      = (f"Gana {h_n}" if v['value'] == 'Home'
                             else f"Gana {a_n}" if v['value'] == 'Away' else "Empate")
                m_probs.append({
                    "mkt": "1X2", "pick": name, "odd": odd, "prob": p_true,
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
    return m_probs


def sanity_check_prob(p_true, mkt_type, odd):
    """
    Circuit breaker: el mercado como AUDITOR, no como co-autor.
    Gap > 0.25 indica datos corruptos en el pipeline de xG.
    """
    VIG       = {"OVER": 1.07, "UNDER": 1.07, "1X2": 1.05}
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
            f"🛡️ <b>QUANT FUND V5.8 DEPLOYED</b>\n"
            f"Estado: {mode}\n"
            f"Pure Pricing + Sanity Check + model_gap logging activos."
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
                        t_shots += sh
                        t_sot   += (sot or 0)
                        t_goals += gls
                        match_counts.append(m)
                except: continue

            m_total = sum(match_counts) / 2
            if m_total > 0:
                # FIX V5.8: desempaquetado correcto (bug crítico en versiones anteriores)
                sh_avg    = t_shots / m_total
                sot_avg   = t_sot   / m_total
                gps       = t_goals / t_shots
                gsot      = t_goals / max(t_sot, 1)
                std_proxy = np.sqrt(gps * sh_avg)

                conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                c.execute("""
                    INSERT INTO league_advanced_factors
                        (league, shots_avg, shots_on_target_avg, goals_per_shot, goals_per_sot,
                         goal_std, matches, window_days, last_updated)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(league) DO UPDATE SET
                        shots_avg           = excluded.shots_avg,
                        shots_on_target_avg = excluded.shots_on_target_avg,
                        goal_std            = excluded.goal_std,
                        last_updated        = excluded.last_updated
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
                    res   = requests.get(
                        f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8",
                        headers=self.headers
                    ).json()
                    found = False
                    if res.get('response'):
                        for b in res['response'][0]['bookmakers'][0]['bets']:
                            for v in b['values']:
                                if f"{b['id']}|{v['value']}" == skey:
                                    c.execute(
                                        "INSERT INTO closing_lines (fixture_id, market, selection_key, odd_close, implied_prob_close, capture_time) VALUES (?,?,?,?,?,?)",
                                        (fid, mkt, skey, float(v['odd']), 1/float(v['odd']), now.isoformat())
                                    )
                                    found = True; break
                    c.execute("UPDATE picks_log SET clv_captured = ? WHERE id = ?", (1 if found else -1, pid))
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

        preliminary_picks = []

        for m in matches[:40]:
            fid         = m['fixture']['id']
            h_n         = m['teams']['home']['name']
            a_n         = m['teams']['away']['name']
            l_name      = TARGET_LEAGUES[m['league']['id']]
            ko          = m['fixture']['date']
            match_label = f"{h_n} vs {a_n}"
            time.sleep(6.1)

            try:
                odds_res  = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8",  headers=self.headers).json().get('response', [])
                preds_res = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}",       headers=self.headers).json().get('response', [])
                inj_res   = requests.get(f"https://v3.football.api-sports.io/injuries?fixture={fid}",          headers=self.headers).json().get('response', [])
            except: continue

            if not odds_res or not preds_res: continue

            bets  = odds_res[0]['bookmakers'][0]['bets']
            preds = preds_res[0]
            hinj  = sum(1 for i in inj_res if i['team']['id'] == m['teams']['home']['id'])
            ainj  = sum(1 for i in inj_res if i['team']['id'] == m['teams']['away']['id'])

            factors    = get_league_factors(l_name)
            xh, xa, xt = synthetic_xg_model(preds, hinj, ainj, factors['pace'])

            # V5.8: Pure Pricing — sin circularidad
            m_probs = build_market_probs_v58(bets, preds, xh, xa, xt, factors['std'], h_n, a_n)

            best_pick, max_ev = None, -1.0
            for item in m_probs:
                ev = (item['prob'] * item['odd']) - 1

                # Circuit breaker
                ok, fail_reason = sanity_check_prob(item['prob'], item['mkt'], item['odd'])
                if not ok:
                    log_rejection(fid, match_label, item['mkt'], item['odd'], ev, fail_reason)
                    continue

                if ev > max_ev:
                    max_ev, best_pick = ev, item

            if not best_pick: continue

            odd  = best_pick['odd']
            prob = best_pick['prob']
            mkt  = best_pick['mkt']
            val  = best_pick['val']
            gap  = best_pick['model_gap']

            if max_ev < 0.02: log_rejection(fid, match_label, mkt, odd, max_ev, "LOW_EV");         continue
            if max_ev > 0.20: log_rejection(fid, match_label, mkt, odd, max_ev, "EV_ALUCINATION"); continue

            aw_category = (
                "💎 SIMPLE" if 1.60 <= odd <= 2.10 and prob > 0.50 else
                "🧱 PARLAY" if 1.40 <= odd <  1.60 and prob > 0.60 else
                None
            )
            if not aw_category:
                log_rejection(fid, match_label, mkt, odd, max_ev, "OUT_OF_ALWAYS_WIN_RANGE"); continue

            base_stake, urs_score, rejection_reason = get_base_kelly_and_urs(max_ev, odd, mkt, l_name)
            if base_stake == 0.0:
                log_rejection(fid, match_label, mkt, odd, max_ev, rejection_reason); continue

            preliminary_picks.append({
                'fid': fid, 'l_name': l_name, 'h_n': h_n, 'a_n': a_n,
                'mkt': mkt, 'pick': best_pick['pick'],
                'skey': f"{best_pick['bid']}|{val}",
                'odd': odd, 'prob': prob, 'ev': max_ev,
                'model_gap': gap,
                'aw_category': aw_category,
                'base_stake': base_stake, 'urs_score': urs_score,
                'ko': ko, 'xh': xh, 'xa': xa, 'xt': xt
            })

        # --- PORTFOLIO RISK ENGINE ---
        final_picks, port_meta = apply_portfolio_risk_engine(preliminary_picks)

        if final_picks:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            reports = [
                f"📊 <b>Portfolio Metrics (V5.8):</b>\n"
                f"Vol Proyectada: {port_meta['port_vol']*100:.2f}%\n"
                f"Damper: {port_meta['damper']:.2f}x\n"
                f"Heat Total: {port_meta['final_heat']*100:.2f}%"
            ]
            for p in final_picks:
                op_stake = p['final_stake'] if LIVE_TRADING else 0.0
                c.execute("""
                    INSERT INTO picks_log
                        (fixture_id, league, home_team, away_team, market, selection, selection_key,
                         odd_open, prob_model, ev_open, stake_pct, xg_home, xg_away, xg_total,
                         pick_time, kickoff_time, urs, model_gap)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (p['fid'], p['l_name'], p['h_n'], p['a_n'], p['mkt'], p['pick'], p['skey'],
                     p['odd'], p['prob'], p['ev'], op_stake, p['xh'], p['xa'], p['xt'],
                     datetime.now(timezone.utc).isoformat(), p['ko'], p['urs_score'], p['model_gap'])
                )
                prefix     = "🟡 [DRY-RUN]" if not LIVE_TRADING else ("💰" if op_stake > 0 else "⚠️ [SHADOW]")
                disp_stake = p['final_stake'] if not LIVE_TRADING else op_stake
                gap_str    = f"+{p['model_gap']*100:.1f}%" if p['model_gap'] >= 0 else f"{p['model_gap']*100:.1f}%"
                reports.append(
                    f"⚽ {p['h_n']} vs {p['a_n']}\n"
                    f"{prefix} {p['aw_category']}: {p['pick']}\n"
                    f"📊 Cuota: @{p['odd']} | EV: +{p['ev']*100:.1f}%\n"
                    f"📉 URS: {p['urs_score']:.2f} | LCP: {p['lcp_applied']:.2f}\n"
                    f"🔬 Gap Modelo: {gap_str}\n"
                    f"🎯 Stake Final: {disp_stake*100:.2f}%"
                )
            conn.commit(); conn.close()
            self.send_msg("\n\n".join(reports))
        else:
            self.send_msg("🔇 <b>Scan completado.</b> Sin picks válidos hoy. Morgue actualizada.")


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
        reasons = c.fetchall()
        if not reasons:
            print("  La morgue está vacía.")
        else:
            for r in reasons:
                print(f"  ❌ {r[0]}: {r[1]} rechazados")
        c.execute("SELECT match, market, odd, ev, reason, timestamp FROM decision_log ORDER BY id DESC LIMIT 10")
        for row in c.fetchall():
            match, market, odd, ev, reason, ts = row
            print(f"  [{ts[:20]}] {match} | {market} @{odd} | EV: +{ev*100:.1f}% -> {reason}")
        conn.close()
        print()
    except Exception as e:
        print(f"Error en Morgue: {e}")

    # --- AUDITOR DE CLV ---
    try:
        print("⏳ Auditoría de CLV...")
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("""
            SELECT p.home_team, p.away_team, p.selection, p.odd_open, c.odd_close,
                   ((p.odd_open - c.odd_close) / p.odd_open) * 100 AS clv_pct,
                   p.model_gap * 100 AS gap_pct
            FROM picks_log p
            JOIN closing_lines c ON p.fixture_id = c.fixture_id
                AND p.market = c.market AND p.selection_key = c.selection_key
            WHERE p.clv_captured = 1
        """)
        picks = c.fetchall(); conn.close()
        if not picks:
            print("  Sin CLVs capturados aún. Listo para acumular datos.\n")
        else:
            beats   = sum(1 for p in picks if p[5] > 0)
            avg_clv = sum(p[5] for p in picks) / len(picks)
            avg_gap = sum(p[6] for p in picks) / len(picks)
            print(f"  Total picks validados : {len(picks)}")
            print(f"  Beat Rate             : {beats}/{len(picks)} ({beats/len(picks)*100:.1f}%)")
            print(f"  CLV Promedio Global   : {avg_clv:.2f}%")
            print(f"  Model Gap Promedio    : {avg_gap:.2f}%  <- sesgo xG vs mercado\n")
    except Exception as e:
        print(f"Error en Auditoría CLV: {e}")

    bot.run_daily_scan()

    while True:
        schedule.run_pending()
        time.sleep(60)
