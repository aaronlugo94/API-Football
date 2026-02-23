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
# V5.7 QUANT FUND CONFIGURATION (PORTFOLIO ENGINE)
# ==========================================

LIVE_TRADING = False  # SHADOW MODE: 7-10 days burn-in required

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

RUN_TIME_SCAN = "02:50"
RUN_TIME_INGEST = "04:00"

# --- V5.7 PORTFOLIO RISK PARAMETERS ---
MAX_DAILY_HEAT = 0.10
TARGET_DAILY_VOLATILITY = 0.05

VOLATILITY_BUCKETS = {
    "OVER": 0.85,
    "UNDER": 0.85,
    "BTTS": 1.00,
    "1X2": 1.25
}

# --- LEAGUES & LIQUIDITY ---
TARGET_LEAGUES = {
    39: '🇬🇧 PREMIER', 140: '🇪🇸 LA LIGA', 135: '🇮🇹 SERIE A',
    78: '🇩🇪 BUNDESLIGA', 61: '🇫🇷 LIGUE 1', 2: '🏆 CHAMPIONS', 3: '🏆 EUROPA',
    71: '🇳🇱 EREDIVISIE', 94: '🇵🇹 PRIMEIRA', 40: '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP'
}

LIQUIDITY_TIERS = {
    '🇬🇧 PREMIER': 1.00, '🏆 CHAMPIONS': 1.00, '🇪🇸 LA LIGA': 1.00, '🇮🇹 SERIE A': 1.00, '🇩🇪 BUNDESLIGA': 1.00,
    '🏆 EUROPA': 0.85, '🇫🇷 LIGUE 1': 0.85, '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP': 0.85,
    '🇳🇱 EREDIVISIE': 0.75, '🇵🇹 PRIMEIRA': 0.75
}

LEAGUE_UPDATE_SCHEDULE = {0: [39, 94], 1: [140, 71], 2: [135, 40], 3: [78], 4: [61], 5: [2], 6: [3]}

# ==========================================
# DATABASE & SCHEMA EVOLUTION
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, league TEXT, home_team TEXT, away_team TEXT,
        market TEXT, selection TEXT, selection_key TEXT, odd_open REAL, prob_model REAL, ev_open REAL, 
        stake_pct REAL, xg_home REAL, xg_away REAL, xg_total REAL, pick_time DATETIME, kickoff_time DATETIME, 
        clv_captured INTEGER DEFAULT 0, urs REAL DEFAULT 0.0
    )""")
    try: c.execute("ALTER TABLE picks_log ADD COLUMN urs REAL DEFAULT 0.0")
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
        for l, sh, sot, gps, gsot, std in baselines:
            c.execute("INSERT INTO league_advanced_factors VALUES (?,?,?,?,?,?,?,?,?)", (l, sh, sot, gps, gsot, std, 100, 30, now))
    conn.commit(); conn.close()

def log_rejection(fixture_id, match, market, odd, ev, reason):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("INSERT INTO decision_log (fixture_id, match, market, odd, ev, reason, timestamp) VALUES (?,?,?,?,?,?,?)", 
                  (fixture_id, match, market, odd, ev, reason, datetime.now(timezone.utc).isoformat()))
        conn.commit(); conn.close()
    except: pass

# ==========================================
# PREDICTIVE METRICS & URS ENGINE
# ==========================================

def get_avg_clv_by_market(market, lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT AVG((p.odd_open - c.odd_close)/p.odd_open) FROM picks_log p JOIN closing_lines c ON p.fixture_id=c.fixture_id AND p.market=c.market AND p.selection_key=c.selection_key WHERE p.market=? AND p.clv_captured=1 ORDER BY p.id DESC LIMIT ?", (market, lookback))
        res = c.fetchone()[0]; conn.close()
        return float(res) if res else 0.0
    except: return 0.0

def get_clv_sharpe():
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT (p.odd_open - c.odd_close)/p.odd_open FROM picks_log p JOIN closing_lines c ON p.fixture_id=c.fixture_id AND p.clv_captured=1 ORDER BY p.id DESC LIMIT 50")
        clvs = [row[0] for row in c.fetchall()]; conn.close()
        if len(clvs) < 10: return 0.0
        mean_clv, std_clv = np.mean(clvs), np.std(clvs, ddof=1)
        return mean_clv / std_clv if std_clv != 0 else 0.0
    except: return 0.0

def score_sharpe(sharpe):
    if sharpe < -0.5: return 0.10
    elif sharpe < 0.0: return 0.30
    elif sharpe < 0.5: return 0.50
    elif sharpe < 1.0: return 0.75
    elif sharpe < 1.5: return 0.90
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
    urs = (w["sharpe"] * score_sharpe(sharpe)) + (w["ev_gcs"] * score_ev_gcs(ev)) + (w["liquidity"] * LIQUIDITY_TIERS.get(league_name, 0.40)) + (w["market"] * score_market_always_win(odd))
    return max(0.10, min(urs, 1.00))

def get_base_kelly_and_urs(ev, odds, market, league_name):
    avg_clv = get_avg_clv_by_market(market)
    
    # Histeresis del Kill-Switch (Schmitt Trigger)
    if avg_clv < -0.015: return 0.0, 0.0, "KILL_SWITCH_ACTIVE"
    
    base_kelly = max(0.0, min(ev / (odds - 1), 0.05))
    if -0.015 <= avg_clv < 0.005: base_kelly *= 0.25 # Zona de cuarentena
    
    urs = calculate_unified_risk_score(get_clv_sharpe(), ev, league_name, odds)
    return base_kelly * urs, urs, None

# ==========================================
# PORTFOLIO RISK ENGINE V5.7
# ==========================================

def apply_portfolio_risk_engine(preliminary_picks):
    if not preliminary_picks: return [], {}

    league_counts = {}
    for p in preliminary_picks: league_counts[p['l_name']] = league_counts.get(p['l_name'], 0) + 1
    
    port_var = 0.0
    for p in preliminary_picks:
        if p['odd'] <= 1.01: # Anti-corrupt odd protector
            p['adj_stake'] = 0; p['lcp_applied'] = 0; continue
            
        lcp = 1.0 / math.sqrt(league_counts[p['l_name']])
        adj_stake = p['base_stake'] * lcp
        
        beta = VOLATILITY_BUCKETS.get(p['mkt'], 1.00)
        var_i = beta * p['prob'] * (1.0 - p['prob']) * (p['odd'] ** 2)
        port_var += (adj_stake ** 2) * var_i
        
        p['adj_stake'] = adj_stake
        p['lcp_applied'] = lcp

    port_vol = math.sqrt(port_var) if port_var > 0 else 0.0001
    damper = min(1.0, TARGET_DAILY_VOLATILITY / port_vol)

    total_heat = 0.0
    for p in preliminary_picks:
        p['final_stake'] = p['adj_stake'] * damper
        total_heat += p['final_stake']

    heat_scale = 1.0
    if total_heat > MAX_DAILY_HEAT:
        heat_scale = MAX_DAILY_HEAT / total_heat
        
    for p in preliminary_picks:
        p['final_stake'] *= heat_scale
        p['final_stake'] = max(0.001, min(p['final_stake'], 0.05)) # Final retail cap

    meta = {'port_vol': port_vol, 'port_var': port_var, 'damper': damper, 'heat_scale': heat_scale, 'final_heat': sum(p['final_stake'] for p in preliminary_picks)}
    return preliminary_picks, meta

# ==========================================
# MATH ENGINE & SCANNERS
# ==========================================

def get_league_factors(league_name):
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT shots_avg, goal_std FROM league_advanced_factors WHERE league = ?", (league_name,))
    row = c.fetchone(); conn.close()
    return {"pace": max(0.85, min(row[0]/24.0, 1.20)), "std": row[1]} if row else {"pace": 1.0, "std": None}

def synthetic_xg_model(preds, h_inj, a_inj, league_pace):
    xh, xa = 1.4, 1.4
    try: xh = (float(preds['teams']['home']['league']['goals']['for']['average']['home']) + float(preds['teams']['away']['league']['goals']['against']['average']['away'])) / 2
    except: pass
    try: xa = (float(preds['teams']['away']['league']['goals']['for']['average']['away']) + float(preds['teams']['home']['league']['goals']['against']['average']['home'])) / 2
    except: pass
    fh_str, fa_str = preds['teams']['home'].get('league', {}).get('form', 'WWDLD')[-5:], preds['teams']['away'].get('league', {}).get('form', 'WWDLD')[-5:]
    fh, fa = fh_str.count('W')*3 + fh_str.count('D'), fa_str.count('W')*3 + fa_str.count('D')
    xh *= max(0.85, min((7 if fh==0 else fh)/10, 1.15)) * (1 - min(h_inj*0.015, 0.08)) * league_pace
    xa *= max(0.85, min((7 if fa==0 else fa)/10, 1.15)) * (1 - min(a_inj*0.015, 0.08)) * league_pace
    return max(0.5, min(xh, 3.5)), max(0.5, min(xa, 3.5)), xh + xa

def negbin_prob(mu, var, k):
    if mu <= 0: return 0.0
    if var <= mu * 1.01: return exp(k * log(mu) - mu - lgamma(k + 1))
    r = (mu ** 2) / (var - mu); p = r / (r + mu)
    return exp(lgamma(k + r) - lgamma(r) - lgamma(k + 1) + r * log(p) + k * log(1 - p)) if r > 0 else exp(k * log(mu) - mu - lgamma(k + 1))

def calc_over_under_prob_adj(xg_total, line, league_std):
    var = max(league_std ** 2, xg_total) if league_std else xg_total
    p_under = sum(negbin_prob(xg_total, var, k) for k in range(int(np.floor(line)) + 1))
    return 1 - p_under, p_under

class QuantFundNode:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        mode = "🔴 LIVE TRADING" if LIVE_TRADING else "🟡 DRY-RUN MODE (Batch Burn-in)"
        self.send_msg(f"🛡️ <b>QUANT FUND V5.7 DEPLOYED</b>\nEstado: {mode}\nPortfolio Risk Engine (Batch) Activo.")

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        except: pass

    def update_league_advanced_factors(self):
        today = datetime.now(timezone.utc); weekday = today.weekday()
        if weekday not in LEAGUE_UPDATE_SCHEDULE: return
        for league_id in LEAGUE_UPDATE_SCHEDULE[weekday]:
            league_name = TARGET_LEAGUES.get(league_id)
            if not league_name: continue
            season = today.year if today.month >= 8 else today.year - 1
            try: teams = requests.get(f"https://v3.football.api-sports.io/teams?league={league_id}&season={season}", headers=self.headers, timeout=15).json().get("response", [])
            except: continue
            t_shots = t_sot = t_goals = 0; match_counts = []
            for t in teams:
                time.sleep(1.1)
                try:
                    stats = requests.get(f"https://v3.football.api-sports.io/teams/statistics?league={league_id}&season={season}&team={t['team']['id']}", headers=self.headers, timeout=15).json().get("response")
                    sh, sot, gls, m = stats['shots'].get('total', 0), stats['shots'].get('on', 0), stats['goals']['for']['total'].get('total', 0), stats['fixtures']['played'].get('total', 0)
                    if sh and gls and m > 0: t_shots += sh; t_sot += (sot or 0); t_goals += gls; match_counts.append(m)
                except: continue
            m_total = sum(match_counts) / 2
            if m_total > 0:
                sh_avg, sot_avg = t_shots / m_total, t_sot / m_total
                gps, gsot = t_goals / t_shots, t_goals / max(t_sot, 1)
                std_proxy = np.sqrt(gps * sh_avg)
                conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                c.execute("""INSERT INTO league_advanced_factors (league, shots_avg, shots_on_target_avg, goals_per_shot, goals_per_sot, goal_std, matches, window_days, last_updated) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(league) DO UPDATE SET shots_avg=excluded.shots_avg, goal_std=excluded.goal_std, last_updated=excluded.last_updated""", (league_name, round(sh_avg, 2), round(sot_avg, 2), round(gps, 4), round(gsot, 4), round(std_proxy, 3), int(m_total), 30, today.isoformat()))
                conn.commit(); conn.close()

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor(); now = datetime.now(timezone.utc)
            c.execute("SELECT id, fixture_id, market, selection_key, kickoff_time FROM picks_log WHERE clv_captured = 0")
            for pid, fid, mkt, skey, ko in c.fetchall():
                if (datetime.fromisoformat(ko) - now).total_seconds() / 60.0 <= 60.0:
                    res = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json()
                    found = False
                    if res.get('response'):
                        for b in res['response'][0]['bookmakers'][0]['bets']:
                            for v in b['values']:
                                if f"{b['id']}|{round(float(v['odd']),2)}" == skey:
                                    c.execute("INSERT INTO closing_lines (fixture_id, market, selection_key, odd_close, implied_prob_close, capture_time) VALUES (?,?,?,?,?,?)", (fid, mkt, skey, float(v['odd']), 1/float(v['odd']), now.isoformat()))
                                    found = True; break
                    c.execute("UPDATE picks_log SET clv_captured = ? WHERE id = ?", (1 if found else -1, pid))
            conn.commit(); conn.close()
        except: pass

    def run_daily_scan(self):
        today, tomorrow = datetime.now().strftime("%Y-%m-%d"), (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        matches = []
        for d in [today, tomorrow]:
            try: matches.extend([f for f in requests.get(f"https://v3.football.api-sports.io/fixtures?date={d}", headers=self.headers).json().get('response', []) if f['league']['id'] in TARGET_LEAGUES])
            except: pass
        
        preliminary_picks = []

        for m in matches[:15]:
            fid, h_n, a_n, l_name, ko = m['fixture']['id'], m['teams']['home']['name'], m['teams']['away']['name'], TARGET_LEAGUES[m['league']['id']], m['fixture']['date']
            time.sleep(6.1)
            match_label = f"{h_n} vs {a_n}"
            try:
                odds_res = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json().get('response', [])
                preds_res = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}", headers=self.headers).json().get('response', [])
                inj_res = requests.get(f"https://v3.football.api-sports.io/injuries?fixture={fid}", headers=self.headers).json().get('response', [])
            except: continue

            if not odds_res or not preds_res: continue
            bets, preds = odds_res[0]['bookmakers'][0]['bets'], preds_res[0]
            hinj, ainj = sum(1 for i in inj_res if i['team']['id'] == m['teams']['home']['id']), sum(1 for i in inj_res if i['team']['id'] == m['teams']['away']['id'])
            factors = get_league_factors(l_name)
            xh, xa, xt = synthetic_xg_model(preds, hinj, ainj, factors['pace'])
            
            c_api, c_mkt = 0.15, 0.85; m_probs = []
            for b in bets:
                if b['id'] == 1:
                    for v in b['values']:
                        name = f"Gana {h_n}" if v['value'] == 'Home' else f"Gana {a_n}" if v['value'] == 'Away' else "Empate"
                        try: p_api = float(preds['predictions']['percent'][v['value'].lower()].replace('%',''))/100
                        except: p_api = 0.33
                        m_probs.append({"mkt": "1X2", "pick": name, "odd": float(v['odd']), "prob": (p_api * c_api) + ((1/float(v['odd'])/1.05) * c_mkt), "bid": b['id']})
                elif b['id'] == 5:
                    po, pu = calc_over_under_prob_adj(xt, 2.5, factors['std'])
                    for v in b['values']:
                        if v['value'] in ['Over 2.5', 'Under 2.5']:
                            mkt_type = "OVER" if 'Over' in v['value'] else "UNDER"
                            m_probs.append({"mkt": mkt_type, "pick": f"{v['value']} Goles", "odd": float(v['odd']), "prob": ((po if mkt_type == "OVER" else pu) * 0.35) + ((1/float(v['odd'])/1.07) * 0.65), "bid": b['id']})

            best_pick, max_ev = None, -1.0
            for item in m_probs:
                ev = (item['prob'] * item['odd']) - 1
                if ev > max_ev: max_ev, best_pick = ev, item

            if best_pick:
                odd, prob, mkt = best_pick['odd'], best_pick['prob'], best_pick['mkt']
                if max_ev < 0.02: log_rejection(fid, match_label, mkt, odd, max_ev, "LOW_EV"); continue
                if max_ev > 0.20: log_rejection(fid, match_label, mkt, odd, max_ev, "EV_ALUCINATION"); continue
                
                aw_category = "💎 SIMPLE" if 1.60 <= odd <= 2.10 and prob > 0.55 else ("🧱 PARLAY" if 1.40 <= odd < 1.60 and prob > 0.65 else None)
                if not aw_category: log_rejection(fid, match_label, mkt, odd, max_ev, "OUT_OF_ALWAYS_WIN_RANGE"); continue
                
                base_stake, urs_score, rejection_reason = get_base_kelly_and_urs(max_ev, odd, mkt, l_name)
                if base_stake == 0.0: log_rejection(fid, match_label, mkt, odd, max_ev, rejection_reason); continue
                
                preliminary_picks.append({
                    'fid': fid, 'l_name': l_name, 'h_n': h_n, 'a_n': a_n, 'mkt': mkt, 'pick': best_pick['pick'],
                    'skey': f"{best_pick['bid']}|{round(odd,2)}", 'odd': odd, 'prob': prob, 'ev': max_ev,
                    'aw_category': aw_category, 'base_stake': base_stake, 'urs_score': urs_score,
                    'ko': ko, 'xh': xh, 'xa': xa, 'xt': xt
                })

        # --- PORTFOLIO RISK ENGINE BATCH EXECUTION ---
        final_picks, port_meta = apply_portfolio_risk_engine(preliminary_picks)
        
        if final_picks:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            reports = [f"📊 <b>Portfolio Metrics:</b>\nVol Proyectada: {port_meta['port_vol']*100:.2f}%\nDamper: {port_meta['damper']:.2f}x\nHeat Total: {port_meta['final_heat']*100:.2f}%"]
            
            for p in final_picks:
                op_stake = p['final_stake'] if LIVE_TRADING else 0.0
                c.execute("""INSERT INTO picks_log (fixture_id, league, home_team, away_team, market, selection, selection_key, odd_open, prob_model, ev_open, stake_pct, xg_home, xg_away, xg_total, pick_time, kickoff_time, urs) 
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (p['fid'], p['l_name'], p['h_n'], p['a_n'], p['mkt'], p['pick'], p['skey'], p['odd'], p['prob'], p['ev'], op_stake, p['xh'], p['xa'], p['xt'], datetime.now(timezone.utc).isoformat(), p['ko'], p['urs_score']))
                
                prefix = "🟡 [DRY-RUN]" if not LIVE_TRADING else ("💰" if op_stake > 0 else "⚠️ [SHADOW]")
                disp_stake = p['final_stake'] if not LIVE_TRADING else op_stake
                reports.append(f"⚽ {p['h_n']} vs {p['a_n']}\n{prefix} {p['aw_category']}: {p['pick']}\n📊 Cuota: @{p['odd']} | EV: +{p['ev']*100:.1f}%\n📉 URS: {p['urs_score']:.2f} | LCP: {p['lcp_applied']:.2f}\n🎯 Stake Final: {disp_stake*100:.2f}%")
            
            conn.commit(); conn.close()
            self.send_msg("\n\n".join(reports))

if __name__ == "__main__":
    bot = QuantFundNode()
    schedule.every().day.at(RUN_TIME_SCAN).do(bot.run_daily_scan)
    schedule.every().day.at(RUN_TIME_INGEST).do(bot.update_league_advanced_factors)
    schedule.every(30).minutes.do(bot.capture_closing_lines)
    
    # --- SCRIPT DE AUDITORÍA AUTOMÁTICA EN LOGS ---
    try:
        print("\n⏳ Iniciando Auditoría de CLV en la Base de Datos...")
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT p.home_team, p.away_team, p.selection, p.odd_open, c.odd_close, 
                   ((p.odd_open - c.odd_close) / p.odd_open) * 100 AS clv_pct
            FROM picks_log p JOIN closing_lines c ON p.fixture_id = c.fixture_id 
                 AND p.market = c.market AND p.selection_key = c.selection_key
            WHERE p.clv_captured = 1
        """)
        picks = c.fetchall()
        conn.close()
        
        if not picks:
            print("⚠️ Aún no hay líneas de cierre capturadas. Faltan datos o partidos por jugar.")
        else:
            beats = sum(1 for pick in picks if pick[5] > 0)
            avg_clv = sum(pick[5] for pick in picks) / len(picks)
            print("\n📊 --- AUDITORÍA DE CLV (DRY-RUN 72H) --- 📊")
            print(f"Total de picks validados: {len(picks)}")
            print(f"Líneas de cierre ganadas (Beats): {beats} ({beats/len(picks)*100:.1f}%)")
            print(f"CLV Promedio Global: {avg_clv:.2f}%\n")
            print("🔍 DETALLE DE LOS ÚLTIMOS PICKS:")
            for p in picks[-10:]:
                trend = "✅ BEAT" if p[5] > 0 else ("❌ LOST" if p[5] < 0 else "➖ EMPATE")
                print(f"{p[0]} vs {p[1]} | {p[2]} | Apertura: @{p[3]} -> Cierre: @{p[4]} | CLV: {p[5]:.2f}% [{trend}]")
        print("--------------------------------------------------\n")
    except Exception as e:
        print(f"Error en Auditoría: {e}")
    # ----------------------------------------------

    # Ejecuta un escaneo de prueba al iniciar y luego entra en bucle
    bot.run_daily_scan()
    
    while True:
        schedule.run_pending()
        time.sleep(60)
