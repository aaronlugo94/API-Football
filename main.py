import os
import time
import requests
import schedule
import sqlite3
import numpy as np
from datetime import datetime, timedelta, timezone
from math import exp, lgamma, log

# ==========================================
# V5.6.1 QUANT FUND CONFIGURATION
# ==========================================

# --- SYSTEM FLAGS ---
LIVE_TRADING = False  # Set to True ONLY after 72h burn-in confirms realistic EVs

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

RUN_TIME_SCAN = "02:50"
RUN_TIME_INGEST = "04:00"

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

# 10 Ligas repartidas en 7 días para no exceder cuota de API (100 req/day)
LEAGUE_UPDATE_SCHEDULE = {
    0: [39, 94],  # Lunes: Premier League + Primeira Liga
    1: [140, 71], # Martes: La Liga + Eredivisie
    2: [135, 40], # Miércoles: Serie A + Championship
    3: [78],      # Jueves: Bundesliga
    4: [61],      # Viernes: Ligue 1
    5: [2],       # Sábado: Champions League
    6: [3]        # Domingo: Europa League
}

# --- RISK PARAMETERS ---
KELLY_MULTIPLIER_BY_MARKET = {
    "UNDER": 0.30,   # Varianza acotada
    "OVER":  0.20,   # Varianza abierta (NegBin fat tails)
    "DC":    0.50,   # Varianza mínima
    "1X2":   0.20    # Alta varianza
}
CLV_KILL_SWITCH_THRESHOLD = -0.015

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
        clv_captured INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT, fixture_id INTEGER, market TEXT, selection_key TEXT,
        odd_close REAL, implied_prob_close REAL, capture_time DATETIME
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS league_advanced_factors (
        league TEXT PRIMARY KEY, shots_avg REAL, shots_on_target_avg REAL, goals_per_shot REAL, 
        goals_per_sot REAL, goal_std REAL, matches INTEGER, window_days INTEGER, last_updated DATETIME
    )""")
    
    # --- AUTO-VACUNA DE BASELINES ---
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
        for league, sh, sot, gps, gsot, std in baselines:
            c.execute("""INSERT INTO league_advanced_factors 
                         (league, shots_avg, shots_on_target_avg, goals_per_shot, goals_per_sot, goal_std, matches, window_days, last_updated) 
                         VALUES (?,?,?,?,?,?, 100, 30, ?)""", (league, sh, sot, gps, gsot, std, now))
        print("✅ Auto-Seed Completado: Promedios de liga inyectados.")

    # V5.6 Migration: Split 'GOALS' into 'OVER' / 'UNDER'
    c.execute("UPDATE picks_log SET market = 'OVER' WHERE market = 'GOALS' AND selection LIKE '%Over%'")
    c.execute("UPDATE picks_log SET market = 'UNDER' WHERE market = 'GOALS' AND selection LIKE '%Under%'")
    c.execute("UPDATE closing_lines SET market = 'OVER' WHERE market = 'GOALS' AND selection_key LIKE '%Over%'")
    c.execute("UPDATE closing_lines SET market = 'UNDER' WHERE market = 'GOALS' AND selection_key LIKE '%Under%'")
    
    conn.commit()
    conn.close()

# ==========================================
# TELEMETRY & METRICS
# ==========================================

def get_avg_clv_by_market(market, lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT AVG((p.odd_open - c.odd_close)/p.odd_open) FROM picks_log p JOIN closing_lines c ON p.fixture_id=c.fixture_id AND p.market=c.market AND p.selection_key=c.selection_key WHERE p.market=? AND p.clv_captured=1 ORDER BY p.id DESC LIMIT ?", (market, lookback))
        res = c.fetchone()[0]; conn.close()
        return float(res) if res else 0.0
    except: return 0.0

def get_global_volatility(lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH); c = conn.cursor()
        c.execute("SELECT (p.odd_open - c.odd_close)/p.odd_open FROM picks_log p JOIN closing_lines c ON p.fixture_id=c.fixture_id AND p.clv_captured=1 ORDER BY p.id DESC LIMIT ?", (lookback,))
        clvs = [row[0] for row in c.fetchall()]; conn.close()
        return np.std(clvs) if len(clvs) >= 10 else 0.0
    except: return 0.0

def get_league_factors(league_name):
    conn = sqlite3.connect(DB_PATH); c = conn.cursor()
    c.execute("SELECT shots_avg, goal_std FROM league_advanced_factors WHERE league = ?", (league_name,))
    row = c.fetchone(); conn.close()
    if row: return {"pace": clamp(row[0]/24.0, 0.85, 1.20), "std": row[1]}
    return {"pace": 1.0, "std": None}

# ==========================================
# MATH & PRICING ENGINE (NEGBIN LITE)
# ==========================================

def clamp(x, low, high): return max(low, min(x, high))

def negbin_prob(mu, var, k):
    if mu <= 0: return 0.0
    if var <= mu * 1.01: return exp(k * log(mu) - mu - lgamma(k + 1)) # Poisson Fallback
    r = (mu ** 2) / (var - mu)
    if r <= 0: return exp(k * log(mu) - mu - lgamma(k + 1))
    p = r / (r + mu)
    log_pmf = lgamma(k + r) - lgamma(r) - lgamma(k + 1) + r * log(p) + k * log(1 - p)
    return exp(log_pmf)

def calc_over_under_prob_adj(xg_total, line, league_std):
    var = max(league_std ** 2, xg_total) if league_std else xg_total
    cutoff = int(np.floor(line))
    p_under = sum(negbin_prob(xg_total, var, k) for k in range(cutoff + 1))
    return 1 - p_under, p_under

def synthetic_xg_model(preds, h_inj, a_inj, league_pace):
    xh, xa = 1.4, 1.4 # Valores base seguros
    
    try:
        ghf = float(preds['teams']['home']['league']['goals']['for']['average']['home'])
        gaa = float(preds['teams']['away']['league']['goals']['against']['average']['away'])
        xh = (ghf + gaa) / 2
    except: pass
    
    try:
        gaf = float(preds['teams']['away']['league']['goals']['for']['average']['away'])
        gha = float(preds['teams']['home']['league']['goals']['against']['average']['home'])
        xa = (gaf + gha) / 2
    except: pass
    
    # Forma reciente robusta
    fh_str = preds['teams']['home'].get('league', {}).get('form', 'WWDLD')[-5:]
    fa_str = preds['teams']['away'].get('league', {}).get('form', 'WWDLD')[-5:]
    
    fh = fh_str.count('W')*3 + fh_str.count('D')
    fa = fa_str.count('W')*3 + fa_str.count('D')
    
    fh = 7 if fh == 0 else fh
    fa = 7 if fa == 0 else fa
    
    # Castigos y premios suavizados
    xh *= clamp(fh/10, 0.85, 1.15) * (1 - min(h_inj*0.015, 0.08)) * league_pace
    xa *= clamp(fa/10, 0.85, 1.15) * (1 - min(a_inj*0.015, 0.08)) * league_pace
    
    return clamp(xh, 0.5, 3.5), clamp(xa, 0.5, 3.5), xh + xa

# ==========================================
# CORE: RISK MANAGEMENT V5.6.1
# ==========================================

def calculate_final_stake(ev, odds, market, league_name):
    avg_clv = get_avg_clv_by_market(market)
    if avg_clv < CLV_KILL_SWITCH_THRESHOLD: return 0.0 # Shadow Mode
    
    base_kelly = ev / (odds - 1)
    market_mult = KELLY_MULTIPLIER_BY_MARKET.get(market, 0.20)
    vol = get_global_volatility()
    vol_penalty = clamp(1 - (vol * 2.5), 0.4, 1.0)
    liq_mult = LIQUIDITY_TIERS.get(league_name, 0.60)
    
    final_kelly = base_kelly * market_mult * vol_penalty * liq_mult
    return clamp(final_kelly, 0.0, 0.05)

# ==========================================
# ASYNC JOBS & SCANNERS
# ==========================================

class QuantFundNode:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        mode = "🔴 LIVE TRADING" if LIVE_TRADING else "🟡 DRY-RUN MODE (72h Burn-in)"
        self.send_msg(f"🛡️ <b>QUANT FUND V5.6.1 DEPLOYED</b>\nEstado: {mode}\nPesos Institucionales y Fix de xG inyectados.")

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        except: pass

    def update_league_advanced_factors(self):
        today = datetime.now(timezone.utc)
        weekday = today.weekday()
        if weekday not in LEAGUE_UPDATE_SCHEDULE: return
        
        for league_id in LEAGUE_UPDATE_SCHEDULE[weekday]:
            league_name = TARGET_LEAGUES.get(league_id)
            if not league_name: continue
            
            season = today.year if today.month >= 8 else today.year - 1
            try:
                teams = requests.get(f"https://v3.football.api-sports.io/teams?league={league_id}&season={season}", headers=self.headers, timeout=15).json().get("response", [])
            except: continue
            
            t_shots = t_sot = t_goals = 0
            match_counts = []
            
            for t in teams:
                time.sleep(1.1) # Rate limit safety
                try:
                    stats = requests.get(f"https://v3.football.api-sports.io/teams/statistics?league={league_id}&season={season}&team={t['team']['id']}", headers=self.headers, timeout=15).json().get("response")
                    if not stats: continue
                    sh, sot, gls, m = stats['shots'].get('total', 0), stats['shots'].get('on', 0), stats['goals']['for']['total'].get('total', 0), stats['fixtures']['played'].get('total', 0)
                    if not sh or not gls or m == 0: continue
                    t_shots += sh; t_sot += (sot or 0); t_goals += gls; match_counts.append(m)
                except: continue
                
            if not match_counts: continue
            
            m_total = sum(match_counts) / 2
            if m_total == 0: continue

            sh_avg, sot_avg = t_shots / m_total, t_sot / m_total
            gps, gsot = t_goals / t_shots, t_goals / max(t_sot, 1)
            std_proxy = np.sqrt(gps * sh_avg)
            
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            c.execute("""INSERT INTO league_advanced_factors (league, shots_avg, shots_on_target_avg, goals_per_shot, goals_per_sot, goal_std, matches, window_days, last_updated) 
                         VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(league) DO UPDATE SET shots_avg=excluded.shots_avg, shots_on_target_avg=excluded.shots_on_target_avg, 
                         goals_per_shot=excluded.goals_per_shot, goals_per_sot=excluded.goals_per_sot, goal_std=excluded.goal_std, last_updated=excluded.last_updated""", 
                      (league_name, round(sh_avg, 2), round(sot_avg, 2), round(gps, 4), round(gsot, 4), round(std_proxy, 3), int(m_total), 30, today.isoformat()))
            conn.commit(); conn.close()

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            now = datetime.now(timezone.utc)
            c.execute("SELECT id, fixture_id, market, selection_key, kickoff_time FROM picks_log WHERE clv_captured = 0")
            for pid, fid, mkt, skey, ko in c.fetchall():
                if (datetime.fromisoformat(ko) - now).total_seconds() / 60.0 <= 60.0:
                    res = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json()
                    found = False
                    if res.get('response'):
                        for b in res['response'][0]['bookmakers'][0]['bets']:
                            for v in b['values']:
                                if f"{b['id']}|{round(float(v['odd']),2)}" == skey:
                                    c.execute("INSERT INTO closing_lines (fixture_id, market, selection_key, odd_close, implied_prob_close, capture_time) VALUES (?,?,?,?,?,?)", 
                                             (fid, mkt, skey, float(v['odd']), 1/float(v['odd']), now.isoformat()))
                                    found = True; break
                    c.execute("UPDATE picks_log SET clv_captured = ? WHERE id = ?", (1 if found else -1, pid))
            conn.commit(); conn.close()
        except: pass

    def run_daily_scan(self):
        reports = []
        today, tomorrow = datetime.now().strftime("%Y-%m-%d"), (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        matches = []
        for d in [today, tomorrow]:
            try:
                res = requests.get(f"https://v3.football.api-sports.io/fixtures?date={d}", headers=self.headers).json()
                matches.extend([f for f in res.get('response', []) if f['league']['id'] in TARGET_LEAGUES])
            except: pass
        
        for m in matches[:15]:
            fid, h_n, a_n = m['fixture']['id'], m['teams']['home']['name'], m['teams']['away']['name']
            l_name, ko = TARGET_LEAGUES[m['league']['id']], m['fixture']['date']
            
            time.sleep(6.1)
            try:
                odds_res = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json().get('response', [])
                preds_res = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}", headers=self.headers).json().get('response', [])
                inj_res = requests.get(f"https://v3.football.api-sports.io/injuries?fixture={fid}", headers=self.headers).json().get('response', [])
            except: continue

            if not odds_res or not preds_res: continue
            bets, preds = odds_res[0]['bookmakers'][0]['bets'], preds_res[0]
            hinj = sum(1 for i in inj_res if i['team']['id'] == m['teams']['home']['id'])
            ainj = sum(1 for i in inj_res if i['team']['id'] == m['teams']['away']['id'])

            factors = get_league_factors(l_name)
            xh, xa, xt = synthetic_xg_model(preds, hinj, ainj, factors['pace'])
            
            # --- PESOS INSTITUCIONALES ---
            c_api = 0.15  # Modelo de la API
            c_mkt = 0.85  # Mercado De-vigged
            m_probs = []

            for b in bets:
                if b['id'] == 1:
                    for v in b['values']:
                        name = f"Gana {h_n}" if v['value'] == 'Home' else f"Gana {a_n}" if v['value'] == 'Away' else "Empate"
                        try:
                            p_api = float(preds['predictions']['percent'][v['value'].lower()].replace('%',''))/100
                        except: p_api = 0.33
                        
                        fp = (p_api * c_api) + ((1/float(v['odd'])/1.05) * c_mkt)
                        m_probs.append({"mkt": "1X2", "pick": name, "odd": float(v['odd']), "prob": fp, "bid": b['id']})
                elif b['id'] == 5:
                    for v in b['values']:
                        if v['value'] in ['Over 2.5', 'Under 2.5']:
                            po, pu = calc_over_under_prob_adj(xt, 2.5, factors['std'])
                            mkt_type = "OVER" if 'Over' in v['value'] else "UNDER"
                            p_mod = po if mkt_type == "OVER" else pu
                            
                            fp = (p_mod * 0.35) + ((1/float(v['odd'])/1.07) * 0.65)
                            m_probs.append({"mkt": mkt_type, "pick": f"{v['value']} Goles", "odd": float(v['odd']), "prob": fp, "bid": b['id']})

            best_pick = None; max_ev = -1.0
            for item in m_probs:
                ev = (item['prob'] * item['odd']) - 1
                if ev > max_ev: max_ev, best_pick = ev, item

            # Filtro robusto: Solo operar con EVs realistas (> 2% y < 20%)
            if best_pick and 0.02 <= max_ev <= 0.20:
                calc_stake = calculate_final_stake(max_ev, best_pick['odd'], best_pick['mkt'], l_name)
                op_stake = calc_stake if LIVE_TRADING else 0.0 # DRY RUN OVERRIDE
                
                skey = f"{best_pick['bid']}|{round(best_pick['odd'],2)}"
                conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                c.execute("""INSERT INTO picks_log (fixture_id, league, home_team, away_team, market, selection, selection_key, odd_open, prob_model, ev_open, stake_pct, xg_home, xg_away, xg_total, pick_time, kickoff_time) 
                             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (fid, l_name, h_n, a_n, best_pick['mkt'], best_pick['pick'], skey, best_pick['odd'], best_pick['prob'], max_ev, op_stake, xh, xa, xt, datetime.now(timezone.utc).isoformat(), ko))
                conn.commit(); conn.close()

                prefix = "🟡 [DRY-RUN]" if not LIVE_TRADING else ("💎" if op_stake > 0 else "⚠️ [SHADOW]")
                disp_stake = calc_stake if not LIVE_TRADING else op_stake
                reports.append(f"⚽ {h_n} vs {a_n}\n{prefix} {best_pick['pick']} | @{best_pick['odd']} | EV: +{max_ev*100:.1f}%\nTarget Stake: {disp_stake*100:.2f}%")

        if reports: self.send_msg("\n\n".join(reports))

if __name__ == "__main__":
    bot = QuantFundNode()
    schedule.every().day.at(RUN_TIME_SCAN).do(bot.run_daily_scan)
    schedule.every().day.at(RUN_TIME_INGEST).do(bot.update_league_advanced_factors)
    schedule.every(30).minutes.do(bot.capture_closing_lines)
    
    # Check baseline injection and run a test scan on boot
    bot.run_daily_scan()
    
    while True:
        schedule.run_pending()
        time.sleep(60)
