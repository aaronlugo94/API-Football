import os
import time
import requests
import schedule
import sqlite3
import numpy as np
from datetime import datetime, timedelta, timezone
from math import exp, lgamma

# ==========================================
# CONFIGURACIÓN V5.3 (QUANT SYNDICATE EDITION)
# ==========================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

RUN_TIME = "01:50" 

TARGET_LEAGUES = {
    39: '🇬🇧 PREMIER', 140: '🇪🇸 LA LIGA', 135: '🇮🇹 SERIE A',
    78: '🇩🇪 BUNDESLIGA', 61: '🇫🇷 LIGUE 1', 2: '🏆 CHAMPIONS', 3: '🏆 EUROPA',
    71: '🇳🇱 EREDIVISIE', 94: '🇵🇹 PRIMEIRA', 40: '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP'
}

LEAGUE_GOAL_FACTOR = {
    '🇩🇪 BUNDESLIGA': 1.12, '🇳🇱 EREDIVISIE': 1.15, '🇫🇷 LIGUE 1': 0.95,
    '🇮🇹 SERIE A': 0.92, '🇪🇸 LA LIGA': 0.94, '🇬🇧 PREMIER': 1.00,
    '🇵🇹 PRIMEIRA': 0.96, '🏴󠁧󠁢󠁥󠁮󠁧󠁿 CHAMPIONSHIP': 1.05, '🏆 CHAMPIONS': 0.98, '🏆 EUROPA': 1.00
}

KELLY_MULTIPLIER_BY_MARKET = {
    "GOALS": 0.25, "DC": 0.50, "1X2": 0.20
}

CLV_KILL_SWITCH_THRESHOLD = -0.015

SDK_AVAILABLE = False
try:
    from google import genai
    from google.genai import types
    SDK_AVAILABLE = True
except ImportError: pass

# ==========================================
# UTILIDADES MATEMÁTICAS
# ==========================================

def poisson_prob(lam, k):
    if lam <= 0: return 0.0
    return exp(k * np.log(lam) - lam - lgamma(k + 1))

def calc_over_under_prob(xg_total, line):
    cutoff = int(np.floor(line))
    p_under = sum(poisson_prob(xg_total, k) for k in range(cutoff + 1))
    return 1 - p_under, p_under

def clamp(x, low, high): return max(low, min(x, high))

# ==========================================
# BASE DE DATOS
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, league TEXT, home_team TEXT, away_team TEXT,
        market TEXT, selection TEXT, selection_key TEXT,
        odd_open REAL, prob_model REAL, ev_open REAL, stake_pct REAL,
        xg_home REAL, xg_away REAL, xg_total REAL,
        pick_time DATETIME, kickoff_time DATETIME,
        clv_captured INTEGER DEFAULT 0
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS closing_lines (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, market TEXT, selection_key TEXT,
        odd_close REAL, implied_prob_close REAL, capture_time DATETIME
    )""")
    conn.commit()
    conn.close()

def get_avg_clv_by_market(market, lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT AVG((p.odd_open - c.odd_close) / p.odd_open)
            FROM picks_log p
            JOIN closing_lines c ON p.fixture_id = c.fixture_id 
                AND p.market = c.market AND p.selection_key = c.selection_key
            WHERE p.market = ? AND p.clv_captured = 1
            ORDER BY p.id DESC LIMIT ?
        """, (market, lookback))
        res = c.fetchone()[0]
        conn.close()
        return float(res) if res is not None else 0.0
    except: return 0.0

def get_global_volatility(lookback=30):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("""
            SELECT (p.odd_open - c.odd_close) / p.odd_open
            FROM picks_log p
            JOIN closing_lines c ON p.fixture_id = c.fixture_id 
                AND p.market = c.market AND p.selection_key = c.selection_key
            WHERE p.clv_captured = 1
            ORDER BY p.id DESC LIMIT ?
        """, (lookback,))
        clvs = [row[0] for row in c.fetchall()]
        conn.close()
        return np.std(clvs) if len(clvs) >= 10 else 0.0
    except: return 0.0

# ==========================================
# MÓDULO xG SINTÉTICO
# ==========================================

def synthetic_xg_model(preds, h_inj, a_inj, league_name):
    try:
        g_h_for = float(preds['teams']['home']['goals']['for']['average']['home'])
        g_h_aga = float(preds['teams']['home']['goals']['against']['average']['home'])
        g_a_for = float(preds['teams']['away']['goals']['for']['average']['away'])
        g_a_aga = float(preds['teams']['away']['goals']['against']['average']['away'])
    except:
        g_h_for = g_h_aga = g_a_for = g_a_aga = 1.0

    xg_home = (g_h_for + g_a_aga) / 2
    xg_away = (g_a_for + g_h_aga) / 2

    form_h_pts = preds['teams']['home'].get('form', '').count('W') * 3 + preds['teams']['home'].get('form', '').count('D')
    form_a_pts = preds['teams']['away'].get('form', '').count('W') * 3 + preds['teams']['away'].get('form', '').count('D')

    xg_home *= clamp(form_h_pts / 15, 0.7, 1.2) * (1 - min(h_inj * 0.015, 0.08))
    xg_away *= clamp(form_a_pts / 15, 0.7, 1.2) * (1 - min(a_inj * 0.015, 0.08))

    lf = LEAGUE_GOAL_FACTOR.get(league_name, 1.0)
    xg_home, xg_away = clamp(xg_home * lf, 0.3, 3.5), clamp(xg_away * lf, 0.3, 3.5)
    return xg_home, xg_away, xg_home + xg_away

# ==========================================
# CLASE PRINCIPAL
# ==========================================

class APIFootballQuantBot:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        self.ai_client = None
        if SDK_AVAILABLE and GEMINI_API_KEY:
            try: self.ai_client = genai.Client(api_key=GEMINI_API_KEY)
            except: pass
        self.send_msg("🛡️ <b>V5.3 SYNDICATE-PROOF</b>\nArquitectura completa restaurada y corregida.")

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try: requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        except: pass

    def adaptive_kelly(self, ev, odds, market):
        avg_clv = get_avg_clv_by_market(market)
        if avg_clv < CLV_KILL_SWITCH_THRESHOLD: return ev, 0.0
        vol = get_global_volatility()
        vol_penalty = clamp(1 - (vol * 2.5), 0.4, 1.0)
        base_kelly = ev / (odds - 1)
        m_mult = KELLY_MULTIPLIER_BY_MARKET.get(market, 0.25)
        return ev, clamp(base_kelly * m_mult * vol_penalty, 0.0, 0.05)

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            now = datetime.now(timezone.utc)
            c.execute("SELECT id, fixture_id, market, selection_key, kickoff_time FROM picks_log WHERE clv_captured = 0")
            for row in c.fetchall():
                pid, fid, mkt, skey, ko = row
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
        self.full_reports_buffer = []
        today_str = datetime.now().strftime("%Y-%m-%d")
        tomorrow_str = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        all_fixtures = []
        for d in [today_str, tomorrow_str]:
            res = requests.get(f"https://v3.football.api-sports.io/fixtures?date={d}", headers=self.headers).json()
            all_fixtures.extend(res.get('response', []))
        
        top_matches = [f for f in all_fixtures if f['league']['id'] in TARGET_LEAGUES][:12]

        for match in top_matches:
            fid = match['fixture']['id']
            h_team = match['teams']['home']
            a_team = match['teams']['away']
            league_n = TARGET_LEAGUES[match['league']['id']]
            ko_time = match['fixture']['date']
            
            time.sleep(6.1)
            bets = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json().get('response', [])
            preds_res = requests.get(f"https://v3.football.api-sports.io/predictions?fixture={fid}", headers=self.headers).json().get('response', [])
            inj_res = requests.get(f"https://v3.football.api-sports.io/injuries?fixture={fid}", headers=self.headers).json().get('response', [])

            if not bets or not preds_res: continue
            
            bets = bets[0]['bookmakers'][0]['bets']
            preds = preds_res[0]
            h_inj = sum(1 for i in inj_res if i['team']['id'] == h_team['id'])
            a_inj = sum(1 for i in inj_res if i['team']['id'] == a_team['id'])

            xh, xa, xt = synthetic_xg_model(preds, h_inj, a_inj, league_n)
            
            # --- Lógica de Mercados ---
            market_probs = []
            conf_api = 0.45 if league_n in ['🏆 CHAMPIONS', '🇬🇧 PREMIER'] else 0.35
            conf_mkt = 1.0 - conf_api

            for b in bets:
                if b['id'] == 1: # 1X2
                    for v in b['values']:
                        name = f"Gana {h_team['name']}" if v['value'] == 'Home' else f"Gana {a_team['name']}" if v['value'] == 'Away' else "Empate"
                        p_api = float(preds['predictions']['percent'][v['value'].lower()].replace('%',''))/100
                        final_p = (p_api * conf_api) + ((1/float(v['odd'])/1.05) * conf_mkt)
                        market_probs.append({"market": "1X2", "pick": name, "odd": float(v['odd']), "prob": final_p, "bid": b['id']})
                elif b['id'] == 5: # Goals
                    for v in b['values']:
                        if v['value'] in ['Over 2.5', 'Under 2.5']:
                            p_over, p_under = calc_over_under_prob(xt, 2.5)
                            p_model = p_over if 'Over' in v['value'] else p_under
                            final_p = (p_model * 0.6) + ((1/float(v['odd'])/1.07) * 0.4)
                            market_probs.append({"market": "GOALS", "pick": f"{v['value']} Goles", "odd": float(v['odd']), "prob": final_p, "bid": b['id']})

            for item in market_probs:
                ev, stake = self.adaptive_kelly(item['prob'], item['odd'], item['market'])
                if ev < 0.02: continue
                
                s_key = f"{item['bid']}|{round(item['odd'], 2)}"
                
                # --- INSERT SEGURO ---
                conn = sqlite3.connect(DB_PATH); c = conn.cursor()
                c.execute("""INSERT INTO picks_log 
                    (fixture_id, league, home_team, away_team, market, selection, selection_key, odd_open, prob_model, ev_open, stake_pct, xg_home, xg_away, xg_total, pick_time, kickoff_time) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (fid, league_n, h_team['name'], a_team['name'], item['market'], item['pick'], s_key, item['odd'], item['prob'], ev, stake, xh, xa, xt, datetime.now(timezone.utc).isoformat(), ko_time))
                conn.commit(); conn.close()

                status = "💎" if stake > 0 else "⚠️ [SHADOW]"
                self.full_reports_buffer.append(f"⚽ {h_team['name']} vs {a_team['name']}\n{status} {item['pick']} | @{item['odd']} | Stake: {stake*100:.1f}%")

        if self.full_reports_buffer: self.send_msg("\n\n".join(self.full_reports_buffer))

if __name__ == "__main__":
    bot = APIFootballQuantBot()
    schedule.every().day.at(RUN_TIME).do(bot.run_daily_scan)
    schedule.every(30).minutes.do(bot.capture_closing_lines)
    bot.run_daily_scan()
    while True:
        schedule.run_pending()
        time.sleep(60)
