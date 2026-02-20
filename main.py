import os
import time
import requests
import schedule
import sqlite3
import numpy as np
from datetime import datetime, timedelta, timezone
from math import exp, lgamma

# ==========================================
# CONFIGURACIÓN V5.3
# ==========================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")

DB_DIR = os.getenv("DB_DIR", "./data")
os.makedirs(DB_DIR, exist_ok=True)
DB_PATH = os.path.join(DB_DIR, "quant_v5.db")

RUN_TIME = "06:30" 

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

# Configuración Kelly V5.3
KELLY_MULTIPLIER_BY_MARKET = {
    "GOALS": 0.25,   # Quarter Kelly
    "DC":    0.50,   # Half Kelly
    "1X2":   0.20    # 1/5 Kelly
}

CLV_KILL_SWITCH_THRESHOLD = -0.015 # -1.5%

# --- DIAGNÓSTICO GEMINI ---
SDK_AVAILABLE = False
try:
    from google import genai
    from google.genai import types
    SDK_AVAILABLE = True
except ImportError: pass

# ==========================================
# UTILIDADES MATEMÁTICAS (FIXES V5.3)
# ==========================================

def poisson_prob(lam, k):
    """Estable numéricamente usando log-gamma para evitar overflows"""
    if lam <= 0: return 0.0
    return exp(k * np.log(lam) - lam - lgamma(k + 1))

def calc_over_under_prob(xg_total, line):
    """Cálculo robusto de Over/Under con cutoff dinámico"""
    cutoff = int(np.floor(line))
    p_under = sum(poisson_prob(xg_total, k) for k in range(cutoff + 1))
    return 1 - p_under, p_under

def clamp(x, low, high): return max(low, min(x, high))

# ==========================================
# BASE DE DATOS & ANALYTICS
# ==========================================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fixture_id INTEGER, league TEXT, home_team TEXT, away_team TEXT,
        market TEXT, selection TEXT, selection_key TEXT,
        odd_open REAL, prob_model REAL, prob_api REAL, prob_mkt REAL,
        ev_open REAL, stake_pct REAL,
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
# MOTOR DE RIESGO V5.3
# ==========================================

class APIFootballQuantBot:
    def __init__(self):
        init_db()
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        self.ai_client = None
        if SDK_AVAILABLE and GEMINI_API_KEY:
            try: self.ai_client = genai.Client(api_key=GEMINI_API_KEY)
            except: pass
        self.send_msg("🛡️ <b>V5.3 SYNDICATE-PROOF ACTIVADA</b>\nKill-Switch y Kelly por Mercado operativos.")

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        try: requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
        except: pass

    def adaptive_kelly(self, ev, odds, market):
        # 1. Kill-Switch (O(1) Rolling CLV)
        avg_clv = get_avg_clv_by_market(market)
        if avg_clv < CLV_KILL_SWITCH_THRESHOLD:
            return ev, 0.0 # Shadow Mode

        # 2. Volatilidad Global
        vol = get_global_volatility()
        vol_penalty = clamp(1 - (vol * 2.5), 0.4, 1.0)

        # 3. Kelly por mercado
        base_kelly = ev / (odds - 1)
        m_mult = KELLY_MULTIPLIER_BY_MARKET.get(market, 0.25)
        
        stake = base_kelly * m_mult * vol_penalty
        return ev, clamp(stake, 0.0, 0.05)

    def capture_closing_lines(self):
        try:
            conn = sqlite3.connect(DB_PATH)
            c = conn.cursor()
            now = datetime.now(timezone.utc)
            c.execute("SELECT id, fixture_id, market, selection_key, kickoff_time FROM picks_log WHERE clv_captured = 0")
            for row in c.fetchall():
                pid, fid, mkt, skey, ko = row
                ko_dt = datetime.fromisoformat(ko)
                if (ko_dt - now).total_seconds() / 60.0 <= 60.0:
                    # Lógica de captura simplificada por matching de key
                    res = requests.get(f"https://v3.football.api-sports.io/odds?fixture={fid}&bookmaker=8", headers=self.headers).json()
                    found = False
                    if res.get('response'):
                        bets = res['response'][0]['bookmakers'][0]['bets']
                        for b in bets:
                            for v in b['values']:
                                if f"{b['id']}|{round(float(v['odd']),2)}" == skey:
                                    c.execute("INSERT INTO closing_lines VALUES (NULL,?,?,?,?,?,?)", 
                                             (fid, mkt, skey, float(v['odd']), 1/float(v['odd']), now.isoformat()))
                                    found = True; break
                    c.execute("UPDATE picks_log SET clv_captured = ? WHERE id = ?", (1 if found else -1, pid))
            conn.commit(); conn.close()
        except Exception as e: print(f"CLV Error: {e}")

    def run_daily_scan(self):
        today = datetime.now().strftime("%Y-%m-%d")
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        all_fixtures = []
        for d in [today, tomorrow]:
            res = requests.get(f"https://v3.football.api-sports.io/fixtures?date={d}", headers=self.headers).json()
            all_fixtures.extend(res.get('response', []))
        
        matches = [f for f in all_fixtures if f['league']['id'] in TARGET_LEAGUES][:15]
        reports = []

        for m in matches:
            fid = m['fixture']['id']
            # Simplificamos el flujo para este ejemplo:
            # En producción aquí llamarías a tus funciones de xG, Injuries y Odds de la V5.2
            # Simulamos un hallazgo de valor para mostrar el output:
            
            fake_pick = {
                "market": "1X2", "pick": f"Gana {m['teams']['home']['name']}", "odd": 2.10, 
                "prob": 0.55, "ev": 0.15, "skey": f"1|2.1"
            }
            
            ev, stake = self.adaptive_kelly(fake_pick['ev'], fake_pick['odd'], fake_pick['market'])
            
            # LOGGING V5.3
            conn = sqlite3.connect(DB_PATH); c = conn.cursor()
            c.execute("INSERT INTO picks_log (...) VALUES (...)", (...)) # Ver estructura de init_db
            conn.commit(); conn.close()

            status_prefix = "💎" if stake > 0 else "⚠️ [SHADOW MODE]"
            reports.append(f"⚽ {m['teams']['home']['name']} vs {m['teams']['away']['name']}\n{status_prefix} {fake_pick['pick']} | @{fake_pick['odd']} | Stake: {stake*100:.1f}%")

        self.send_msg("\n\n".join(reports))

if __name__ == "__main__":
    bot = APIFootballQuantBot()
    schedule.every().day.at(RUN_TIME).do(bot.run_daily_scan)
    schedule.every(30).minutes.do(bot.capture_closing_lines)
    
    # Test inicial
    bot.run_daily_scan()
    
    while True:
        schedule.run_pending()
        time.sleep(60)
