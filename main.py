import os
import time
import requests
import schedule
from datetime import datetime, timedelta

# --- CONFIGURACIÓN V2.0 (API-FOOTBALL ULTRA-LEAN) ---

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")  # <--- TU NUEVA API KEY

RUN_TIME = "02:50" 

# LIGAS TOP (IDs oficiales de API-FOOTBALL)
TARGET_LEAGUES = {
    39: '🇬🇧 PREMIER LEAGUE',
    140: '🇪🇸 LA LIGA',
    135: '🇮🇹 SERIE A',
    78: '🇩🇪 BUNDESLIGA',
    61: '🇫🇷 LIGUE 1',
    2: '🏆 CHAMPIONS LEAGUE',
    3: '🏆 EUROPA LEAGUE'
}

# --- DIAGNÓSTICO GEMINI ---
SDK_AVAILABLE = False
try:
    from google import genai
    from google.genai import types
    SDK_AVAILABLE = True
except ImportError: pass

class APIFootballBot:
    def __init__(self):
        self.headers = {
            'x-apisports-key': API_SPORTS_KEY
        }
        self.full_reports_buffer = []
        
        self.ai_client = None
        if SDK_AVAILABLE and GEMINI_API_KEY:
            try: self.ai_client = genai.Client(api_key=GEMINI_API_KEY)
            except: pass
            
        self.send_msg("🚀 <b>INICIANDO BOT V2.0 (API-FOOTBALL)</b>\nModo Ultra-Lean Activado. Reglas Always Win estrictas.")

    def send_msg(self, text):
        if not TELEGRAM_TOKEN: return
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
        try: requests.post(url, json=payload, timeout=10)
        except: pass

    def dec_to_am(self, decimal_odd):
        if decimal_odd <= 1.01: return "-10000"
        if decimal_odd >= 2.00: return f"+{int((decimal_odd - 1) * 100)}"
        else: return f"{int(-100 / (decimal_odd - 1))}"

    def call_gemini(self, prompt):
        if not SDK_AVAILABLE or not self.ai_client: return "❌ SDK no disponible."
        try:
            config = types.GenerateContentConfig(temperature=0.7)
            r = self.ai_client.models.generate_content(model="gemini-2.0-flash", contents=prompt, config=config)
            return r.text if r.text else "⚠️ Respuesta vacía."
        except Exception as e: return f"⚠️ Error Gemini: {e}"

    # --- LLAMADAS A LA API (AHORRANDO CUOTA) ---
    def get_fixtures_today(self, date_str):
        # 1 sola petición para ver TODO el calendario del día
        url = f"https://v3.football.api-sports.io/fixtures?date={date_str}"
        try:
            res = requests.get(url, headers=self.headers, timeout=10).json()
            return res.get('response', [])
        except: return []

    def get_odds(self, fixture_id):
        # Bookmaker 8 = Bet365 (El estándar de oro)
        url = f"https://v3.football.api-sports.io/odds?fixture={fixture_id}&bookmaker=8"
        try:
            res = requests.get(url, headers=self.headers, timeout=10).json()
            if res['results'] > 0:
                return res['response'][0]['bookmakers'][0]['bets']
            return []
        except: return []

    def get_predictions(self, fixture_id):
        url = f"https://v3.football.api-sports.io/predictions?fixture={fixture_id}"
        try:
            res = requests.get(url, headers=self.headers, timeout=10).json()
            if res['results'] > 0:
                return res['response'][0]
            return None
        except: return None

    # --- MOTOR DE REGLAS "ALWAYS WIN" ---
    def evaluate_pick(self, name, odd, prob):
        # Reglas implacables de tu estrategia
        if odd < 1.05: return None
        
        status = "VALID"
        reason = "OK"
        
        # 2. Rango PARLAY: -250 a -130 (1.40 a 1.76 decimal)
        if 1.40 <= odd < 1.60:
            if prob < 0.65: status = "REJECTED"; reason = "Prob < 65% para Parlay"
        
        # 1. Rango SIMPLE: -165 a +110 (1.60 a 2.10 decimal)
        elif 1.60 <= odd <= 2.10:
            if prob < 0.55: status = "REJECTED"; reason = "Prob < 55% para Simple"
        
        # Límites Prohibidos
        elif odd > 2.10: status = "REJECTED"; reason = "Cuota Alta (> +110)"
        elif odd < 1.40: status = "REJECTED"; reason = "Cuota Basura (< -250)"

        if status == "VALID":
            return {'pick': name, 'odd': odd, 'prob': prob}
        return None

    def run_daily_scan(self):
        self.full_reports_buffer = []
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if not API_SPORTS_KEY:
            self.send_msg("⚠️ ERROR: Falta la API_SPORTS_KEY en las variables de entorno.")
            return

        self.send_msg(f"🔎 <b>Escaneando API-FOOTBALL...</b>\nBuscando partidos para hoy: {today_str}")
        
        # 1. Traer todos los partidos de hoy (1 Request)
        all_fixtures = self.get_fixtures_today(today_str)
        
        # 2. Filtrar solo las ligas TOP
        top_matches = [f for f in all_fixtures if f['league']['id'] in TARGET_LEAGUES]
        
        if not top_matches:
            self.send_msg("🧹 Barrido completado. No hay partidos de las ligas principales hoy.")
            return
            
        self.send_msg(f"✅ Encontrados {len(top_matches)} partidos TOP. Extrayendo métricas de la API...")

        # 3. Analizar los partidos interesantes
        for match in top_matches:
            fix_id = match['fixture']['id']
            home_team = match['teams']['home']['name']
            away_team = match['teams']['away']['name']
            league_name = TARGET_LEAGUES[match['league']['id']]
            
            # Pausa de seguridad para no saturar la API (Rate Limit)
            time.sleep(1) 
            
            # --- OBTENER CUOTAS (ODDS) ---
            bets = self.get_odds(fix_id)
            if not bets: continue
            
            odds_data = {}
            for bet in bets:
                if bet['id'] == 1: # Match Winner
                    for val in bet['values']: odds_data[val['value']] = float(val['odd'])
            
            # Si no hay cuota de local, saltar
            if 'Home' not in odds_data: continue

            # --- OBTENER PREDICCIONES (PROBABILIDADES REALES) ---
            preds = self.get_predictions(fix_id)
            if not preds: continue
            
            # Extraer porcentajes de la API (vienen como "45%")
            try:
                p_home = float(preds['predictions']['percent']['home'].replace('%', '')) / 100
                p_draw = float(preds['predictions']['percent']['draw'].replace('%', '')) / 100
                p_away = float(preds['predictions']['percent']['away'].replace('%', '')) / 100
            except: continue
            
            # Probabilidades calculadas
            prob_1x = p_home + p_draw
            prob_x2 = p_away + p_draw
            
            candidates = []
            
            # Evaluar Winner Directo
            if 'Home' in odds_data:
                c = self.evaluate_pick(f"Gana {home_team}", odds_data['Home'], p_home)
                if c: candidates.append(c)
            if 'Away' in odds_data:
                c = self.evaluate_pick(f"Gana {away_team}", odds_data['Away'], p_away)
                if c: candidates.append(c)
                
            # Calcular Double Chance (1X2) matemáticamente si la API no nos dio el momio directo
            # En Bet365 la doble oportunidad suele rondar estas fórmulas aproximadas
            odd_1x = 1 / ((1/odds_data.get('Home', 1)) + (1/odds_data.get('Draw', 1))) * 0.94 if 'Home' in odds_data and 'Draw' in odds_data else 0
            odd_x2 = 1 / ((1/odds_data.get('Away', 1)) + (1/odds_data.get('Draw', 1))) * 0.94 if 'Away' in odds_data and 'Draw' in odds_data else 0
            
            c1x = self.evaluate_pick(f"{home_team} o Empate (1X)", odd_1x, prob_1x)
            if c1x: candidates.append(c1x)
            
            cx2 = self.evaluate_pick(f"{away_team} o Empate (X2)", odd_x2, prob_x2)
            if cx2: candidates.append(cx2)

            # Tomar el mejor candidato (mayor probabilidad que cumpla las reglas)
            if candidates:
                candidates.sort(key=lambda x: x['prob'], reverse=True)
                best = candidates[0]
                
                fair_odd = self.dec_to_am(1/best['prob']) if best['prob'] > 0 else "-"
                
                msg_ai = f"PARTIDO: {home_team} vs {away_team}\nLIGA: {league_name}\nSTATUS: VALID\nPICK: {best['pick']}\nPROB: {best['prob']:.2f}\nODD: {self.dec_to_am(best['odd'])}"
                self.full_reports_buffer.append(msg_ai)
                
                log_msg = (
                    f"🛡️ <b>ANÁLISIS V2.0 API</b> | {league_name}\n"
                    f"⚽ <b>{home_team} vs {away_team}</b>\n"
                    f"───────────────\n"
                    f"🎯 PICK ENCONTRADO: <b>{best['pick']}</b>\n"
                    f"⚖️ Cuota Avg: <b>{self.dec_to_am(best['odd'])}</b> ({best['odd']:.2f})\n"
                    f"🧠 Probabilidad API: <b>{best['prob']*100:.1f}%</b> (Fair: {fair_odd})\n"
                )
                self.send_msg(log_msg)

        if self.full_reports_buffer:
            self.generate_vip_summary()
        else:
            self.send_msg("🧹 Análisis de la API finalizado. Ningún pick superó los filtros 'Always Win' (Rango -250 a +110).")

    def generate_vip_summary(self):
        self.send_msg("⏳ <b>Generando Reporte VIP Oficial...</b>")
        reports_text = "\n\n".join(self.full_reports_buffer)
        
        prompt = f"""
        Actúa como un Gestor de Inversiones Deportivas. Tienes los picks filtrados directamente desde API-FOOTBALL.
        Tu misión es generar el reporte VIP copiando EXACTAMENTE los datos sin inventar matemáticas.
        
        🚨 REGLAS ESTRICTAS:
        1. COPIA la cuota (ODD) que viene en el texto.
        2. NO generes picks que no vengan en el texto.
        
        --- FORMATO VISUAL OBLIGATORIO ---
        🏆 <b>ANÁLISIS VIP V2.0 (API POWERED)</b>
        <i>Algoritmos de precisión procesando valor real.</i>
        ───────────────────

        (Repite para cada partido recibido):
        ⚽ <b>[Local] vs [Visita]</b>
        <i>[Narrativa corta]</i>
        💎 <code>[PICK]</code> @ <b>[Odd Copiada]</b> ([Prob]%)
        ───────────────────

        🎫 <b>TICKET MAESTRO DEL DÍA</b>
        1️⃣ [Mejor Pick 1]
        2️⃣ [Mejor Pick 2]
        
        {reports_text}
        """
        self.send_msg(self.call_gemini(prompt))

if __name__ == "__main__":
    bot = APIFootballBot()
    # Para probarlo INMEDIATAMENTE al hacer el deploy:
    bot.run_daily_scan() 
    
    # Loop de horario
    schedule.every().day.at(RUN_TIME).do(bot.run_daily_scan)
    while True: 
        schedule.run_pending()
        time.sleep(60)
