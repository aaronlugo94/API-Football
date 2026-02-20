import os
import time
import requests
import schedule
from datetime import datetime

# --- CONFIGURACIÓN V2.2 (ULTRA-LEAN + BTTS + DNB) ---

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
API_SPORTS_KEY = os.getenv("API_SPORTS_KEY", "")

RUN_TIME = "02:50" 

# LIGAS TOP (IDs oficiales de API-FOOTBALL)
TARGET_LEAGUES = {
    39: '🇬🇧 PREMIER LEAGUE', 140: '🇪🇸 LA LIGA', 135: '🇮🇹 SERIE A',
    78: '🇩🇪 BUNDESLIGA', 61: '🇫🇷 LIGUE 1', 2: '🏆 CHAMPIONS LEAGUE', 3: '🏆 EUROPA LEAGUE'
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
        self.headers = {'x-apisports-key': API_SPORTS_KEY}
        self.full_reports_buffer = []
        
        self.ai_client = None
        if SDK_AVAILABLE and GEMINI_API_KEY:
            try: self.ai_client = genai.Client(api_key=GEMINI_API_KEY)
            except: pass
            
        self.send_msg("🚀 <b>BOT V2.2 ACTUALIZADO (API-FOOTBALL)</b>\nModo Ultra-Lean. DNB y BTTS Activados. Reglas Always Win estrictas (-250).")

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

    # --- LLAMADAS A LA API (1 Petición = Cientos de Datos) ---
    def get_fixtures_today(self, date_str):
        url = f"https://v3.football.api-sports.io/fixtures?date={date_str}"
        try:
            res = requests.get(url, headers=self.headers, timeout=10).json()
            return res.get('response', [])
        except: return []

    def get_odds(self, fixture_id):
        # Bookmaker 8 = Bet365
        url = f"https://v3.football.api-sports.io/odds?fixture={fixture_id}&bookmaker=8"
        try:
            res = requests.get(url, headers=self.headers, timeout=10).json()
            if res['results'] > 0: return res['response'][0]['bookmakers'][0]['bets']
            return []
        except: return []

    def get_predictions(self, fixture_id):
        url = f"https://v3.football.api-sports.io/predictions?fixture={fixture_id}"
        try:
            res = requests.get(url, headers=self.headers, timeout=10).json()
            if res['results'] > 0: return res['response'][0]
            return None
        except: return None

    # --- ESCÁNER PRINCIPAL ---
    def run_daily_scan(self):
        self.full_reports_buffer = []
        today_str = datetime.now().strftime("%Y-%m-%d")
        
        if not API_SPORTS_KEY:
            self.send_msg("⚠️ ERROR: Falta la API_SPORTS_KEY en las variables de entorno.")
            return

        self.send_msg(f"🔎 <b>Escaneando Mercados de Élite...</b>\nFecha: {today_str}")
        
        # 1. Calendario del día
        all_fixtures = self.get_fixtures_today(today_str)
        top_matches = [f for f in all_fixtures if f['league']['id'] in TARGET_LEAGUES]
        
        if not top_matches:
            self.send_msg("🧹 Barrido completado. No hay partidos de las ligas principales hoy.")
            return

        # 2. Análisis de Partidos Top
        for match in top_matches:
            fix_id = match['fixture']['id']
            home_team = match['teams']['home']['name']
            away_team = match['teams']['away']['name']
            league_name = TARGET_LEAGUES[match['league']['id']]
            
            time.sleep(1) # Rate limit safety (Ahorra créditos y evita bloqueos)
            
            bets = self.get_odds(fix_id)
            preds = self.get_predictions(fix_id)
            if not bets or not preds: continue
            
            # Extraer Probabilidades de la API (Machine Learning)
            try:
                p_home = float(preds['predictions']['percent']['home'].replace('%', '')) / 100
                p_draw = float(preds['predictions']['percent']['draw'].replace('%', '')) / 100
                p_away = float(preds['predictions']['percent']['away'].replace('%', '')) / 100
            except: continue

            advice = str(preds['predictions']['advice']).lower()
            market_probs = {}
            
            # Variables para calcular DNB matemáticamente
            odd_h = odd_a = odd_d = 0

            # --- EXTRACCIÓN DE TODOS LOS MERCADOS DE BET365 ---
            for bet in bets:
                # 1. MATCH WINNER (ID 1)
                if bet['id'] == 1: 
                    for v in bet['values']:
                        name = f"Gana {home_team}" if v['value'] == 'Home' else f"Gana {away_team}" if v['value'] == 'Away' else "Empate"
                        base_p = p_home if v['value'] == 'Home' else p_away if v['value'] == 'Away' else p_draw
                        implied = (1 / float(v['odd'])) * 0.95
                        final_p = (base_p * 0.3) + (implied * 0.7) 
                        market_probs[name] = {'odd': float(v['odd']), 'prob': final_p}
                        
                        if v['value'] == 'Home': odd_h = float(v['odd'])
                        elif v['value'] == 'Away': odd_a = float(v['odd'])
                        elif v['value'] == 'Draw': odd_d = float(v['odd'])
                
                # 2. DOUBLE CHANCE (ID 12)
                elif bet['id'] == 12: 
                    for v in bet['values']:
                        if v['value'] == 'Home/Draw': name = f"{home_team} o Empate (1X)"; base_p = p_home + p_draw
                        elif v['value'] == 'Draw/Away': name = f"{away_team} o Empate (X2)"; base_p = p_away + p_draw
                        else: continue
                        implied = (1 / float(v['odd'])) * 0.95
                        final_p = (base_p * 0.3) + (implied * 0.7)
                        market_probs[name] = {'odd': float(v['odd']), 'prob': final_p}
                
                # 3. GOALS OVER/UNDER (ID 5)
                elif bet['id'] == 5: 
                    for v in bet['values']:
                        if v['value'] in ['Over 1.5', 'Over 2.5', 'Under 2.5', 'Under 3.5']:
                            implied = (1 / float(v['odd'])) * 0.94
                            bonus = 0.05 if ("over" in advice and "Over" in v['value']) or ("under" in advice and "Under" in v['value']) else 0
                            market_probs[f"{v['value']} Goles"] = {'odd': float(v['odd']), 'prob': implied + bonus}

                # 4. AMBOS ANOTAN - BTTS (ID 8)
                elif bet['id'] == 8:
                    for v in bet['values']:
                        name = f"Ambos Anotan (BTTS): SÍ" if v['value'] == 'Yes' else f"Ambos Anotan (BTTS): NO"
                        implied = (1 / float(v['odd'])) * 0.94
                        market_probs[name] = {'odd': float(v['odd']), 'prob': implied}

            # 5. EMPATE NO ACCIÓN - DNB (Cálculo Seguro)
            if odd_h > 0 and odd_a > 0 and odd_d > 0:
                dnb_h_odd = odd_h * (1 - (1 / odd_d)) * 0.95
                dnb_a_odd = odd_a * (1 - (1 / odd_d)) * 0.95
                p_dnb_h = p_home / (p_home + p_away) if (p_home + p_away) > 0 else 0
                p_dnb_a = p_away / (p_home + p_away) if (p_home + p_away) > 0 else 0
                market_probs[f"{home_team} (Empate No Acción)"] = {'odd': dnb_h_odd, 'prob': p_dnb_h}
                market_probs[f"{away_team} (Empate No Acción)"] = {'odd': dnb_a_odd, 'prob': p_dnb_a}

            # --- FILTROS DE DINERO ESTRICTOS (ALWAYS WIN) ---
            simples = []
            parlays = []
            
            for pick_name, data in market_probs.items():
                odd = data['odd']
                prob = data['prob']
                if odd < 1.05: continue
                
                # 🧱 Regla PARLAY (Rango 1.40 a 1.59 / Probabilidad >= 65%)
                if 1.40 <= odd < 1.60 and prob >= 0.65:
                    parlays.append({'pick': pick_name, 'odd': odd, 'prob': prob})
                
                # 💎 Regla SIMPLE (Rango 1.60 a 2.10 / Probabilidad >= 55%)
                elif 1.60 <= odd <= 2.10 and prob >= 0.55:
                    simples.append({'pick': pick_name, 'odd': odd, 'prob': prob})

            # Tomar los de mayor probabilidad
            simples.sort(key=lambda x: x['prob'], reverse=True)
            parlays.sort(key=lambda x: x['prob'], reverse=True)
            
            best_simple = simples[0] if simples else None
            best_parlay = parlays[0] if parlays else None
            
            if not best_simple and not best_parlay: continue

            # Preparamos textos para el reporte de Gemini
            picks_text = ""
            if best_simple:
                picks_text += f"\n💎 SIMPLE: {best_simple['pick']} | CUOTA: {self.dec_to_am(best_simple['odd'])} | PROB: {best_simple['prob']*100:.1f}%"
            if best_parlay:
                picks_text += f"\n🧱 PARLAY: {best_parlay['pick']} | CUOTA: {self.dec_to_am(best_parlay['odd'])} | PROB: {best_parlay['prob']*100:.1f}%"

            msg_ai = f"PARTIDO: {home_team} vs {away_team}\nLIGA: {league_name}{picks_text}"
            self.full_reports_buffer.append(msg_ai)

        if self.full_reports_buffer:
            self.generate_vip_summary()
        else:
            self.send_msg("🧹 Análisis finalizado. Ningún pick superó los filtros estrictos de valor y seguridad (-250) para el día de hoy.")

    def generate_vip_summary(self):
        self.send_msg("⏳ <b>Armando Reporte VIP Dual...</b>")
        reports_text = "\n\n".join(self.full_reports_buffer)
        
        prompt = f"""
        Actúa como un Tipster Profesional y Gestor de Inversiones. Tienes los picks filtrados por la API.
        
        REGLAS ESTRICTAS:
        1. COPIA los datos exactos del texto provisto (Las cuotas americanas y probabilidades). NUNCA inventes números.
        2. REDACTA una oración breve y atractiva analizando el partido (Ej: "El local busca consolidarse en la cima en un duelo cerrado").
        3. Para cada partido, muestra los picks que te paso (ya sea 💎 Simple, 🧱 Parlay, o ambos).
        
        --- FORMATO VISUAL OBLIGATORIO ---
        🏆 <b>ANÁLISIS VIP V2.2 (API POWERED)</b>
        <i>Algoritmos de élite detectando Valor y Seguridad.</i>
        ───────────────────

        (Repite para cada partido recibido):
        ⚽ <b>[Local] vs [Visita]</b>
        <i>[Aquí redacta tu línea de análisis real]</i>
        (Pon aquí la línea de 💎 SIMPLE si el texto la trae, con su Pick, Cuota Americana y Prob)
        (Pon aquí la línea de 🧱 PARLAY si el texto la trae, con su Pick, Cuota Americana y Prob)
        ───────────────────

        🎫 <b>TICKET MAESTRO DEL DÍA</b>
        (REGLA DE ORO: Arma este ticket 1️⃣ y 2️⃣ SOLAMENTE usando picks que tengan el icono 🧱 PARLAY. Combina un par de ellos. Si el texto no te dio NINGÚN pick 🧱 PARLAY, entonces escribe: "Hoy el valor está en las cuotas simples. No hay piezas de alta seguridad para armar Parlay.")
        
        DATOS A PROCESAR:
        {reports_text}
        """
        self.send_msg(self.call_gemini(prompt))

if __name__ == "__main__":
    bot = APIFootballBot()
    # Ejecuta un escaneo al arrancar
    bot.run_daily_scan() 
    
    # Loop de horario configurado
    schedule.every().day.at(RUN_TIME).do(bot.run_daily_scan)
    while True: 
        schedule.run_pending()
        time.sleep(60)
