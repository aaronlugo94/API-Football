# ============================================================
# MÓDULO: BURN-IN EVALUATOR — Criterio de Salida a Producción
# Versión: 1.0 | Compatible con quant_v5.db
# ============================================================
#
# HIPÓTESIS NULA (H0): El CLV promedio del modelo es <= 0.
#   Si rechazamos H0 con p < 0.10, hay evidencia estadística
#   de que el modelo tiene edge real contra el mercado.
#
# CRITERIO DE ACTIVACIÓN (todos deben cumplirse simultáneamente):
#   1. N >= 30 picks con CLV capturado válido
#   2. CLV promedio > +1.5%
#   3. p-value < 0.10 (t-test de una muestra, cola derecha)
#   4. Sin kill-switch activo en ningún mercado
#
# USO:
#   result = evaluate_burn_in(DB_PATH)
#   if result['ready_for_live']:
#       # activar LIVE_TRADING
# ============================================================

import sqlite3
import numpy as np
from scipy import stats
from datetime import datetime, timezone


# ── CONSTANTES ───────────────────────────────────────────────
MIN_SAMPLE        = 30      # picks mínimos con CLV válido
MIN_CLV_MEAN      = 0.015   # +1.5% mínimo
MAX_P_VALUE       = 0.10    # significancia estadística
MIN_BEAT_RATE     = 0.52    # al menos 52% de picks baten la línea de cierre


def get_clv_sample(db_path):
    """
    Extrae la muestra de CLVs válidos de la base de datos.
    
    CLV = (odd_open - odd_close) / odd_open
    
    Positivo = abrimos mejor que el cierre (capturamos valor).
    Negativo = la línea se movió en nuestra contra.
    
    Solo incluye picks donde clv_captured = 1 (captura exitosa),
    excluyendo explícitamente los corruptos (-1) y los pendientes (0).
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("""
        SELECT 
            p.id,
            p.market,
            p.league,
            p.home_team || ' vs ' || p.away_team AS match,
            p.odd_open,
            c.odd_close,
            (p.odd_open - c.odd_close) / p.odd_open AS clv,
            p.ev_open,
            p.urs,
            p.pick_time
        FROM picks_log p
        JOIN closing_lines c 
            ON p.fixture_id = c.fixture_id 
            AND p.market = c.market 
            AND p.selection_key = c.selection_key
        WHERE p.clv_captured = 1
        ORDER BY p.id ASC
    """)
    rows = c.fetchall()
    conn.close()
    return rows


def evaluate_burn_in(db_path):
    """
    Evalúa si el sistema ha superado el burn-in estadístico.
    
    Returns dict con:
        ready_for_live  bool    True solo si TODOS los criterios se cumplen
        n               int     Tamaño de muestra actual
        clv_mean        float   CLV promedio (como decimal, ej. 0.023 = +2.3%)
        clv_std         float   Desviación estándar del CLV
        t_stat          float   Estadístico t
        p_value         float   p-value (cola derecha: H1: CLV_mean > 0)
        beat_rate       float   % de picks que batieron la línea de cierre
        criteria        dict    Estado de cada criterio individualmente
        market_breakdown dict   CLV promedio por mercado
        warnings        list    Alertas que no bloquean pero son relevantes
        message         str     Diagnóstico legible para el operador
    """
    result = {
        'ready_for_live': False,
        'n': 0,
        'clv_mean': None,
        'clv_std': None,
        't_stat': None,
        'p_value': None,
        'beat_rate': None,
        'criteria': {
            'sample_size': False,
            'clv_magnitude': False,
            'statistical_significance': False,
            'beat_rate': False,
        },
        'market_breakdown': {},
        'warnings': [],
        'message': '',
        'evaluated_at': datetime.now(timezone.utc).isoformat()
    }

    rows = get_clv_sample(db_path)
    n = len(rows)
    result['n'] = n

    # ── CASO: MUESTRA INSUFICIENTE ───────────────────────────
    if n < MIN_SAMPLE:
        remaining = MIN_SAMPLE - n
        result['message'] = (
            f"BURN-IN EN PROGRESO: {n}/{MIN_SAMPLE} picks con CLV válido. "
            f"Faltan {remaining} picks para poder evaluar significancia estadística."
        )
        return result

    # ── CÁLCULO DEL CLV ──────────────────────────────────────
    clvs = np.array([row[6] for row in rows], dtype=float)
    clv_mean = float(np.mean(clvs))
    clv_std  = float(np.std(clvs, ddof=1))
    beat_rate = float(np.mean(clvs > 0))

    result['clv_mean']  = clv_mean
    result['clv_std']   = clv_std
    result['beat_rate'] = beat_rate

    # ── T-TEST DE UNA MUESTRA (COLA DERECHA) ────────────────
    # H0: mu_clv <= 0   vs   H1: mu_clv > 0
    t_stat, p_two_tailed = stats.ttest_1samp(clvs, popmean=0)
    p_value = p_two_tailed / 2 if t_stat > 0 else 1.0 - p_two_tailed / 2

    result['t_stat']  = float(t_stat)
    result['p_value'] = float(p_value)

    # ── INTERVALO DE CONFIANZA AL 90% ───────────────────────
    se = clv_std / np.sqrt(n)
    ci_low  = clv_mean - 1.645 * se
    ci_high = clv_mean + 1.645 * se
    result['ci_90'] = (round(ci_low, 4), round(ci_high, 4))

    # ── DESGLOSE POR MERCADO ─────────────────────────────────
    market_data = {}
    for row in rows:
        mkt = row[1]
        if mkt not in market_data:
            market_data[mkt] = []
        market_data[mkt].append(row[6])

    for mkt, vals in market_data.items():
        result['market_breakdown'][mkt] = {
            'n': len(vals),
            'clv_mean': round(float(np.mean(vals)), 4),
            'beat_rate': round(float(np.mean(np.array(vals) > 0)), 3)
        }

    # ── EVALUACIÓN DE CRITERIOS ──────────────────────────────
    c = result['criteria']
    c['sample_size']              = n >= MIN_SAMPLE
    c['clv_magnitude']            = clv_mean >= MIN_CLV_MEAN
    c['statistical_significance'] = p_value < MAX_P_VALUE
    c['beat_rate']                = beat_rate >= MIN_BEAT_RATE

    result['ready_for_live'] = all(c.values())

    # ── ADVERTENCIAS NO BLOQUEANTES ──────────────────────────
    warnings = []

    if clv_std > 0.12:
        warnings.append(
            f"ALTA DISPERSIÓN: std={clv_std:.3f}. "
            "El modelo es inconsistente entre picks. Revisar segmentación por mercado."
        )

    if n > 0:
        recent_clvs = np.array([row[6] for row in rows[-10:]])
        if len(recent_clvs) >= 5 and float(np.mean(recent_clvs)) < clv_mean * 0.5:
            warnings.append(
                "DEGRADACIÓN RECIENTE: Los últimos 10 picks tienen CLV "
                f"promedio de {np.mean(recent_clvs)*100:.1f}%, "
                f"vs media global de {clv_mean*100:.1f}%. "
                "Posible deterioro del edge."
            )

    over_under_mkts = [m for m in result['market_breakdown'] if m in ('OVER', 'UNDER')]
    for mkt in over_under_mkts:
        if result['market_breakdown'][mkt]['clv_mean'] < -0.01:
            warnings.append(
                f"KILL-SWITCH CERCANO: Mercado {mkt} con CLV promedio "
                f"{result['market_breakdown'][mkt]['clv_mean']*100:.1f}%. "
                "Si cae por debajo de -1.5%, se activará el kill-switch automático."
            )

    result['warnings'] = warnings

    # ── MENSAJE DE DIAGNÓSTICO ───────────────────────────────
    if result['ready_for_live']:
        result['message'] = (
            f"✅ BURN-IN SUPERADO. SISTEMA LISTO PARA LIVE TRADING.\n"
            f"   N={n} | CLV={clv_mean*100:.2f}% | p={p_value:.4f} | "
            f"Beat Rate={beat_rate*100:.1f}%\n"
            f"   IC 90%: [{ci_low*100:.2f}%, {ci_high*100:.2f}%]\n"
            f"   Acción requerida: cambiar LIVE_TRADING=True y reiniciar."
        )
    else:
        failed = [k for k, v in c.items() if not v]
        details = {
            'sample_size':              f"N={n} (mínimo {MIN_SAMPLE})",
            'clv_magnitude':            f"CLV={clv_mean*100:.2f}% (mínimo +{MIN_CLV_MEAN*100:.1f}%)",
            'statistical_significance': f"p={p_value:.4f} (máximo {MAX_P_VALUE})",
            'beat_rate':                f"Beat Rate={beat_rate*100:.1f}% (mínimo {MIN_BEAT_RATE*100:.0f}%)"
        }
        failed_str = " | ".join(details[f] for f in failed)
        result['message'] = (
            f"❌ BURN-IN INCOMPLETO. Criterios fallidos: {failed_str}. "
            f"Mantener LIVE_TRADING=False."
        )

    return result


def print_burn_in_report(db_path):
    """
    Imprime un reporte legible en consola.
    Llamar desde el bloque __main__ o desde el auditor de arranque.
    """
    r = evaluate_burn_in(db_path)

    print("\n" + "="*60)
    print("   BURN-IN EVALUATOR — CRITERIO DE SALIDA A PRODUCCIÓN")
    print("="*60)
    print(f"\n{r['message']}\n")

    if r['n'] >= MIN_SAMPLE:
        print(f"{'CRITERIO':<30} {'VALOR':<20} {'ESTADO'}")
        print("-"*60)
        labels = {
            'sample_size':              f"N = {r['n']}",
            'clv_magnitude':            f"CLV = {r['clv_mean']*100:.2f}%",
            'statistical_significance': f"p-value = {r['p_value']:.4f}",
            'beat_rate':                f"Beat Rate = {r['beat_rate']*100:.1f}%"
        }
        for k, label in labels.items():
            status = "✅ OK" if r['criteria'][k] else "❌ FALLA"
            print(f"  {k:<28} {label:<20} {status}")

        print(f"\n  IC 90%: [{r['ci_90'][0]*100:.2f}%, {r['ci_90'][1]*100:.2f}%]")

        if r['market_breakdown']:
            print("\n  DESGLOSE POR MERCADO:")
            for mkt, data in r['market_breakdown'].items():
                print(f"    {mkt:<10} N={data['n']:<4} "
                      f"CLV={data['clv_mean']*100:.2f}%  "
                      f"Beat={data['beat_rate']*100:.0f}%")

        if r['warnings']:
            print("\n  ⚠️  ADVERTENCIAS:")
            for w in r['warnings']:
                print(f"    → {w}")

    print("="*60 + "\n")
    return r


# ── INTEGRACIÓN EN main.py ───────────────────────────────────
# Añadir al bloque de arranque, después de init_db():
#
#   from burn_in_evaluator import print_burn_in_report
#   burn_in_result = print_burn_in_report(DB_PATH)
#   if burn_in_result['ready_for_live'] and not LIVE_TRADING:
#       bot.send_msg("🟢 BURN-IN SUPERADO. Revisar y activar LIVE_TRADING manualmente.")
#
# NOTA IMPORTANTE:
#   El módulo NO activa LIVE_TRADING automáticamente.
#   La decisión final es siempre del operador humano.
#   El sistema solo alerta cuando los criterios se cumplen.
# ─────────────────────────────────────────────────────────────


if __name__ == "__main__":
    import sys
    db = sys.argv[1] if len(sys.argv) > 1 else "./data/quant_v5.db"
    print_burn_in_report(db)
