"""
Debug de rendimiento — Messenger Ads Dashboard
Ejecuta el mismo flujo de fetch_all e instrumenta cada llamada a la API.
Genera: debug_report.txt  y  debug_report.json

Uso:
    python debug_performance.py
    python debug_performance.py --token TU_TOKEN --dias 7 --max-convs 10
"""

import argparse
import json
import os
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

# ── Config ─────────────────────────────────────────────────────────────────────
PAGE_ID  = "1795816893869115"
BASE_URL = "https://graph.facebook.com/v19.0"
_token      = [""]
_page_token = [""]

def _now():
    return datetime.now(timezone.utc)

# ══════════════════════════════════════════════════════════════════════════════
#  Instrumentación
# ══════════════════════════════════════════════════════════════════════════════
_calls: list = []          # registro de cada llamada HTTP
_step_times: list = []     # registro de cada paso lógico
_start_total: float = 0.0


def _record_call(method: str, url: str, elapsed: float,
                 status: int, items: int, error: str = ""):
    _calls.append({
        "ts"     : datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "method" : method,
        "url"    : url[:90],
        "ms"     : round(elapsed * 1000),
        "status" : status,
        "items"  : items,
        "error"  : error,
    })


def _step(name: str):
    """Context manager para medir un paso lógico."""
    class _Step:
        def __init__(self, n): self.name = n; self.t0 = 0.0
        def __enter__(self):
            self.t0 = time.perf_counter()
            _print(f"  ▶ {self.name}")
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            elapsed = time.perf_counter() - self.t0
            status  = "ERROR" if exc_type else "OK"
            _step_times.append({"paso": self.name, "seg": round(elapsed, 2), "status": status})
            mark = "✗" if exc_type else "✓"
            _print(f"  {mark} {self.name} — {elapsed:.2f}s")
            return False
    return _Step(name)


def _print(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ══════════════════════════════════════════════════════════════════════════════
#  API helpers instrumentados
# ══════════════════════════════════════════════════════════════════════════════
def _tok() -> str:
    pt = _page_token[0].strip()
    if pt: return pt
    ut = _token[0].strip()
    if not ut: raise RuntimeError("Sin token.")
    return ut


def api_get(endpoint: str, params: dict = None) -> dict:
    params = params or {}
    params["access_token"] = _tok()
    url = f"{BASE_URL}/{endpoint}"
    t0  = time.perf_counter()
    r   = requests.get(url, params=params, timeout=30)
    elapsed = time.perf_counter() - t0
    data = r.json()
    error = data.get("error", {}).get("message", "") if "error" in data else ""
    _record_call("GET", url, elapsed, r.status_code, 1, error)
    if "error" in data:
        raise RuntimeError(error)
    return data


def api_paginate(endpoint: str, params: dict = None) -> list:
    results, params = [], params or {}
    params["access_token"] = _tok()
    url   = f"{BASE_URL}/{endpoint}"
    pages = 0
    while url:
        t0 = time.perf_counter()
        r  = requests.get(url, params=params, timeout=30)
        elapsed = time.perf_counter() - t0
        data  = r.json()
        error = data.get("error", {}).get("message", "") if "error" in data else ""
        batch = data.get("data", [])
        _record_call("GET", url, elapsed, r.status_code, len(batch), error)
        if "error" in data:
            raise RuntimeError(error)
        results.extend(batch)
        url    = data.get("paging", {}).get("next")
        params = {}
        pages += 1
    return results


def resolve_page_token() -> str:
    with _step("Resolver Page Token (estrategia 1: /{PAGE_ID}?fields=access_token)"):
        r = requests.get(
            f"{BASE_URL}/{PAGE_ID}",
            params={"fields": "access_token,name", "access_token": _token[0].strip()},
            timeout=30,
        )
        d = r.json()
        _record_call("GET", f"{BASE_URL}/{PAGE_ID}", 0, r.status_code, 1,
                     d.get("error", {}).get("message", "") if "error" in d else "")
        if "access_token" in d:
            _page_token[0] = d["access_token"]
            _print(f"    → Page Token OK: {d.get('name')}")
            return _page_token[0]

    with _step("Resolver Page Token (estrategia 2: /me/accounts)"):
        r2 = requests.get(
            f"{BASE_URL}/me/accounts",
            params={"access_token": _token[0].strip(), "limit": 100},
            timeout=30,
        )
        d2 = r2.json()
        _record_call("GET", f"{BASE_URL}/me/accounts", 0, r2.status_code,
                     len(d2.get("data", [])),
                     d2.get("error", {}).get("message", "") if "error" in d2 else "")
        if "error" in d2:
            raise RuntimeError(d2["error"].get("message", str(d2["error"])))
        for page in d2.get("data", []):
            if page.get("id") == PAGE_ID:
                _page_token[0] = page["access_token"]
                _print(f"    → Page Token OK vía /me/accounts")
                return _page_token[0]

    _print("    ⚠ No se encontró la página — usando token original")
    _page_token[0] = _token[0].strip()
    return _page_token[0]


# ══════════════════════════════════════════════════════════════════════════════
#  Flujo principal instrumentado
# ══════════════════════════════════════════════════════════════════════════════
def run_debug(since_days: int, max_convs: int) -> dict:
    global _start_total
    _start_total = time.perf_counter()
    since_dt  = _now() - timedelta(days=since_days)
    since_str = since_dt.strftime("%Y-%m-%d")

    _print(f"\n{'═'*60}")
    _print(f"  DEBUG RUN — período: {since_days}d · máx convs: {max_convs}")
    _print(f"  Desde: {since_str}")
    _print(f"{'═'*60}\n")

    # ── Token ─────────────────────────────────────────────────────────────────
    resolve_page_token()

    # ── Conversaciones ────────────────────────────────────────────────────────
    since_ts_filter = int(since_dt.timestamp())
    convs_raw = []
    with _step("Obtener lista de conversaciones (filtro since en API — OPTIMIZADO)"):
        convs_raw = api_paginate(
            f"{PAGE_ID}/conversations",
            params={
                "platform": "messenger",
                "fields"  : "id,updated_time,message_count,unread_count,participants",
                "limit"   : 100,
                "since"   : since_ts_filter,   # ← Facebook filtra, no descargamos todo
            },
        )
        _print(f"    → {len(convs_raw)} conversaciones en período (filtradas por API)")

    convs = convs_raw[:max_convs]
    _print(f"    → usando {len(convs)} (límite max_convs={max_convs})")

    # ── Insights ──────────────────────────────────────────────────────────────
    with _step("Obtener insights de página"):
        try:
            api_get(f"{PAGE_ID}/insights", params={
                "metric": "page_messages_total_messaging_connections,"
                          "page_messages_new_conversations_unique",
                "period": "day",
                "since": int(since_dt.timestamp()),
                "until": int(_now().timestamp()),
            })
        except RuntimeError as e:
            _print(f"    ⚠ {e}")

    # ── Campañas ──────────────────────────────────────────────────────────────
    with _step("Obtener cuentas publicitarias"):
        try:
            api_get("me/adaccounts", params={"fields": "id,name", "limit": 10})
        except RuntimeError as e:
            _print(f"    ⚠ {e}")

    # ── Mensajes por conversación ─────────────────────────────────────────────
    msg_times: list = []
    total_msgs = 0
    ad_count   = 0
    slow_convs: list = []

    _print(f"\n[PASO CRÍTICO] Obtener mensajes de {len(convs)} conversaciones…")
    for idx, conv in enumerate(convs, 1):
        conv_id     = conv["id"]
        msg_count   = conv.get("message_count", 0)
        t0          = time.perf_counter()

        try:
            msgs = api_paginate(
                f"{conv_id}/messages",
                params={"fields": "id,created_time,from,referral", "limit": 100},
            )
        except RuntimeError as e:
            _print(f"    ⚠ Conv {conv_id}: {e}")
            msgs = []

        elapsed   = time.perf_counter() - t0
        refs      = sum(1 for m in msgs if m.get("referral"))
        total_msgs += len(msgs)
        ad_count   += refs
        msg_times.append(elapsed)

        if elapsed > 2.0:
            slow_convs.append({
                "conv_id" : conv_id,
                "msgs"    : len(msgs),
                "refs"    : refs,
                "seg"     : round(elapsed, 2),
            })

        bar  = "█" * min(int(elapsed * 10), 30)
        flag = " ⚠ LENTO" if elapsed > 2.0 else ""
        _print(f"  [{idx:>3}/{len(convs)}] {elapsed:5.2f}s  {bar}  "
               f"{len(msgs):>4} msgs  {refs} refs  {msg_count} total_api{flag}")

    total_elapsed = time.perf_counter() - _start_total

    # ── Resumen estadístico ───────────────────────────────────────────────────
    avg_t  = sum(msg_times) / max(len(msg_times), 1)
    max_t  = max(msg_times, default=0)
    min_t  = min(msg_times, default=0)
    p95_t  = sorted(msg_times)[int(len(msg_times) * 0.95)] if msg_times else 0
    msgs_step_total = sum(msg_times)

    report = {
        "generado_en"    : _now().isoformat(),
        "config": {
            "since_days" : since_days,
            "max_convs"  : max_convs,
            "since_str"  : since_str,
        },
        "totales": {
            "tiempo_total_seg"        : round(total_elapsed, 2),
            "tiempo_paso_mensajes_seg": round(msgs_step_total, 2),
            "pct_tiempo_en_mensajes"  : round(msgs_step_total / max(total_elapsed, 0.01) * 100, 1),
            "conversaciones_analizadas": len(convs),
            "mensajes_obtenidos"      : total_msgs,
            "mensajes_con_referral"   : ad_count,
            "llamadas_http_totales"   : len(_calls),
        },
        "tiempos_por_conv": {
            "promedio_seg": round(avg_t, 3),
            "minimo_seg"  : round(min_t, 3),
            "maximo_seg"  : round(max_t, 3),
            "p95_seg"     : round(p95_t, 3),
            "estimado_500_convs_min": round(avg_t * 500 / 60, 1),
        },
        "convs_lentas_mas_2s": slow_convs,
        "pasos": _step_times,
        "llamadas_http": _calls,
        "recomendaciones": [],
    }

    # ── Recomendaciones automáticas ───────────────────────────────────────────
    recs = report["recomendaciones"]

    if msgs_step_total / max(total_elapsed, 0.01) > 0.8:
        recs.append({
            "problema"  : "El paso de mensajes consume >80% del tiempo total",
            "causa"     : f"Se hacen {len(convs)} llamadas HTTP secuenciales (1 por conversación)",
            "solucion"  : "Paralelizar con ThreadPoolExecutor (ver sugerencia en reporte .txt)",
            "impacto"   : "ALTO — puede reducir el tiempo hasta 5-10x",
        })

    if avg_t > 1.5:
        recs.append({
            "problema"  : f"Tiempo promedio por conversación alto ({avg_t:.2f}s)",
            "causa"     : "Conversaciones con muchos mensajes generan múltiples páginas de API",
            "solucion"  : "Agregar filtro 'since' a la paginación de mensajes para no traer histórico",
            "impacto"   : "MEDIO",
        })

    slow_api = [c for c in _calls if c["ms"] > 3000]
    if slow_api:
        recs.append({
            "problema"  : f"{len(slow_api)} llamadas HTTP tardaron más de 3 segundos",
            "causa"     : "Latencia de red o rate limiting de Facebook",
            "solucion"  : "Agregar retry con backoff exponencial",
            "impacto"   : "MEDIO",
        })

    errors = [c for c in _calls if c["error"]]
    if errors:
        recs.append({
            "problema"  : f"{len(errors)} llamadas HTTP con error",
            "causa"     : "Permisos insuficientes o tokens inválidos",
            "solucion"  : "Ver campo 'error' en llamadas_http del JSON",
            "impacto"   : "ALTO si bloquea datos clave",
        })

    if max_convs >= 200 and avg_t > 0.5:
        recs.append({
            "problema"  : f"Configuración actual ({max_convs} convs × {avg_t:.1f}s) = ~{max_convs*avg_t/60:.0f} min",
            "causa"     : "Máximo de conversaciones muy alto",
            "solucion"  : "Usar 25-50 convs para actualizaciones frecuentes, aumentar solo para análisis profundo",
            "impacto"   : "ALTO",
        })

    return report


# ══════════════════════════════════════════════════════════════════════════════
#  Generar reporte legible
# ══════════════════════════════════════════════════════════════════════════════
def write_txt_report(r: dict, path: str):
    SEP  = "═" * 65
    SEP2 = "─" * 65

    lines = [
        SEP,
        "  REPORTE DE RENDIMIENTO — Messenger Ads Dashboard",
        f"  Generado: {r['generado_en']}",
        SEP,
        "",
        "CONFIGURACIÓN",
        SEP2,
        f"  Período        : {r['config']['since_days']} días (desde {r['config']['since_str']})",
        f"  Máx. convs     : {r['config']['max_convs']}",
        "",
        "TIEMPOS TOTALES",
        SEP2,
        f"  Tiempo total               : {r['totales']['tiempo_total_seg']}s",
        f"  Tiempo en paso mensajes    : {r['totales']['tiempo_paso_mensajes_seg']}s  "
        f"({r['totales']['pct_tiempo_en_mensajes']}% del total)",
        f"  Conversaciones analizadas  : {r['totales']['conversaciones_analizadas']}",
        f"  Mensajes obtenidos         : {r['totales']['mensajes_obtenidos']}",
        f"  Mensajes con referral/ads  : {r['totales']['mensajes_con_referral']}",
        f"  Llamadas HTTP totales      : {r['totales']['llamadas_http_totales']}",
        "",
        "TIEMPOS POR CONVERSACIÓN",
        SEP2,
        f"  Promedio  : {r['tiempos_por_conv']['promedio_seg']}s",
        f"  Mínimo    : {r['tiempos_por_conv']['minimo_seg']}s",
        f"  Máximo    : {r['tiempos_por_conv']['maximo_seg']}s",
        f"  Percentil 95 : {r['tiempos_por_conv']['p95_seg']}s",
        f"  ⏱ Estimado para 500 convs : ~{r['tiempos_por_conv']['estimado_500_convs_min']} min",
        "",
    ]

    # Pasos
    lines += ["TIEMPO POR PASO LÓGICO", SEP2]
    for p in r["pasos"]:
        bar = "█" * min(int(p["seg"] * 4), 40)
        lines.append(f"  {p['seg']:6.2f}s  {bar}  {p['paso']}  [{p['status']}]")
    lines.append("")

    # Convs lentas
    if r["convs_lentas_mas_2s"]:
        lines += [f"CONVERSACIONES LENTAS (>{2}s) — {len(r['convs_lentas_mas_2s'])} encontradas", SEP2]
        for c in r["convs_lentas_mas_2s"]:
            lines.append(f"  {c['seg']}s  conv:{c['conv_id']}  msgs:{c['msgs']}  refs:{c['refs']}")
        lines.append("")

    # Llamadas HTTP
    lines += ["TOP 10 LLAMADAS HTTP MÁS LENTAS", SEP2]
    sorted_calls = sorted(r["llamadas_http"], key=lambda x: -x["ms"])[:10]
    for c in sorted_calls:
        err = f"  ⚠ {c['error'][:50]}" if c["error"] else ""
        lines.append(f"  {c['ms']:>5}ms  {c['items']:>4} items  {c['url'][:70]}{err}")
    lines.append("")

    # Errores HTTP
    errors = [c for c in r["llamadas_http"] if c["error"]]
    if errors:
        lines += [f"LLAMADAS CON ERROR — {len(errors)}", SEP2]
        for c in errors:
            lines.append(f"  [{c['ts']}] {c['url'][:60]}")
            lines.append(f"    → {c['error'][:80]}")
        lines.append("")

    # Recomendaciones
    lines += ["RECOMENDACIONES", SEP2]
    if r["recomendaciones"]:
        for i, rec in enumerate(r["recomendaciones"], 1):
            lines += [
                f"  [{i}] {rec['problema']}",
                f"      Causa    : {rec['causa']}",
                f"      Solución : {rec['solucion']}",
                f"      Impacto  : {rec['impacto']}",
                "",
            ]
    else:
        lines.append("  ✓ Sin problemas de rendimiento detectados")
        lines.append("")

    # Sugerencia de código paralelo
    lines += [
        SEP2,
        "SUGERENCIA — Paralelizar llamadas de mensajes con ThreadPoolExecutor:",
        SEP2,
        "  from concurrent.futures import ThreadPoolExecutor, as_completed",
        "",
        "  def fetch_conv_msgs(conv):",
        "      return api_paginate(f\"{conv['id']}/messages\", ...)",
        "",
        "  with ThreadPoolExecutor(max_workers=5) as pool:",
        "      futures = {pool.submit(fetch_conv_msgs, c): c for c in convs}",
        "      for future in as_completed(futures):",
        "          msgs = future.result()",
        "          # procesar msgs...",
        "",
        "  Reducción estimada: de {:.0f}s → ~{:.0f}s (workers=5)".format(
            r["totales"]["tiempo_paso_mensajes_seg"],
            r["totales"]["tiempo_paso_mensajes_seg"] / 5
        ),
        SEP,
    ]

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"\n✅  Reporte TXT guardado en '{path}'")


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="Debug de rendimiento — Messenger Ads")
    parser.add_argument("--token",     "-t", default="")
    parser.add_argument("--dias",      "-d", type=int, default=7)
    parser.add_argument("--max-convs", "-m", type=int, default=10)
    args = parser.parse_args()

    tok = args.token.strip() or os.environ.get("FB_PAGE_TOKEN", "").strip()
    if not tok:
        tok = input("Pega tu Page Access Token: ").strip()
    if not tok:
        print("ERROR: Token vacío.")
        sys.exit(1)

    _token[0] = tok

    try:
        report = run_debug(since_days=args.dias, max_convs=args.max_convs)
    except Exception as e:
        print(f"\n✗ Error crítico: {e}")
        traceback.print_exc()
        sys.exit(1)

    # Guardar JSON
    with open("debug_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print("✅  Reporte JSON guardado en 'debug_report.json'")

    # Guardar TXT
    write_txt_report(report, "debug_report.txt")

    # Imprimir resumen en consola
    t = report["totales"]
    tc = report["tiempos_por_conv"]
    print(f"\n{'─'*50}")
    print(f"  Tiempo total        : {t['tiempo_total_seg']}s")
    print(f"  % en mensajes       : {t['pct_tiempo_en_mensajes']}%")
    print(f"  Llamadas HTTP       : {t['llamadas_http_totales']}")
    print(f"  Tiempo prom/conv    : {tc['promedio_seg']}s")
    print(f"  Estimado 500 convs  : ~{tc['estimado_500_convs_min']} min")
    print(f"  Recomendaciones     : {len(report['recomendaciones'])}")
    print(f"{'─'*50}\n")
    for rec in report["recomendaciones"]:
        print(f"  ⚠ [{rec['impacto']}] {rec['problema']}")
        print(f"    → {rec['solucion']}")


if __name__ == "__main__":
    main()
