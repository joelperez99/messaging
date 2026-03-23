"""
Messenger Ads Stats — versión CLI (sin interfaz gráfica)
Página: Crecelac | Page ID: 1795816893869115

Uso:
    python messenger_stats_cli.py
    python messenger_stats_cli.py --token TU_TOKEN --dias 30 --max-convs 50
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

# ── Configuración ──────────────────────────────────────────────────────────────
PAGE_ID  = "1795816893869115"
BASE_URL = "https://graph.facebook.com/v19.0"

_token      = [""]
_page_token = [""]


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── Helpers de API ─────────────────────────────────────────────────────────────
def _tok() -> str:
    pt = _page_token[0].strip()
    if pt:
        return pt
    ut = _token[0].strip()
    if not ut:
        raise RuntimeError("No se proporcionó Access Token.")
    return ut


def api_get(endpoint: str, params: dict = None) -> dict:
    params = params or {}
    params["access_token"] = _tok()
    r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", str(data["error"])))
    return data


def api_paginate(endpoint: str, params: dict = None) -> list:
    results, params = [], params or {}
    params["access_token"] = _tok()
    url = f"{BASE_URL}/{endpoint}"
    while url:
        r = requests.get(url, params=params, timeout=30)
        data = r.json()
        if "error" in data:
            raise RuntimeError(data["error"].get("message", str(data["error"])))
        results.extend(data.get("data", []))
        url = data.get("paging", {}).get("next")
        params = {}
    return results


def resolve_page_token() -> str:
    """Convierte User Token → Page Access Token automáticamente."""
    user_tok = _token[0].strip()
    log("Resolviendo Page Access Token…")

    # Estrategia 1: pedir el token directamente a la página
    r = requests.get(
        f"{BASE_URL}/{PAGE_ID}",
        params={"fields": "access_token,name", "access_token": user_tok},
        timeout=30,
    )
    data = r.json()
    if "access_token" in data:
        _page_token[0] = data["access_token"]
        log(f"  ✓ Page Token resuelto para '{data.get('name', PAGE_ID)}'")
        return _page_token[0]

    log(f"  Estrategia 1 sin token, probando /me/accounts…")

    # Estrategia 2: /me/accounts
    r2 = requests.get(
        f"{BASE_URL}/me/accounts",
        params={"access_token": user_tok, "limit": 100},
        timeout=30,
    )
    data2 = r2.json()
    if "error" in data2:
        raise RuntimeError(data2["error"].get("message", str(data2["error"])))

    for page in data2.get("data", []):
        if page.get("id") == PAGE_ID:
            _page_token[0] = page["access_token"]
            log(f"  ✓ Page Token resuelto vía /me/accounts")
            return _page_token[0]

    log("  ⚠ No se encontró la página — usando token original.")
    _page_token[0] = user_tok
    return user_tok


# ── Log ────────────────────────────────────────────────────────────────────────
def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ── Fetch principal ────────────────────────────────────────────────────────────
def fetch_all(since_days: int = 30, max_convs: int = 50) -> dict:
    since_dt  = _now() - timedelta(days=since_days)
    since_ts  = int(since_dt.timestamp())
    until_ts  = int(_now().timestamp())
    since_str = since_dt.strftime("%Y-%m-%d")

    resolve_page_token()

    # ── Conversaciones ────────────────────────────────────────────────────────
    log(f"Obteniendo conversaciones desde {since_str} (máx. {max_convs})…")
    convs_raw = api_paginate(
        f"{PAGE_ID}/conversations",
        params={
            "platform": "messenger",
            "fields": "id,updated_time,message_count,unread_count,participants",
            "limit": 100,
        },
    )
    convs = [c for c in convs_raw if c.get("updated_time", "")[:10] >= since_str][:max_convs]
    log(f"  → {len(convs_raw)} totales · {len(convs)} en el período")

    # ── Insights ──────────────────────────────────────────────────────────────
    log(f"Obteniendo insights ({since_days} días)…")
    metrics = [
        "page_messages_total_messaging_connections",
        "page_messages_new_conversations_unique",
        "page_messages_blocked_conversations_unique",
        "page_messages_reported_conversations_unique",
        "page_response_time_median",
    ]
    insights = {}
    try:
        ins_raw = api_get(
            f"{PAGE_ID}/insights",
            params={"metric": ",".join(metrics), "period": "day",
                    "since": since_ts, "until": until_ts},
        )
        for item in ins_raw.get("data", []):
            total = sum(
                v.get("value", 0) for v in item.get("values", [])
                if isinstance(v.get("value"), (int, float))
            )
            insights[item["name"]] = total
        log(f"  → {len(insights)} métricas obtenidas")
    except RuntimeError as e:
        log(f"  ⚠ Insights no disponibles: {e}")

    # ── Campañas ──────────────────────────────────────────────────────────────
    log("Buscando campañas Click-to-Messenger…")
    campaigns = []
    try:
        accounts = api_get("me/adaccounts", params={"fields": "id,name", "limit": 10})
        for acc in accounts.get("data", []):
            clist = api_paginate(
                f"{acc['id']}/campaigns",
                params={
                    "fields": "id,name,objective,status,insights{impressions,reach,actions}",
                    "filtering": '[{"field":"objective","operator":"EQUAL","value":"MESSAGES"}]',
                    "limit": 50,
                },
            )
            for c in clist:
                c["_account"] = acc.get("name", acc["id"])
            campaigns.extend(clist)
        log(f"  → {len(campaigns)} campañas de mensajes")
    except RuntimeError as e:
        log(f"  ⚠ Campañas no disponibles: {e}")

    # ── Mensajes con referral ─────────────────────────────────────────────────
    log(f"Analizando referrals en {len(convs)} conversaciones…")
    ad_msgs, by_day, ad_sources = [], defaultdict(int), defaultdict(int)
    total = len(convs)
    for idx, conv in enumerate(convs, 1):
        msgs = api_paginate(
            f"{conv['id']}/messages",
            params={"fields": "id,created_time,from,message,referral", "limit": 100},
        )
        for msg in msgs:
            ref = msg.get("referral", {})
            if ref:
                ad_msgs.append({
                    "time"  : msg.get("created_time", "")[:10],
                    "from"  : msg.get("from", {}).get("name", "Desconocido"),
                    "ad_id" : ref.get("ad_id", "N/A"),
                    "source": ref.get("source", "N/A"),
                    "type"  : ref.get("type", "N/A"),
                })
                by_day[msg.get("created_time", "")[:10]] += 1
                ad_sources[ref.get("source", "N/A")] += 1
        if idx % 10 == 0 or idx == total:
            log(f"  Revisadas {idx}/{total} conversaciones…")

    # ── Resumen campañas ──────────────────────────────────────────────────────
    campaign_summary = []
    for c in campaigns[:10]:
        ins = (c.get("insights") or {}).get("data", [{}])[0]
        actions = {a["action_type"]: a["value"] for a in ins.get("actions", [])}
        campaign_summary.append({
            "nombre"        : c.get("name", ""),
            "estado"        : c.get("status", ""),
            "impresiones"   : ins.get("impressions", 0),
            "alcance"       : ins.get("reach", 0),
            "msgs_iniciados": actions.get(
                "onsite_conversion.messaging_conversation_started_7d", 0),
        })

    total_convs = len(convs)
    return {
        "pagina"     : "Crecelac",
        "page_id"    : PAGE_ID,
        "periodo"    : f"últimos {since_days} días (desde {since_str})",
        "generado_en": _now().strftime("%Y-%m-%d %H:%M UTC"),
        "resumen": {
            "total_convs"     : total_convs,
            "no_leidos"       : sum(c.get("unread_count", 0) for c in convs),
            "total_mensajes"  : sum(c.get("message_count", 0) for c in convs),
            "desde_anuncios"  : len(ad_msgs),
            "tasa_ads_%"      : round(len(ad_msgs) / max(total_convs, 1) * 100, 1),
            "campanas_activas": sum(1 for c in campaigns if c.get("status") == "ACTIVE"),
        },
        "mensajes_por_dia" : dict(sorted(by_day.items())),
        "fuentes_anuncios" : dict(sorted(ad_sources.items(), key=lambda x: -x[1])),
        "insights"         : insights,
        "campanas"         : campaign_summary,
        "detalle_ads"      : ad_msgs[:200],
    }


# ── Imprimir reporte ───────────────────────────────────────────────────────────
def print_report(db: dict):
    SEP  = "═" * 62
    SEP2 = "─" * 62

    print(f"\n{SEP}")
    print(f"  MESSENGER ADS REPORT — {db['pagina']} ({db['page_id']})")
    print(f"  Período  : {db['periodo']}")
    print(f"  Generado : {db['generado_en']}")
    print(SEP)

    rs = db["resumen"]
    print("\n📬  RESUMEN DE CONVERSACIONES")
    print(f"  {'Total conversaciones':<35} {rs['total_convs']}")
    print(f"  {'No leídas':<35} {rs['no_leidos']}")
    print(f"  {'Total mensajes':<35} {rs['total_mensajes']}")
    print(f"  {'Desde anuncios (referral)':<35} {rs['desde_anuncios']}")
    print(f"  {'% desde anuncios':<35} {rs['tasa_ads_%']}%")
    print(f"  {'Campañas activas':<35} {rs['campanas_activas']}")

    print(f"\n{SEP2}")
    print("📅  MENSAJES POR DÍA (desde anuncios)")
    print(SEP2)
    mpd = db["mensajes_por_dia"]
    if mpd:
        max_val = max(mpd.values(), default=1)
        for day, cnt in list(mpd.items())[-30:]:
            bar = "█" * int(cnt / max_val * 30)
            print(f"  {day}  {bar:<30} {cnt}")
    else:
        print("  Sin datos")

    print(f"\n{SEP2}")
    print("🎯  FUENTES DE ANUNCIOS")
    print(SEP2)
    for src, cnt in db["fuentes_anuncios"].items():
        print(f"  {src:<40} {cnt}")
    if not db["fuentes_anuncios"]:
        print("  Sin datos")

    print(f"\n{SEP2}")
    print("📈  INSIGHTS DE PÁGINA")
    print(SEP2)
    labels = {
        "page_messages_total_messaging_connections" : "Conexiones totales",
        "page_messages_new_conversations_unique"    : "Nuevas conversaciones",
        "page_messages_blocked_conversations_unique": "Conversaciones bloqueadas",
        "page_messages_reported_conversations_unique":"Conversaciones reportadas",
        "page_response_time_median"                 : "Tiempo resp. mediano (s)",
    }
    for k, v in db["insights"].items():
        print(f"  {labels.get(k, k):<40} {int(v)}")
    if not db["insights"]:
        print("  Sin datos (verifica permisos del token)")

    print(f"\n{SEP2}")
    print("🏹  CAMPAÑAS CLICK-TO-MESSENGER")
    print(SEP2)
    if db["campanas"]:
        for c in db["campanas"]:
            estado = "✓ ACTIVA" if c["estado"] == "ACTIVE" else c["estado"]
            print(f"  {c['nombre'][:45]:<45} [{estado}]")
            print(f"    Impresiones: {c['impresiones']}  |  Alcance: {c['alcance']}  |  Msgs iniciados: {c['msgs_iniciados']}")
    else:
        print("  Sin campañas (verifica permisos ads_read)")

    print(f"\n{SEP}\n")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Estadísticas de Messenger por pauta Facebook Ads — Crecelac"
    )
    parser.add_argument("--token",     "-t", default="",  help="Page Access Token de Facebook")
    parser.add_argument("--dias",      "-d", type=int, default=30, help="Días hacia atrás (default: 30)")
    parser.add_argument("--max-convs", "-m", type=int, default=50, help="Máx. conversaciones a analizar (default: 50)")
    parser.add_argument("--output",    "-o", default="dashboard_data.json", help="Archivo JSON de salida")
    parser.add_argument("--solo-json", action="store_true", help="No imprimir reporte, solo guardar JSON")
    args = parser.parse_args()

    # Token: argumento > variable de entorno > input interactivo
    tok = args.token.strip()
    if not tok:
        import os
        tok = os.environ.get("FB_PAGE_TOKEN", "").strip()
    if not tok:
        print("Access Token requerido.")
        tok = input("Pega tu Page Access Token: ").strip()
    if not tok:
        print("ERROR: Token vacío. Saliendo.")
        sys.exit(1)

    _token[0] = tok

    print(f"\n🚀  Iniciando — período: {args.dias} días · máx. conversaciones: {args.max_convs}\n")

    try:
        data = fetch_all(since_days=args.dias, max_convs=args.max_convs)
    except RuntimeError as e:
        print(f"\n✗ ERROR: {e}\n")
        sys.exit(1)

    if not args.solo_json:
        print_report(data)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"✅  Datos guardados en '{args.output}'")


if __name__ == "__main__":
    main()
