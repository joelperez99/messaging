"""
Exportador a Google Sheets — Crecelac
Hoja: 1-9GL26RN2DqpiD7wgyl4TH1whxzeNpikyCsMOK3_yds

IMPORTANTE: Comparte la hoja con:
  predict@master-plateau-489706-m4.iam.gserviceaccount.com  (Editor)

Dependencias:
  pip install gspread google-auth
"""

import os
import time

import gspread
from google.oauth2.service_account import Credentials

# ── Config ─────────────────────────────────────────────────────────────────────
SPREADSHEET_ID = "1-9GL26RN2DqpiD7wgyl4TH1whxzeNpikyCsMOK3_yds"
CREDS_FILE     = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "master-plateau-489706-m4-685e52dee3cc.json",
)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ── Helpers ────────────────────────────────────────────────────────────────────
def _client():
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_or_create_tab(ss, title: str, rows: int = 2000, cols: int = 20):
    try:
        return ss.worksheet(title)
    except gspread.WorksheetNotFound:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def _write_tab(ws, headers: list, data_rows: list):
    """Borra el tab y escribe headers + filas de golpe (mínimas llamadas a la API)."""
    ws.clear()
    all_rows = [headers] + data_rows if data_rows else [headers]
    # Sheets API tiene límite de celdas por llamada; si hay muchas filas, dividir en lotes
    BATCH = 1000
    for i in range(0, len(all_rows), BATCH):
        ws.append_rows(all_rows[i:i + BATCH], value_input_option="USER_ENTERED")
        if i > 0:
            time.sleep(1)   # pausa entre lotes para no exceder cuota


# ── Exportar Contactos Nuevos ──────────────────────────────────────────────────
def export_new_contacts(data: dict, log_cb=None):
    """
    Exporta datos de new_contacts_dashboard.
    Tabs creados/actualizados:
      · Contactos Nuevos — una fila por día (Fecha, Nuevos)
    """
    def log(msg): log_cb and log_cb(msg)

    log("☁ Conectando con Google Sheets…")
    ss = _client().open_by_key(SPREADSHEET_ID)

    # ── Tab: Contactos Nuevos ──────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Contactos Nuevos")
    rows = [[d, v] for d, v in sorted(data.get("new_by_day", {}).items())]
    _write_tab(ws, ["Fecha", "Nuevos Contactos"], rows)
    log(f"  ✓ 'Contactos Nuevos' — {len(rows)} días escritos")

    # ── Tab: Meta ──────────────────────────────────────────────────────────────
    ws_meta = _get_or_create_tab(ss, "Meta - Contactos")
    meta_rows = [
        ["Generado en",  data.get("generado_en", "")],
        ["Desde",        data.get("since_str", "")],
        ["Método",       data.get("method", "")],
        ["Total nuevos", sum(data.get("new_by_day", {}).values())],
    ]
    _write_tab(ws_meta, ["Campo", "Valor"], meta_rows)

    log("✓ Exportación de Contactos Nuevos completada")


# ── Exportar Messenger Stats ───────────────────────────────────────────────────
def export_messenger_stats(data: dict, log_cb=None):
    """
    Exporta datos de messenger_ads_stats.
    Tabs creados/actualizados:
      · Resumen
      · Mensajes por Día (ads)
      · Mensajes por Día (todos)
      · Detalle Anuncios
      · Campañas
      · Insights
      · Fuentes Anuncios
    """
    def log(msg): log_cb and log_cb(msg)

    log("☁ Conectando con Google Sheets…")
    ss = _client().open_by_key(SPREADSHEET_ID)

    # ── Resumen ────────────────────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Resumen")
    rs = data.get("resumen", {})
    labels = {
        "total_convs"     : "Total conversaciones",
        "no_leidos"       : "No leídas",
        "total_mensajes"  : "Total mensajes",
        "desde_anuncios"  : "Desde anuncios",
        "tasa_ads"        : "% desde anuncios",
        "campanas_activas": "Campañas activas",
    }
    rows = [[labels.get(k, k), v] for k, v in rs.items()]
    rows += [
        ["Página",       data.get("pagina", "")],
        ["Período",      data.get("desde_fecha", "")],
        ["Generado en",  data.get("generado_en", "")],
    ]
    _write_tab(ws, ["Métrica", "Valor"], rows)
    log(f"  ✓ 'Resumen' — {len(rows)} filas")

    # ── Mensajes por Día (ads) ─────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Msgs por Día (ads)")
    rows = [[d, v] for d, v in sorted(data.get("por_dia", {}).items())]
    _write_tab(ws, ["Fecha", "Mensajes desde Anuncios"], rows)
    log(f"  ✓ 'Msgs por Día (ads)' — {len(rows)} días")

    # ── Mensajes por Día (todos) ───────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Msgs por Día (todos)")
    rows = [[d, v] for d, v in sorted(data.get("por_dia_todos", {}).items())]
    _write_tab(ws, ["Fecha", "Total Mensajes"], rows)
    log(f"  ✓ 'Msgs por Día (todos)' — {len(rows)} días")

    # ── Detalle Anuncios ───────────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Detalle Anuncios")
    ads = data.get("detalle_ads", [])
    rows = [
        [m.get("time", ""), m.get("from", ""), m.get("ad_id", ""),
         m.get("source", ""), m.get("type", ""), m.get("campaign", "")]
        for m in ads
    ]
    _write_tab(ws, ["Fecha", "Usuario", "Ad ID", "Fuente", "Tipo", "Campaña"], rows)
    log(f"  ✓ 'Detalle Anuncios' — {len(rows)} mensajes")

    # ── Campañas ───────────────────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Campañas")
    camps = data.get("campaigns", [])
    rows = [
        [c.get("nombre", ""), c.get("estado", ""),
         c.get("impresiones", 0), c.get("alcance", 0), c.get("msgs_iniciados", 0)]
        for c in camps
    ]
    _write_tab(ws, ["Nombre", "Estado", "Impresiones", "Alcance", "Msgs Iniciados"], rows)
    log(f"  ✓ 'Campañas' — {len(rows)} campañas")

    # ── Insights ───────────────────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Insights")
    labels_ins = {
        "page_messages_total_messaging_connections" : "Conexiones totales",
        "page_messages_new_conversations_unique"    : "Nuevas conversaciones",
        "page_messages_blocked_conversations_unique": "Conversaciones bloqueadas",
        "page_messages_reported_conversations_unique":"Conversaciones reportadas",
        "page_response_time_median"                 : "Tiempo resp. mediano (s)",
    }
    ins = data.get("insights", {})
    rows = [[labels_ins.get(k, k), v] for k, v in ins.items()]
    _write_tab(ws, ["Métrica", "Valor"], rows)
    log(f"  ✓ 'Insights' — {len(rows)} métricas")

    # ── Fuentes de Anuncios ────────────────────────────────────────────────────
    ws = _get_or_create_tab(ss, "Fuentes Anuncios")
    fuentes = data.get("fuentes", {})
    rows = [[src, cnt] for src, cnt in sorted(fuentes.items(), key=lambda x: -x[1])]
    _write_tab(ws, ["Fuente", "Cantidad"], rows)
    log(f"  ✓ 'Fuentes Anuncios' — {len(rows)} fuentes")

    log("✓ Exportación de Messenger Stats completada")


# ── Exportar Análisis de Mensajes ─────────────────────────────────────────────
def export_message_analysis(date_str: str, results: list, log_cb=None):
    """
    Guarda el análisis de mensajes del día indicado en la pestaña 'Análisis Mensajes'.
    Si ya existen filas para ese día, se borran y se reemplazan con los nuevos resultados.

    results: lista de dicts con claves: motivo, cantidad, porcentaje
    """
    def log(msg): log_cb and log_cb(msg)

    log("☁ Guardando análisis de mensajes en Google Sheets…")
    ss = _client().open_by_key(SPREADSHEET_ID)
    ws = _get_or_create_tab(ss, "Análisis Mensajes")

    # Leer datos existentes y filtrar filas que NO sean del mismo día
    existing = ws.get_all_values()
    if existing:
        header = existing[0]
        other_rows = [r for r in existing[1:] if r and r[0] != date_str]
    else:
        header = ["Fecha", "Motivo de contacto", "Cantidad", "% del total"]
        other_rows = []

    new_rows = [
        [date_str, r["motivo"], r["cantidad"], f"{r['porcentaje']}%"]
        for r in results
    ]

    ws.clear()
    ws.append_rows([header] + other_rows + new_rows, value_input_option="USER_ENTERED")
    log(f"  ✓ {len(new_rows)} categorías guardadas para {date_str}")
    log("✓ Análisis guardado correctamente")
