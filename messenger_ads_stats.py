"""
Dashboard de Estadísticas - Mensajes de Messenger por Pauta Facebook Ads
Página: Crecelac | Page ID: 1795816893869115
GUI: tkinter + matplotlib
"""

import json
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

def _now() -> datetime:
    return datetime.now(timezone.utc)

import requests
import tkinter as tk
from tkinter import ttk, messagebox

import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ── Paleta de colores ──────────────────────────────────────────────────────────
BG        = "#0f1117"
PANEL     = "#1a1d27"
ACCENT    = "#1877f2"       # azul Facebook
ACCENT2   = "#00c6ff"
GREEN     = "#25d366"
YELLOW    = "#f5a623"
RED       = "#e74c3c"
TEXT      = "#e8eaf0"
SUBTEXT   = "#8b9bbf"

# ── Configuración API ──────────────────────────────────────────────────────────
PAGE_ID      = "1795816893869115"
ACCESS_TOKEN = ""          # se sobrescribe desde la UI
BASE_URL     = "https://graph.facebook.com/v19.0"


# ══════════════════════════════════════════════════════════════════════════════
#  API helpers
# ══════════════════════════════════════════════════════════════════════════════
_token      = [ACCESS_TOKEN]   # token de mensajería (pages_messaging)
_page_token = [""]             # page token resuelto automáticamente
_ads_token  = [""]             # token de ads (ads_read) — opcional


def _tok(ads: bool = False) -> str:
    """Devuelve el token correcto según el tipo de llamada."""
    if ads:
        t = _ads_token[0].strip()
        if t:
            return t
        # fallback al token de mensajería si no hay token de ads
    pt = _page_token[0].strip()
    if pt:
        return pt
    ut = _token[0].strip()
    if not ut:
        raise RuntimeError("Debes pegar un Access Token de mensajería antes de actualizar.")
    return ut


def resolve_page_token(log_cb) -> str:
    """
    Convierte un User Token en Page Access Token para PAGE_ID.
    Estrategia 1: GET /{PAGE_ID}?fields=access_token  (más directo)
    Estrategia 2: GET /me/accounts y buscar la página por ID
    """
    user_tok = _token[0].strip()
    if not user_tok:
        raise RuntimeError("Pega un Access Token antes de actualizar.")

    log_cb("Validando token de mensajería…")

    # ── Estrategia 0: verificar si el token ya es un Page Token válido ─────
    # (cuando el usuario pega directamente un Page Access Token)
    r0 = requests.get(
        f"{BASE_URL}/{PAGE_ID}/conversations",
        params={"access_token": user_tok, "limit": 1, "platform": "messenger"},
        timeout=30,
    )
    d0 = r0.json()
    if "error" not in d0:
        _page_token[0] = user_tok
        log_cb("  ✓ Token de página válido — usándolo directamente")
        return _page_token[0]

    err0 = d0.get("error", {})
    log_cb(f"  Token directo no válido para conversaciones ({err0.get('code','?')}), intentando resolver…")

    # ── Estrategia 1: obtener page token desde user token ─────────────────
    r = requests.get(
        f"{BASE_URL}/{PAGE_ID}",
        params={"fields": "access_token,name", "access_token": user_tok},
        timeout=30,
    )
    data = r.json()
    if "access_token" in data:
        _page_token[0] = data["access_token"]
        log_cb(f"  ✓ Page Token resuelto para '{data.get('name', PAGE_ID)}'")
        return _page_token[0]

    log_cb(f"  Estrategia 1 sin éxito, probando /me/accounts…")

    # ── Estrategia 2: /me/accounts ─────────────────────────────────────────
    r2 = requests.get(
        f"{BASE_URL}/me/accounts",
        params={"access_token": user_tok, "limit": 100},
        timeout=30,
    )
    data2 = r2.json()
    if "error" not in data2:
        for page in data2.get("data", []):
            if page.get("id") == PAGE_ID:
                _page_token[0] = page["access_token"]
                log_cb(f"  ✓ Page Token resuelto vía /me/accounts")
                return _page_token[0]

    # ── Fallback: usar el token tal cual ───────────────────────────────────
    log_cb("  ⚠ No se pudo resolver el Page Token automáticamente.")
    log_cb(f"  ⚠ Error original: {err0.get('message','')[:80]}")
    log_cb("  ℹ  Asegúrate de pegar un Page Access Token (no User Token).")
    _page_token[0] = user_tok
    return user_tok


def api_get(endpoint: str, params: dict = None, ads: bool = False) -> dict:
    params = params or {}
    params["access_token"] = _tok(ads=ads)
    r = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
    data = r.json()
    if "error" in data:
        raise RuntimeError(data["error"].get("message", str(data["error"])))
    return data


def api_paginate(endpoint: str, params: dict = None, ads: bool = False) -> list:
    results, params = [], params or {}
    params["access_token"] = _tok(ads=ads)
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


# ══════════════════════════════════════════════════════════════════════════════
#  Caché local
# ══════════════════════════════════════════════════════════════════════════════
CACHE_FILE = "messenger_cache.json"


def cache_load() -> dict:
    """Lee el caché local. Devuelve dict vacío si no existe."""
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def cache_save(data: dict):
    """Guarda el estado completo en el caché local."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def cache_merge(old: dict, new_convs_data: dict, new_insights: dict,
                new_campaigns: list, since_str: str) -> dict:
    """
    Fusiona el caché existente con los datos nuevos.
    - Mensajes por día: suma acumulada (sin duplicar días ya existentes)
    - Conv activity: reemplaza con los más recientes
    - Insights y campañas: siempre se sobreescriben (datos frescos)
    """
    # Mensajes por día (todos)
    merged_day_all = dict(old.get("por_dia_todos", {}))
    for day, cnt in new_convs_data.get("por_dia_todos", {}).items():
        merged_day_all[day] = cnt          # reemplaza el día (más preciso)

    # Mensajes por día (ads)
    merged_day_ads = dict(old.get("por_dia", {}))
    for day, cnt in new_convs_data.get("por_dia", {}).items():
        merged_day_ads[day] = cnt

    # Fuentes de anuncios — acumular
    merged_fuentes = dict(old.get("fuentes", {}))
    for src, cnt in new_convs_data.get("fuentes", {}).items():
        merged_fuentes[src] = merged_fuentes.get(src, 0) + cnt

    # Detalle de mensajes de ads — deduplicar por msg_id vía "time+from+ad_id"
    old_ads  = {f"{m['time']}|{m['from']}|{m['ad_id']}": m
                for m in old.get("detalle_ads", [])}
    for m in new_convs_data.get("detalle_ads", []):
        key = f"{m['time']}|{m['from']}|{m['ad_id']}"
        old_ads[key] = m
    merged_ads = list(old_ads.values())[:200]

    # Resumen recalculado
    total_mensajes_ads = sum(merged_day_ads.values())
    total_convs        = new_convs_data["resumen"]["total_convs"]

    resumen = {
        "total_convs"     : total_convs,
        "no_leidos"       : new_convs_data["resumen"]["no_leidos"],
        "total_mensajes"  : new_convs_data["resumen"]["total_mensajes"],
        "desde_anuncios"  : total_mensajes_ads,
        "tasa_ads"        : round(total_mensajes_ads / max(total_convs, 1) * 100, 1),
        "campanas_activas": sum(1 for c in new_campaigns if c.get("estado") == "ACTIVE"),
    }

    return {
        "pagina"        : new_convs_data.get("pagina", "Crecelac"),
        "page_id"       : PAGE_ID,
        "periodo_dias"  : new_convs_data.get("periodo_dias", 30),
        "desde_fecha"   : since_str,
        "generado_en"   : _now().strftime("%Y-%m-%d %H:%M UTC"),
        "last_fetch_ts" : _now().isoformat(),
        "resumen"       : resumen,
        "por_dia_todos" : dict(sorted(merged_day_all.items())),
        "por_dia"       : dict(sorted(merged_day_ads.items())),
        "fuentes"       : merged_fuentes,
        "insights"      : new_insights,
        "campaigns"     : new_campaigns,
        "conv_activity" : new_convs_data.get("conv_activity", []),
        "detalle_ads"   : merged_ads,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Lógica de negocio
# ══════════════════════════════════════════════════════════════════════════════
def fetch_all(log_cb, since_days: int = 30, max_convs: int = 100) -> dict:
    """Descarga datos nuevos, fusiona con caché local y devuelve el dict completo."""

    # ── Caché ────────────────────────────────────────────────────────────────
    cache = cache_load()
    last_fetch_ts = cache.get("last_fetch_ts", "")
    if last_fetch_ts:
        last_dt   = datetime.fromisoformat(last_fetch_ts)
        fetch_since_str = last_dt.strftime("%Y-%m-%d")
        log_cb(f"💾 Caché encontrado (último fetch: {fetch_since_str}) — solo se cargarán datos nuevos")
    else:
        fetch_since_str = None
        log_cb("💾 Sin caché previo — carga completa inicial")

    since_dt  = _now() - timedelta(days=since_days)
    since_ts  = int(since_dt.timestamp())
    until_ts  = int(_now().timestamp())
    since_str = since_dt.strftime("%Y-%m-%d")

    # Paso 0: convertir User Token → Page Token automáticamente
    resolve_page_token(log_cb)

    # ── Conversaciones ────────────────────────────────────────────────────────
    # Filtrar del lado del servidor con "since" para evitar paginar todo el historial
    filter_from     = fetch_since_str if fetch_since_str else since_str
    filter_since_ts = int(datetime.strptime(filter_from, "%Y-%m-%d")
                          .replace(tzinfo=timezone.utc).timestamp())
    log_cb(f"Obteniendo conversaciones desde {filter_from} (máx. {max_convs}) — filtro en API…")
    convs_raw = api_paginate(
        f"{PAGE_ID}/conversations",
        params={
            "platform": "messenger",
            "fields"  : "id,updated_time,message_count,unread_count,participants",
            "limit"   : 100,
            "since"   : filter_since_ts,      # ← filtra en Facebook, no descarga todo
        },
    )
    convs = convs_raw[:max_convs]
    log_cb(f"  → {len(convs_raw)} conversaciones en período · usando {len(convs)}")

    log_cb(f"Obteniendo insights de la página ({since_days} días)…")
    since = since_ts
    until = until_ts
    metrics = [
        "page_messages_total_messaging_connections",
        "page_messages_new_conversations_unique",
        "page_messages_blocked_conversations_unique",
        "page_messages_reported_conversations_unique",
        "page_response_time_median",
    ]
    try:
        ins_raw = api_get(
            f"{PAGE_ID}/insights",
            params={"metric": ",".join(metrics), "period": "day",
                    "since": since, "until": until},
        )
        insights = {}
        for item in ins_raw.get("data", []):
            total = sum(
                v.get("value", 0) for v in item.get("values", [])
                if isinstance(v.get("value"), (int, float))
            )
            insights[item["name"]] = total
    except RuntimeError as e:
        log_cb(f"  [Insights] {e}")
        insights = {}

    log_cb("Buscando campañas Click-to-Messenger…")
    campaigns = []
    has_ads_token = bool(_ads_token[0].strip())
    if not has_ads_token:
        log_cb("  ⚠ Sin token de Ads — omitiendo campañas (agrega el token de Ads en la UI)")
    else:
        try:
            # Paso 1: verificar token de ads
            me_info = api_get("me", params={"fields": "id,name"}, ads=True)
            log_cb(f"  ✓ Token Ads válido — {me_info.get('name', me_info.get('id'))}")

            # Paso 2: obtener cuentas publicitarias
            accounts = api_get("me/adaccounts", params={"fields": "id,name", "limit": 10}, ads=True)
            ad_accs  = accounts.get("data", [])
            log_cb(f"  → {len(ad_accs)} cuentas publicitarias")

            if not ad_accs:
                log_cb("  ⚠ No hay cuentas publicitarias vinculadas a este usuario")
                log_cb("  ℹ  Ve a business.facebook.com → Configuración → Cuentas publicitarias")

            for acc in ad_accs:
                acc_id = acc["id"]
                log_cb(f"  Cuenta: {acc.get('name', acc_id)} ({acc_id})")

                # Paso 3: todas las campañas (sin filtro de objetivo para diagnóstico)
                all_camps = api_paginate(
                    f"{acc_id}/campaigns",
                    params={"fields": "id,name,objective,status", "limit": 50},
                    ads=True,
                )
                log_cb(f"    → {len(all_camps)} campañas totales")
                for c in all_camps:
                    log_cb(f"      · [{c.get('status')}] {c.get('name')} — objetivo: {c.get('objective')}")

                # Paso 4: filtrar MESSAGES y obtener insights
                msg_camps = [c for c in all_camps if c.get("objective") == "MESSAGES"]
                log_cb(f"    → {len(msg_camps)} con objetivo MESSAGES")
                for c in msg_camps:
                    try:
                        ins = api_get(f"{c['id']}/insights",
                                      params={"fields": "impressions,reach,actions", "limit": 1},
                                      ads=True)
                        c["insights"] = {"data": ins.get("data", [])}
                    except RuntimeError:
                        c["insights"] = {"data": []}
                    c["_account"] = acc.get("name", acc_id)
                campaigns.extend(msg_camps)

        except RuntimeError as e:
            log_cb(f"  [Campañas ERROR] {e}")

    log_cb(f"  → {len(campaigns)} campañas con objetivo MESSAGES encontradas")

    log_cb("Analizando mensajes por conversación…")
    ad_msgs, by_day_ads, ad_sources = [], defaultdict(int), defaultdict(int)
    by_day_all   = defaultdict(int)   # todos los mensajes por día
    conv_activity = []                # mensajes por conversación (para gráfico)
    total = len(convs)

    for idx, conv in enumerate(convs, 1):
        msgs = api_paginate(
            f"{conv['id']}/messages",
            params={"fields": "id,created_time,from,message,referral", "limit": 100},
        )
        conv_msg_count = 0
        for msg in msgs:
            day = msg.get("created_time", "")[:10]
            if day >= since_str:
                by_day_all[day] += 1
                conv_msg_count += 1
            ref = msg.get("referral", {})
            if ref:
                ad_msgs.append({
                    "time"   : day,
                    "from"   : msg.get("from", {}).get("name", "Desconocido"),
                    "ad_id"  : ref.get("ad_id", "N/A"),
                    "source" : ref.get("source", "N/A"),
                    "type"   : ref.get("type", "N/A"),
                })
                by_day_ads[day] += 1
                ad_sources[ref.get("source", "N/A")] += 1

        # Participantes para nombre de conversación
        participants = conv.get("participants", {}).get("data", [])
        other = next((p["name"] for p in participants if p.get("name") != "Crecelac"), "Usuario")
        conv_activity.append({"nombre": other, "mensajes": conv_msg_count,
                               "no_leidos": conv.get("unread_count", 0)})

        if idx % 5 == 0 or idx == total:
            log_cb(f"  Revisadas {idx}/{total} conversaciones…")

    log_cb(f"  → {len(ad_msgs)} mensajes desde anuncios · {sum(by_day_all.values())} mensajes totales en período")

    # ── Resolver ad_id → nombre de campaña ───────────────────────────────────
    # Intentar con cada ad_id único que llegó en los referrals
    ad_id_to_campaign: dict = {}
    unique_ad_ids = {m["ad_id"] for m in ad_msgs if m["ad_id"] != "N/A"}
    if unique_ad_ids:
        log_cb(f"Resolviendo nombres de campaña para {len(unique_ad_ids)} ad_ids…")
        for ad_id in unique_ad_ids:
            try:
                info = api_get(ad_id, params={"fields": "id,name,campaign{id,name}"}, ads=True)
                camp = info.get("campaign", {})
                camp_name = camp.get("name") or info.get("name") or ad_id
                ad_id_to_campaign[ad_id] = camp_name
                log_cb(f"  ✓ {ad_id} → {camp_name}")
            except RuntimeError:
                ad_id_to_campaign[ad_id] = f"Anuncio {ad_id}"
        # Agregar nombre de campaña a cada mensaje
        for m in ad_msgs:
            m["campaign"] = ad_id_to_campaign.get(m["ad_id"], "Desconocido")
        # Construir campaign_summary desde los ad_ids resueltos si no hubo campaigns de API
        if not campaigns:
            from collections import Counter
            camp_counts = Counter(m["campaign"] for m in ad_msgs)
            for camp_name, count in camp_counts.most_common(10):
                campaigns.append({
                    "nombre": camp_name, "estado": "ACTIVA",
                    "impresiones": 0, "alcance": 0, "msgs_iniciados": count,
                })
            log_cb(f"  → {len(campaigns)} campañas resueltas desde ad_ids")
    else:
        for m in ad_msgs:
            m["campaign"] = "N/A"

    campaign_summary = []
    for c in campaigns[:10]:
        ins = (c.get("insights") or {}).get("data", [{}])[0]
        actions = {a["action_type"]: a["value"] for a in ins.get("actions", [])}
        campaign_summary.append({
            "nombre"      : c.get("nombre") or c.get("name", ""),
            "estado"      : c.get("estado") or c.get("status", ""),
            "impresiones" : ins.get("impressions", 0) or c.get("impresiones", 0),
            "alcance"     : ins.get("reach", 0) or c.get("alcance", 0),
            "msgs_iniciados": actions.get(
                "onsite_conversion.messaging_conversation_started_7d", 0)
                or c.get("msgs_iniciados", 0),
        })

    total_convs = len(convs)
    new_data = {
        "pagina"      : "Crecelac",
        "page_id"     : PAGE_ID,
        "periodo_dias": since_days,
        "desde_fecha" : since_str,
        "resumen": {
            "total_convs"     : total_convs,
            "no_leidos"       : sum(c.get("unread_count", 0) for c in convs),
            "total_mensajes"  : sum(c.get("message_count", 0) for c in convs),
            "desde_anuncios"  : len(ad_msgs),
            "tasa_ads"        : 0,
            "campanas_activas": sum(1 for c in campaigns if c.get("status") == "ACTIVE"),
        },
        "por_dia_todos" : dict(sorted(by_day_all.items())),
        "por_dia"       : dict(sorted(by_day_ads.items())),
        "fuentes"       : dict(ad_sources),
        "conv_activity" : sorted(conv_activity, key=lambda x: -x["mensajes"])[:15],
        "detalle_ads"   : ad_msgs[:200],
    }

    # Fusionar con caché y guardar
    log_cb("💾 Fusionando con caché local…")
    merged = cache_merge(cache, new_data, insights, campaign_summary, since_str)
    cache_save(merged)
    log_cb(f"  ✓ Caché actualizado en '{CACHE_FILE}'")
    return merged


# ══════════════════════════════════════════════════════════════════════════════
#  GUI
# ══════════════════════════════════════════════════════════════════════════════
class Dashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Messenger Ads Dashboard — Crecelac")
        self.configure(bg=BG)
        self.state("zoomed")            # maximizado en Windows
        self.data = None

        self._build_header()
        self._build_body()
        self._build_statusbar()

        # Cargar caché al iniciar (sin llamadas a la API)
        self.after(100, self._load_cache_on_start)

    # ── Caché al inicio ───────────────────────────────────────────────────────
    def _load_cache_on_start(self):
        cached = cache_load()
        if not cached or "resumen" not in cached:
            self._log("Sin caché previo — pega tu token y presiona «Cargar información».", "info")
            return
        last = cached.get("generado_en", "?")
        self._log(f"💾 Caché cargado (datos del {last}) — actualiza para obtener datos nuevos", "ok")
        self._render(cached)

    # ── Header ─────────────────────────────────────────────────────────────────
    def _build_header(self):
        # Fila 1: título + botón
        hdr = tk.Frame(self, bg=PANEL, height=52)
        hdr.pack(fill="x", side="top")
        hdr.pack_propagate(False)

        tk.Label(
            hdr,
            text="  \U0001f4ac  Messenger Ads Dashboard",
            font=("Segoe UI", 16, "bold"),
            bg=PANEL, fg=TEXT,
        ).pack(side="left", padx=16)

        tk.Label(
            hdr,
            text="Crecelac · Page 1795816893869115",
            font=("Segoe UI", 9),
            bg=PANEL, fg=SUBTEXT,
        ).pack(side="left", padx=4)

        self.btn_refresh = tk.Button(
            hdr,
            text="⟳  Actualizar datos",
            font=("Segoe UI", 10, "bold"),
            bg=ACCENT, fg="white",
            activebackground="#1565c0", activeforeground="white",
            relief="flat", padx=16, pady=4, cursor="hand2",
            command=self._start_fetch,
        )
        self.btn_refresh.pack(side="right", padx=4, pady=8)

        self.btn_sheets = tk.Button(
            hdr,
            text="☁  Sheets",
            font=("Segoe UI", 10, "bold"),
            bg="#1e7e34", fg="white",
            activebackground="#155724", activeforeground="white",
            relief="flat", padx=14, pady=4, cursor="hand2",
            command=self._export_sheets,
        )
        self.btn_sheets.pack(side="right", padx=4, pady=8)

        self.lbl_update = tk.Label(
            hdr, text="", font=("Segoe UI", 9),
            bg=PANEL, fg=SUBTEXT,
        )
        self.lbl_update.pack(side="right", padx=8)

        # Fila 2: campo Access Token
        token_bar = tk.Frame(self, bg="#111827", height=36)
        token_bar.pack(fill="x", side="top")
        token_bar.pack_propagate(False)

        tk.Label(token_bar, text="  Access Token:",
                 font=("Segoe UI", 9, "bold"),
                 bg="#111827", fg=YELLOW).pack(side="left", padx=(12, 4))

        self.token_var = tk.StringVar()
        token_entry = tk.Entry(
            token_bar, textvariable=self.token_var,
            font=("Consolas", 8), bg="#1e2536", fg=TEXT,
            insertbackground=TEXT, relief="flat",
            show="•",          # oculta el texto por seguridad
        )
        token_entry.pack(side="left", fill="x", expand=True, padx=4, pady=5)

        # Botón para mostrar/ocultar
        self._token_visible = False
        def toggle_vis():
            self._token_visible = not self._token_visible
            token_entry.config(show="" if self._token_visible else "•")
        tk.Button(token_bar, text="👁", font=("Segoe UI", 9),
                  bg="#111827", fg=SUBTEXT, relief="flat", cursor="hand2",
                  activebackground="#111827", command=toggle_vis
                  ).pack(side="left", padx=2)

        # ─ Selector de período ─
        tk.Label(token_bar, text="  │  Período:",
                 font=("Segoe UI", 8), bg="#111827", fg=SUBTEXT).pack(side="left", padx=(8,2))

        self.period_var = tk.StringVar(value="30")
        period_opts = [("7 días", "7"), ("15 días", "15"), ("30 días", "30"),
                       ("60 días", "60"), ("90 días", "90")]
        period_menu = ttk.Combobox(
            token_bar, textvariable=self.period_var,
            values=[v for _, v in period_opts],
            width=5, state="readonly", font=("Segoe UI", 8),
        )
        # mostrar etiquetas legibles
        period_menu["values"] = [label for label, _ in period_opts]
        self._period_map = {label: val for label, val in period_opts}
        self.period_var.set("30 días")
        period_menu.pack(side="left", padx=2, pady=5)

        # ─ Límite de conversaciones ─
        tk.Label(token_bar, text="  Máx. convs:",
                 font=("Segoe UI", 8), bg="#111827", fg=SUBTEXT).pack(side="left", padx=(8,2))
        self.maxconv_var = tk.StringVar(value="50")
        maxconv_menu = ttk.Combobox(
            token_bar, textvariable=self.maxconv_var,
            values=["25", "50", "100", "200", "500"],
            width=5, state="readonly", font=("Segoe UI", 8),
        )
        maxconv_menu.pack(side="left", padx=2, pady=5)

        # ── Fila 3: token de Ads (opcional) ───────────────────────────────────
        ads_bar = tk.Frame(self, bg="#0d1a2e", height=32)
        ads_bar.pack(fill="x", side="top")
        ads_bar.pack_propagate(False)

        tk.Label(ads_bar, text="  🏹 Token Ads (ads_read):",
                 font=("Segoe UI", 9, "bold"),
                 bg="#0d1a2e", fg="#9b59b6").pack(side="left", padx=(12, 4))

        self.ads_token_var = tk.StringVar()
        ads_entry = tk.Entry(
            ads_bar, textvariable=self.ads_token_var,
            font=("Consolas", 8), bg="#1a1030", fg=TEXT,
            insertbackground=TEXT, relief="flat", show="•",
        )
        ads_entry.pack(side="left", fill="x", expand=True, padx=4, pady=4)

        self._ads_token_visible = False
        def toggle_ads_vis():
            self._ads_token_visible = not self._ads_token_visible
            ads_entry.config(show="" if self._ads_token_visible else "•")
        tk.Button(ads_bar, text="👁", font=("Segoe UI", 9),
                  bg="#0d1a2e", fg=SUBTEXT, relief="flat", cursor="hand2",
                  activebackground="#0d1a2e", command=toggle_ads_vis
                  ).pack(side="left", padx=2)

        tk.Label(ads_bar,
                 text="  Opcional — para ver campañas y métricas de Ads Manager",
                 font=("Segoe UI", 8), bg="#0d1a2e", fg=SUBTEXT,
                 ).pack(side="left", padx=8)

    # ── Body ───────────────────────────────────────────────────────────────────
    def _build_body(self):
        body = tk.Frame(self, bg=BG)
        body.pack(fill="both", expand=True, padx=16, pady=12)

        # ─ Barra de filtro de campaña ─
        filter_bar = tk.Frame(body, bg=PANEL, height=34)
        filter_bar.pack(fill="x", pady=(0, 6))
        filter_bar.pack_propagate(False)

        tk.Label(filter_bar, text="  🏹 Campaña:",
                 font=("Segoe UI", 9, "bold"), bg=PANEL, fg=YELLOW).pack(side="left", padx=(10, 4))

        self.campaign_var = tk.StringVar(value="— Todas las campañas —")
        self.campaign_menu = ttk.Combobox(
            filter_bar, textvariable=self.campaign_var,
            values=["— Todas las campañas —"],
            width=40, state="readonly", font=("Segoe UI", 9),
        )
        self.campaign_menu.pack(side="left", padx=4, pady=4)
        self.campaign_menu.bind("<<ComboboxSelected>>", lambda *_: self._apply_filter())

        tk.Label(filter_bar, text="  🎯 Ad ID:",
                 font=("Segoe UI", 9, "bold"), bg=PANEL, fg=SUBTEXT).pack(side="left", padx=(16, 4))
        self.adid_var = tk.StringVar(value="— Todos —")
        self.adid_menu = ttk.Combobox(
            filter_bar, textvariable=self.adid_var,
            values=["— Todos —"],
            width=20, state="readonly", font=("Segoe UI", 9),
        )
        self.adid_menu.pack(side="left", padx=4, pady=4)
        self.adid_menu.bind("<<ComboboxSelected>>", lambda *_: self._apply_filter())

        self.lbl_filter_info = tk.Label(
            filter_bar, text="", font=("Segoe UI", 8),
            bg=PANEL, fg=SUBTEXT,
        )
        self.lbl_filter_info.pack(side="left", padx=12)

        tk.Button(filter_bar, text="✕ Limpiar filtro",
                  font=("Segoe UI", 8), bg=PANEL, fg=SUBTEXT,
                  relief="flat", cursor="hand2",
                  activebackground=PANEL, activeforeground=TEXT,
                  command=self._clear_filter).pack(side="right", padx=10)

        # ─ Tarjetas KPI ─
        self.cards_frame = tk.Frame(body, bg=BG)
        self.cards_frame.pack(fill="x")
        self.cards = {}
        kpis = [
            ("total_convs",      "Conversaciones",      ACCENT),
            ("total_mensajes",   "Total Mensajes",       ACCENT2),
            ("desde_anuncios",   "Desde Anuncios",       GREEN),
            ("tasa_ads",         "% desde Ads",          YELLOW),
            ("no_leidos",        "No Leídos",            RED),
            ("campanas_activas", "Campañas Activas",     "#9b59b6"),
        ]
        for key, label, color in kpis:
            card = self._make_card(self.cards_frame, label, "—", color)
            card.pack(side="left", expand=True, fill="both", padx=6, pady=4)
            self.cards[key] = card

        # ─ Zona central: placeholder inicial / gráficos ─
        self.center = tk.Frame(body, bg=BG)
        self.center.pack(fill="both", expand=True, pady=8)

        # Pantalla de bienvenida (visible antes de cargar)
        self.welcome = tk.Frame(self.center, bg=PANEL)
        self.welcome.pack(fill="both", expand=True)

        tk.Label(self.welcome, text="📊", font=("Segoe UI", 52),
                 bg=PANEL, fg=ACCENT).pack(pady=(40, 8))
        tk.Label(self.welcome,
                 text="Dashboard de Mensajes por Pauta",
                 font=("Segoe UI", 20, "bold"), bg=PANEL, fg=TEXT).pack()
        tk.Label(self.welcome,
                 text="Pega tu Page Access Token en la barra superior y presiona el botón para cargar los datos.",
                 font=("Segoe UI", 11), bg=PANEL, fg=SUBTEXT, wraplength=560).pack(pady=8)

        tk.Button(
            self.welcome,
            text="  ⟳  Cargar información  ",
            font=("Segoe UI", 14, "bold"),
            bg=ACCENT, fg="white",
            activebackground="#1565c0", activeforeground="white",
            relief="flat", padx=28, pady=12, cursor="hand2",
            command=self._start_fetch,
        ).pack(pady=24)

        # Tips rápidos
        tips = tk.Frame(self.welcome, bg=PANEL)
        tips.pack(pady=4)
        for icon, txt in [
            ("🔑", "Obtén el token en Meta for Developers → Graph API Explorer"),
            ("📅", "Selecciona el período y máx. conversaciones en la barra superior"),
            ("⚡", "Empieza con 7 días / 25 convs para carga rápida"),
        ]:
            row = tk.Frame(tips, bg=PANEL)
            row.pack(anchor="w", padx=60, pady=2)
            tk.Label(row, text=icon, font=("Segoe UI", 11), bg=PANEL, fg=TEXT).pack(side="left", padx=(0,6))
            tk.Label(row, text=txt,  font=("Segoe UI", 9),  bg=PANEL, fg=SUBTEXT).pack(side="left")

        # Marco de gráficos (oculto hasta que haya datos)
        self.charts_frame = tk.Frame(self.center, bg=BG)
        self.fig = plt.Figure(figsize=(14, 7), facecolor=BG)
        self.fig.subplots_adjust(hspace=0.45, wspace=0.3)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.charts_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        # ─ Tablas ─
        tabs = ttk.Notebook(body)
        tabs.pack(fill="both", expand=True, pady=6)

        self.tab_campaigns = self._make_tab(tabs, "Campañas",
            ["Nombre", "Estado", "Impresiones", "Alcance", "Msgs Iniciados"])
        self.tab_msgs = self._make_tab(tabs, "Mensajes desde Ads",
            ["Fecha", "Usuario", "Ad ID", "Fuente", "Tipo"])

        # estilos ttk
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview",
            background=PANEL, foreground=TEXT, fieldbackground=PANEL,
            rowheight=26, font=("Segoe UI", 9))
        style.configure("Treeview.Heading",
            background=ACCENT, foreground="white", font=("Segoe UI", 9, "bold"))
        style.map("Treeview", background=[("selected", ACCENT)])
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure("TNotebook.Tab",
            background=PANEL, foreground=TEXT, padding=[12, 5],
            font=("Segoe UI", 10))
        style.map("TNotebook.Tab", background=[("selected", ACCENT)],
                  foreground=[("selected", "white")])

    def _make_card(self, parent, label: str, value: str, color: str) -> tk.Frame:
        frame = tk.Frame(parent, bg=PANEL, bd=0, relief="flat")
        frame.configure(highlightbackground=color, highlightthickness=2)
        tk.Label(frame, text=label, font=("Segoe UI", 9), bg=PANEL, fg=SUBTEXT).pack(pady=(10, 2))
        val_lbl = tk.Label(frame, text=value, font=("Segoe UI", 22, "bold"),
                           bg=PANEL, fg=color)
        val_lbl.pack(pady=(0, 10))
        frame._val_label = val_lbl
        return frame

    def _make_tab(self, notebook, title: str, columns: list) -> ttk.Treeview:
        frame = tk.Frame(notebook, bg=PANEL)
        notebook.add(frame, text=f"  {title}  ")
        sb = ttk.Scrollbar(frame, orient="vertical")
        sb.pack(side="right", fill="y")
        tv = ttk.Treeview(frame, columns=columns, show="headings",
                          yscrollcommand=sb.set)
        sb.config(command=tv.yview)
        for col in columns:
            tv.heading(col, text=col)
            tv.column(col, width=160, anchor="center")
        tv.pack(fill="both", expand=True)
        return tv

    # ── Panel de log + barra de progreso ─────────────────────────────────────
    def _build_statusbar(self):
        log_frame = tk.Frame(self, bg=PANEL)
        log_frame.pack(fill="x", side="bottom")

        # ─ Barra de progreso indeterminada ─
        self._pbar_frame = tk.Frame(log_frame, bg=PANEL, height=4)
        self._pbar_frame.pack(fill="x")
        self._pbar_frame.pack_propagate(False)
        style = ttk.Style()
        style.configure("Loading.Horizontal.TProgressbar",
                        troughcolor=PANEL, background=ACCENT,
                        thickness=4, borderwidth=0)
        self.pbar = ttk.Progressbar(
            self._pbar_frame, mode="indeterminate", length=0,
            style="Loading.Horizontal.TProgressbar",
        )
        # se muestra solo al cargar

        # ─ Título del log ─
        title_bar = tk.Frame(log_frame, bg="#111827", height=26)
        title_bar.pack(fill="x")
        title_bar.pack_propagate(False)

        self.status_var = tk.StringVar(value="Listo — pega tu token y presiona «Actualizar datos»")
        self.lbl_status = tk.Label(
            title_bar, textvariable=self.status_var,
            font=("Consolas", 8), bg="#111827", fg=SUBTEXT, anchor="w",
        )
        self.lbl_status.pack(side="left", fill="x", expand=True, padx=8)

        self._log_visible = True
        btn_toggle = tk.Button(
            title_bar, text="▼ Log", font=("Segoe UI", 8),
            bg="#111827", fg=SUBTEXT, relief="flat", cursor="hand2",
            activebackground="#111827", activeforeground=TEXT,
        )
        btn_toggle.pack(side="right", padx=6)

        # ─ Área de texto del log ─
        self._log_container = tk.Frame(log_frame, bg="#0b0e17", height=130)
        self._log_container.pack(fill="x")
        self._log_container.pack_propagate(False)

        log_sb = ttk.Scrollbar(self._log_container, orient="vertical")
        log_sb.pack(side="right", fill="y")
        self.log_text = tk.Text(
            self._log_container,
            font=("Consolas", 9), bg="#0b0e17", fg="#a8d8a8",
            relief="flat", state="disabled", wrap="word",
            yscrollcommand=log_sb.set,
        )
        self.log_text.pack(fill="both", expand=True, padx=4, pady=2)
        log_sb.config(command=self.log_text.yview)

        self.log_text.tag_config("error", foreground=RED)
        self.log_text.tag_config("ok",    foreground=GREEN)
        self.log_text.tag_config("info",  foreground="#a8d8a8")
        self.log_text.tag_config("step",  foreground=ACCENT2)

        def toggle_log():
            if self._log_visible:
                self._log_container.pack_forget()
                btn_toggle.config(text="▲ Log")
            else:
                self._log_container.pack(fill="x")
                btn_toggle.config(text="▼ Log")
            self._log_visible = not self._log_visible
        btn_toggle.config(command=toggle_log)

        self._log("Sistema iniciado — pega tu Page Access Token y presiona «Actualizar datos».", "info")

    def _start_loading_ui(self):
        """Activa barra de progreso + spinner en botón."""
        self.pbar.pack(fill="x")
        self.pbar.start(12)
        self._spinner_chars = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
        self._spinner_idx   = 0
        self._spin()

    def _spin(self):
        if self.btn_refresh["state"] == "disabled":
            ch = self._spinner_chars[self._spinner_idx % len(self._spinner_chars)]
            self.btn_refresh.config(text=f"{ch}  Cargando datos…")
            self._spinner_idx += 1
            self.after(100, self._spin)

    def _stop_loading_ui(self):
        self.pbar.stop()
        self.pbar.pack_forget()
        self.btn_refresh.config(state="normal", text="⟳  Actualizar datos")

    def _log(self, msg: str, tag: str = "info"):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self.log_text.config(state="normal")
        self.log_text.insert("end", line, tag)
        self.log_text.see("end")
        self.log_text.config(state="disabled")
        self.status_var.set(msg)

    # ── Exportar a Google Sheets ──────────────────────────────────────────────
    def _export_sheets(self):
        if not self.data:
            messagebox.showwarning("Sin datos", "Carga los datos antes de exportar.")
            return
        self.btn_sheets.config(state="disabled", text="☁  Exportando…")
        self._log("━━━ Exportando a Google Sheets ━━━", "step")
        threading.Thread(target=self._sheets_thread, daemon=True).start()

    def _sheets_thread(self):
        def log(msg: str):
            tag = ("error" if any(w in msg.lower() for w in ["error", "fallo", "✗"])
                   else "ok" if "✓" in msg else "info")
            self.after(0, self._log, msg, tag)
        try:
            from sheets_export import export_messenger_stats
            export_messenger_stats(self.data, log_cb=log)
        except ImportError:
            log("✗ Instala las dependencias: pip install gspread google-auth")
        except Exception as e:
            log(f"✗ Error al exportar: {e}")
        finally:
            self.after(0, lambda: self.btn_sheets.config(state="normal", text="☁  Sheets"))

    # ── Fetch en hilo ─────────────────────────────────────────────────────────
    def _start_fetch(self):
        tok = self.token_var.get().strip()
        if not tok:
            messagebox.showwarning(
                "Token requerido",
                "Pega un Access Token válido en el campo de arriba antes de actualizar.\n\n"
                "Obtén uno en: Meta for Developers → Graph API Explorer\n"
                "→ «Obtener token de acceso a la página» → Crecelac"
            )
            return
        _token[0] = tok
        _page_token[0] = ""
        _ads_token[0] = self.ads_token_var.get().strip()
        period_label = self.period_var.get()
        self._since_days = int(self._period_map.get(period_label, "30"))
        self._max_convs  = int(self.maxconv_var.get())
        self.btn_refresh.config(state="disabled")
        self._start_loading_ui()
        self._log(f"━━━ Iniciando carga · período: {period_label} · máx. {self._max_convs} convs ━━━", "step")
        threading.Thread(target=self._fetch_thread, daemon=True).start()

    def _fetch_thread(self):
        def log(msg: str):
            if any(w in msg.lower() for w in ["error", "fallo", "exception", "⚠"]):
                tag = "error"
            elif msg.startswith("✓") or msg.startswith("━"):
                tag = "ok"
            elif msg.startswith("  →") or msg.startswith("  ✓"):
                tag = "ok"
            else:
                tag = "info"
            self.after(0, self._log, msg, tag)

        try:
            data = fetch_all(log_cb=log,
                             since_days=self._since_days,
                             max_convs=self._max_convs)
            self.after(0, self._log, "━━━ Carga completada exitosamente ━━━", "ok")
            self.after(0, self._render, data)
        except Exception as e:
            msg = str(e)
            self.after(0, self._log, f"✗ ERROR: {msg}", "error")
            # Solo mostrar popup si no hay datos cargados previamente
            if self.data is None:
                self.after(0, messagebox.showerror, "Error de API", msg)
            else:
                self.after(0, self._log,
                           "  ℹ Se mantienen los datos anteriores del caché.", "info")
        finally:
            self.after(0, self._stop_loading_ui)

    # ── Render principal ──────────────────────────────────────────────────────
    def _render(self, data: dict):
        self.data = data

        # Mostrar gráficos, ocultar bienvenida
        self.welcome.pack_forget()
        self.charts_frame.pack(fill="both", expand=True)

        # Poblar dropdown de campañas (desde API o desde ad_ids resueltos)
        campanas = data.get("campaigns", [])
        # También incluir campañas únicas del campo "campaign" en detalle_ads
        camps_from_msgs = sorted({m["campaign"] for m in data.get("detalle_ads", [])
                                  if m.get("campaign") and m["campaign"] not in ("N/A", "Desconocido")})
        camps_from_api  = [c["nombre"] for c in campanas if c.get("nombre")]
        all_camp_names  = sorted(set(camps_from_api + camps_from_msgs))
        camp_names = ["— Todas las campañas —"] + all_camp_names
        self.campaign_menu["values"] = camp_names
        if self.campaign_var.get() not in camp_names:
            self.campaign_var.set("— Todas las campañas —")

        # Poblar dropdown de Ad IDs (desde detalle_ads)
        ad_ids = sorted({m["ad_id"] for m in data.get("detalle_ads", []) if m.get("ad_id") != "N/A"})
        adid_opts = ["— Todos —"] + ad_ids
        self.adid_menu["values"] = adid_opts
        if self.adid_var.get() not in adid_opts:
            self.adid_var.set("— Todos —")

        self.lbl_update.config(text=f"Actualizado: {data['generado_en']}")
        rs = data["resumen"]
        self._log(
            f"Datos cargados — {rs['total_convs']} convs · "
            f"{rs['desde_anuncios']} desde anuncios · "
            f"{rs['campanas_activas']} campañas activas",
            "ok"
        )

        # Guardar JSON
        with open("dashboard_data.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # Dibujar con el filtro actual
        self._apply_filter()

    # ── Filtros ───────────────────────────────────────────────────────────────
    def _clear_filter(self):
        self.campaign_var.set("— Todas las campañas —")
        self.adid_var.set("— Todos —")
        self._apply_filter()

    def _apply_filter(self):
        if not self.data:
            return

        data          = self.data
        camp_sel      = self.campaign_var.get()
        adid_sel      = self.adid_var.get()
        all_camps     = camp_sel == "— Todas las campañas —"
        all_adids     = adid_sel == "— Todos —"

        # Encontrar ad_ids asociados a la campaña seleccionada
        camp_ad_ids: set = set()
        if not all_camps:
            camp_obj = next((c for c in data.get("campaigns", [])
                             if c["nombre"] == camp_sel), None)
            if camp_obj:
                # Buscar en detalle_ads los msgs que pertenecen a esa campaña
                # La API no relaciona campaign_id con ad_id directamente en referral,
                # pero sí tenemos ad_id en cada mensaje. Usamos el nombre de campaña
                # para filtrar por el ad_id que el usuario seleccione adicionalmente.
                # Si solo se filtra por campaña mostramos todos sus ad_ids conocidos.
                camp_ad_ids = {m["ad_id"] for m in data.get("detalle_ads", [])
                               if m.get("ad_id") != "N/A"}

        # Filtrar detalle_ads
        msgs_filtrados = data.get("detalle_ads", [])
        if not all_camps:
            msgs_filtrados = [m for m in msgs_filtrados
                              if m.get("campaign") == camp_sel or m.get("ad_id") in camp_ad_ids]
        if not all_adids:
            msgs_filtrados = [m for m in msgs_filtrados if m.get("ad_id") == adid_sel]

        # Reconstruir por_dia y fuentes filtrados
        by_day_f: dict = defaultdict(int)
        fuentes_f: dict = defaultdict(int)
        for m in msgs_filtrados:
            by_day_f[m["time"]] += 1
            fuentes_f[m.get("source", "N/A")] += 1

        # Datos filtrados para gráficos
        filtered = {
            **data,
            "por_dia"       : dict(sorted(by_day_f.items())),
            "fuentes"       : dict(fuentes_f),
            "detalle_ads"   : msgs_filtrados,
        }

        # Info del filtro activo
        partes = []
        if not all_camps:
            partes.append(f"Campaña: {camp_sel[:30]}")
        if not all_adids:
            partes.append(f"Ad ID: {adid_sel}")
        info = f"  Filtro: {' · '.join(partes)}  ({len(msgs_filtrados)} msgs)" if partes else ""
        self.lbl_filter_info.config(text=info, fg=YELLOW if partes else SUBTEXT)

        self._render_view(filtered, msgs_filtrados)

    def _render_view(self, data: dict, msgs: list):
        """Dibuja gráficos y llena tablas con los datos (filtrados o no)."""
        rs = data["resumen"]

        # Tarjetas (siempre muestran totales globales, no filtrados)
        mapping = {
            "total_convs"     : str(rs["total_convs"]),
            "total_mensajes"  : str(rs["total_mensajes"]),
            "desde_anuncios"  : str(len(msgs)),
            "tasa_ads"        : f"{round(len(msgs) / max(rs['total_convs'], 1) * 100, 1)}%",
            "no_leidos"       : str(rs["no_leidos"]),
            "campanas_activas": str(rs["campanas_activas"]),
        }
        for key, val in mapping.items():
            self.cards[key]._val_label.config(text=val)

        # Gráficos
        self.fig.clear()
        gs = gridspec.GridSpec(2, 3, figure=self.fig)
        self._chart_mensajes_dia(self.fig.add_subplot(gs[0, :2]), data)
        self._chart_conv_activity(self.fig.add_subplot(gs[0, 2]), data.get("conv_activity", []))
        self._chart_insights(self.fig.add_subplot(gs[1, :2]), data["insights"])
        self._chart_campaigns(self.fig.add_subplot(gs[1, 2]),  data["campaigns"])
        self.canvas.draw()

        # Tabla campañas
        self.tab_campaigns.delete(*self.tab_campaigns.get_children())
        for c in data["campaigns"]:
            self.tab_campaigns.insert("", "end", values=(
                c["nombre"], c["estado"],
                c["impresiones"], c["alcance"], c["msgs_iniciados"],
            ))

        # Tabla mensajes desde ads (filtrada)
        self.tab_msgs.delete(*self.tab_msgs.get_children())
        for m in msgs:
            self.tab_msgs.insert("", "end", values=(
                m["time"], m["from"], m["ad_id"], m["source"], m["type"],
            ))

    # ── Subgráficos ───────────────────────────────────────────────────────────
    def _chart_mensajes_dia(self, ax, data: dict):
        """Barras apiladas: mensajes totales + mensajes desde anuncios."""
        ax.set_facecolor(PANEL)
        ax.figure.set_facecolor(BG)
        dias_todos = data.get("por_dia_todos", {})
        dias_ads   = data.get("por_dia", {})
        periodo    = data.get("periodo_dias", 30)

        if not dias_todos:
            ax.text(0.5, 0.5, "Sin mensajes en el período seleccionado",
                    ha="center", va="center", color=SUBTEXT, transform=ax.transAxes)
            ax.set_title(f"Mensajes por día (últimos {periodo} días)", color=TEXT, pad=8)
            return

        days     = sorted(dias_todos.keys())[-30:]
        totales  = [dias_todos.get(d, 0) for d in days]
        desde_ad = [dias_ads.get(d, 0) for d in days]
        otros    = [t - a for t, a in zip(totales, desde_ad)]

        x = range(len(days))
        ax.bar(x, otros,    color=ACCENT,  width=0.7, label="Orgánico")
        ax.bar(x, desde_ad, color=GREEN,   width=0.7, label="Desde anuncios", bottom=otros)
        ax.set_xticks(x)
        ax.set_xticklabels([d[5:] for d in days], rotation=45, ha="right",
                           fontsize=7, color=SUBTEXT)
        ax.tick_params(axis="y", colors=SUBTEXT)
        ax.spines[:].set_visible(False)
        ax.legend(fontsize=7, frameon=False, labelcolor=TEXT,
                  loc="upper right")
        ax.set_title(f"Mensajes por día (últimos {periodo} días)", color=TEXT, pad=8)
        for i, val in enumerate(totales):
            if val:
                ax.text(i, val + 0.1, str(val), ha="center", va="bottom",
                        color=TEXT, fontsize=7)

    def _chart_conv_activity(self, ax, conv_activity: list):
        """Barras horizontales: conversaciones más activas."""
        ax.set_facecolor(PANEL)
        if not conv_activity:
            ax.text(0.5, 0.5, "Sin datos", ha="center", va="center",
                    color=SUBTEXT, transform=ax.transAxes)
            ax.set_title("Conversaciones más activas", color=TEXT, pad=8)
            return
        items  = conv_activity[:10]
        names  = [c["nombre"][:18] + "…" if len(c["nombre"]) > 18
                  else c["nombre"] for c in items]
        vals   = [c["mensajes"] for c in items]
        unreads= [c["no_leidos"] for c in items]
        colors = [RED if u > 0 else ACCENT2 for u in unreads]
        bars   = ax.barh(range(len(names)), vals, color=colors, height=0.6)
        ax.set_yticks(range(len(names)))
        ax.set_yticklabels(names, color=TEXT, fontsize=8)
        ax.tick_params(axis="x", colors=SUBTEXT, labelsize=7)
        ax.spines[:].set_visible(False)
        ax.set_title("Conversaciones más activas", color=TEXT, pad=8)
        for bar, val, u in zip(bars, vals, unreads):
            label = f"{val}" + (f"  ({u} NL)" if u else "")
            ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
                    label, va="center", color=TEXT, fontsize=7)
        # Leyenda manual
        from matplotlib.patches import Patch
        ax.legend(handles=[Patch(color=ACCENT2, label="Leídas"),
                            Patch(color=RED,    label="Con no leídos")],
                  fontsize=7, frameon=False, labelcolor=TEXT, loc="lower right")

    def _chart_insights(self, ax, insights: dict):
        ax.set_facecolor(PANEL)
        if not insights:
            ax.text(0.5, 0.5, "Sin datos de insights\n(verifica permisos del token)",
                    ha="center", va="center", color=SUBTEXT, transform=ax.transAxes)
            ax.set_title("Insights de página (28 días)", color=TEXT, pad=8)
            return
        labels_map = {
            "page_messages_total_messaging_connections" : "Conexiones totales",
            "page_messages_new_conversations_unique"    : "Nuevas convs.",
            "page_messages_blocked_conversations_unique": "Bloqueadas",
            "page_messages_reported_conversations_unique":"Reportadas",
            "page_response_time_median"                 : "T. resp. mediano (s)",
        }
        labels = [labels_map.get(k, k) for k in insights]
        vals   = list(insights.values())
        colors = [GREEN, ACCENT2, RED, YELLOW, ACCENT]
        bars   = ax.barh(range(len(labels)), vals,
                         color=colors[:len(vals)], height=0.5)
        ax.set_yticks(range(len(labels)))
        ax.set_yticklabels(labels, color=TEXT, fontsize=8)
        ax.tick_params(axis="x", colors=SUBTEXT, labelsize=7)
        ax.spines[:].set_visible(False)
        ax.set_title("Insights de página (28 días)", color=TEXT, pad=8)
        for bar, val in zip(bars, vals):
            ax.text(bar.get_width() + max(vals, default=1) * 0.01,
                    bar.get_y() + bar.get_height() / 2,
                    str(int(val)), va="center", color=TEXT, fontsize=7)

    def _chart_campaigns(self, ax, campaigns: list):
        ax.set_facecolor(PANEL)
        if not campaigns:
            ax.text(0.5, 0.5, "Sin campañas\n(verifica permisos ads_read)",
                    ha="center", va="center", color=SUBTEXT, transform=ax.transAxes)
            ax.set_title("Top campañas — Msgs iniciados", color=TEXT, pad=8)
            return
        nombres = [c["nombre"][:20] + "…" if len(c["nombre"]) > 20
                   else c["nombre"] for c in campaigns]
        vals    = [int(c.get("msgs_iniciados") or 0) for c in campaigns]
        bars    = ax.barh(range(len(nombres)), vals, color=GREEN, height=0.5)
        ax.set_yticks(range(len(nombres)))
        ax.set_yticklabels(nombres, color=TEXT, fontsize=8)
        ax.tick_params(axis="x", colors=SUBTEXT, labelsize=7)
        ax.spines[:].set_visible(False)
        ax.set_title("Top campañas — Msgs iniciados", color=TEXT, pad=8)
        for bar, val in zip(bars, vals):
            if val:
                ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height() / 2,
                        str(val), va="center", color=TEXT, fontsize=7)


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    app = Dashboard()
    app.mainloop()
