"""
Tablero de Contactos Nuevos — Crecelac  (Streamlit)
Página: Crecelac | Page ID: 1795816893869115

Uso:
    streamlit run new_contacts_streamlit.py
"""

import json
import calendar as _cal
from datetime import datetime, timedelta, timezone, date

import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.colors as mcolors
import requests

# ── Configuración ──────────────────────────────────────────────────────────────
PAGE_ID    = "1795816893869115"
BASE_URL   = "https://graph.facebook.com/v19.0"
CACHE_FILE = "new_contacts_cache.json"

# ── Paleta ─────────────────────────────────────────────────────────────────────
BG      = "#0f1117"
PANEL   = "#1a1f2e"
PANEL2  = "#242b3d"
TEXT    = "#e8eaf0"
SUBTEXT = "#6b7280"
ACCENT  = "#1e88e5"
GREEN   = "#43a047"
YELLOW  = "#f9a825"
RED     = "#e53935"

MONTHS_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
             "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
DAYS_ES   = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]


# ── Caché local ────────────────────────────────────────────────────────────────
def cache_load() -> dict:
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def cache_save(data: dict):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def cache_merge(old: dict, new_by_day: dict) -> dict:
    merged = dict(old.get("new_by_day", {}))
    merged.update(new_by_day)
    return merged


# ── API helpers ────────────────────────────────────────────────────────────────
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _tok(token: str, page_token: str) -> str:
    if page_token: return page_token
    if token:      return token
    raise RuntimeError("No se proporcionó Access Token.")


def api_get(endpoint: str, token: str, page_token: str, params: dict = None) -> dict:
    p = dict(params or {})
    p["access_token"] = _tok(token, page_token)
    r = requests.get(f"{BASE_URL}/{endpoint}", params=p, timeout=30)
    d = r.json()
    if "error" in d:
        raise RuntimeError(d["error"].get("message", str(d["error"])))
    return d


def api_paginate(endpoint: str, token: str, page_token: str, params: dict = None) -> list:
    results = []
    p = dict(params or {})
    p["access_token"] = _tok(token, page_token)
    url = f"{BASE_URL}/{endpoint}"
    while url:
        r = requests.get(url, params=p, timeout=30)
        d = r.json()
        if "error" in d:
            raise RuntimeError(d["error"].get("message", str(d["error"])))
        results.extend(d.get("data", []))
        url = d.get("paging", {}).get("next")
        p = {}
    return results


def resolve_page_token(token: str, logs: list) -> str:
    logs.append("Resolviendo Page Access Token…")

    r0 = requests.get(f"{BASE_URL}/{PAGE_ID}/conversations",
        params={"access_token": token, "limit": 1, "platform": "messenger"}, timeout=30)
    if "error" not in r0.json():
        logs.append("  ✓ Token válido como Page Token")
        return token

    r1 = requests.get(f"{BASE_URL}/{PAGE_ID}",
        params={"fields": "access_token,name", "access_token": token}, timeout=30)
    d1 = r1.json()
    if "access_token" in d1:
        logs.append(f"  ✓ Page Token resuelto para '{d1.get('name', PAGE_ID)}'")
        return d1["access_token"]

    r2 = requests.get(f"{BASE_URL}/me/accounts",
        params={"access_token": token, "limit": 100}, timeout=30)
    for pg in r2.json().get("data", []):
        if pg.get("id") == PAGE_ID:
            logs.append("  ✓ Page Token vía /me/accounts")
            return pg["access_token"]

    logs.append("  ⚠ Usando token original")
    return token


# ── Fetch ──────────────────────────────────────────────────────────────────────
def fetch_new_contacts(token: str, since_days: int = 90,
                        max_convs: int = 300) -> tuple[dict, list]:
    """Retorna (data_dict, log_lines)."""
    logs: list[str] = []
    today     = _now().date()
    today_str = today.strftime("%Y-%m-%d")
    since_dt  = _now() - timedelta(days=since_days)
    since_str = since_dt.strftime("%Y-%m-%d")

    cache = cache_load()
    cached_days = cache.get("new_by_day", {})

    if cached_days:
        last_cached = max(cached_days.keys())
        fetch_from = (datetime.strptime(last_cached, "%Y-%m-%d").date()
                      - timedelta(days=2)).strftime("%Y-%m-%d")
        logs.append(f"💾 Caché hasta {last_cached} — cargando desde {fetch_from}")
    else:
        fetch_from = since_str
        logs.append("💾 Sin caché previo — carga completa")

    fetch_from_dt  = datetime.strptime(fetch_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    fetch_since_ts = int(fetch_from_dt.timestamp())
    until_ts       = int(_now().timestamp())

    page_token = resolve_page_token(token, logs)
    new_by_day: dict = {}
    method_used = "desconocido"

    # Intento 1: Insights API
    logs.append("Consultando Insights API (nuevas conversaciones únicas)…")
    try:
        ins = api_get(f"{PAGE_ID}/insights", token, page_token, params={
            "metric": "page_messages_new_conversations_unique",
            "period": "day", "since": fetch_since_ts, "until": until_ts,
        })
        for item in ins.get("data", []):
            for v in item.get("values", []):
                day = v.get("end_time", "")[:10]
                val = v.get("value", 0)
                if isinstance(val, (int, float)) and fetch_from <= day <= today_str:
                    new_by_day[day] = int(val)
        if new_by_day:
            logs.append(f"  ✓ Insights: {sum(new_by_day.values())} contactos en "
                        f"{len(new_by_day)} días")
            method_used = "Insights API"
        else:
            logs.append("  ⚠ Insights sin valores — usando fallback")
    except RuntimeError as e:
        logs.append(f"  ⚠ Insights no disponible ({e}) — usando fallback")

    # Intento 2: Heurística message_count
    if not new_by_day:
        logs.append(f"Fallback: conversaciones desde {fetch_from} (máx. {max_convs})…")
        convs = api_paginate(f"{PAGE_ID}/conversations", token, page_token, params={
            "platform": "messenger",
            "fields": "id,updated_time,message_count",
            "limit": 100, "since": fetch_since_ts,
        })[:max_convs]

        MSG_THRESHOLD = 8
        new_convs = [c for c in convs if c.get("message_count", 99) <= MSG_THRESHOLD]
        old_convs = len(convs) - len(new_convs)
        logs.append(f"  → {len(convs)} convs · {len(new_convs)} nuevos "
                    f"(≤{MSG_THRESHOLD} msgs) · {old_convs} recurrentes ignorados")

        for conv in new_convs:
            day = conv.get("updated_time", "")[:10]
            if fetch_from <= day <= today_str:
                new_by_day[day] = new_by_day.get(day, 0) + 1

        method_used = "heurística message_count"
        logs.append(f"  ✓ {sum(new_by_day.values())} contactos nuevos")

    # Fusionar con caché y rellenar días con 0
    merged = cache_merge(cache, new_by_day)
    cur = since_dt.date()
    while cur <= today:
        merged.setdefault(cur.strftime("%Y-%m-%d"), 0)
        cur += timedelta(days=1)
    merged = dict(sorted(merged.items()))

    result = {
        "new_by_day" : merged,
        "method"     : method_used,
        "since_str"  : since_str,
        "since_days" : since_days,
        "generado_en": _now().strftime("%Y-%m-%d %H:%M UTC"),
    }
    cache_save(result)
    logs.append("  💾 Caché guardado")
    logs.append(f"✓ Total período: {sum(v for d,v in merged.items() if d >= since_str)} contactos nuevos")
    return result, logs


# ── Dibujo del mes ─────────────────────────────────────────────────────────────
def draw_month(ax, year: int, month: int, new_by_day: dict,
               max_val: int, active_range: tuple = None):
    ax.set_facecolor(BG)
    ax.set_xlim(0, 7)
    ax.set_ylim(-6.8, 1.4)
    ax.axis("off")
    ax.set_title(f"{MONTHS_ES[month]}  {year}",
                 color=TEXT, fontsize=12, fontweight="bold", pad=10)

    for col, name in enumerate(DAYS_ES):
        color = "#ef9a9a" if col >= 5 else SUBTEXT
        ax.text(col + 0.5, 0.9, name, ha="center", va="center",
                color=color, fontsize=8, fontweight="bold")

    cmap = mcolors.LinearSegmentedColormap.from_list(
        "nc",
        [(0.0, "#133020"), (0.25, "#1b5e34"), (0.55, "#2e7d52"),
         (0.80, "#43a047"), (1.0, "#81c784")],
    )

    first_weekday, num_days = _cal.monthrange(year, month)
    today = _now().date()
    day = 1

    for week in range(6):
        for weekday in range(7):
            if week == 0 and weekday < first_weekday:
                continue
            if day > num_days:
                break

            col   = weekday
            row   = -(week + 1)
            ds    = f"{year:04d}-{month:02d}-{day:02d}"
            count = new_by_day.get(ds, 0)
            d_obj = date(year, month, day)

            in_range = True
            if active_range:
                in_range = active_range[0] <= d_obj <= active_range[1]

            if not in_range:
                bg, alpha = PANEL, 0.5
            elif count > 0 and max_val > 0:
                bg = cmap(min(count / max_val, 1.0))
                alpha = 1.0
            else:
                bg, alpha = PANEL2, 1.0

            is_today = (d_obj == today)
            ec = YELLOW if is_today else ("none" if in_range else PANEL)
            lw = 2.2 if is_today else 0

            ax.add_patch(patches.FancyBboxPatch(
                (col + 0.06, row + 0.06), 0.88, 0.88,
                boxstyle="round,pad=0.05",
                facecolor=bg, edgecolor=ec, linewidth=lw, alpha=alpha,
            ))

            ax.text(col + 0.5, row + 0.74, str(day),
                    ha="center", va="center",
                    color=TEXT if in_range else SUBTEXT, fontsize=7.5,
                    fontweight="bold" if is_today else "normal")

            if count > 0 and in_range:
                ax.text(col + 0.5, row + 0.30, str(count),
                        ha="center", va="center",
                        color="white", fontsize=13, fontweight="bold")
            elif count > 0:
                ax.text(col + 0.5, row + 0.30, str(count),
                        ha="center", va="center", color=SUBTEXT, fontsize=8)
            day += 1


def build_figure(new_by_day: dict, view_mode: str, days_n: int,
                 view_year: int, view_month: int):
    """Construye la figura matplotlib y devuelve (fig, period_dict)."""
    today = _now().date()

    if view_mode == "days":
        start = today - timedelta(days=days_n - 1)
        months_list = []
        cur = start
        while cur <= today:
            m = (cur.year, cur.month)
            if m not in months_list:
                months_list.append(m)
            cur += timedelta(days=1)

        period = {d: v for d, v in new_by_day.items()
                  if start.strftime("%Y-%m-%d") <= d <= today.strftime("%Y-%m-%d")}
        max_val = max(period.values(), default=1) or 1

        n = len(months_list)
        fig, axes = plt.subplots(1, n, figsize=(7 * n, 6))
        if n == 1:
            axes = [axes]
        for ax, (yr, mo) in zip(axes, months_list):
            draw_month(ax, yr, mo, new_by_day, max_val, active_range=(start, today))

        title = (f"Últimos {days_n} días  ·  "
                 f"{sum(period.values())} contactos nuevos")
    else:
        _, num_days = _cal.monthrange(view_year, view_month)
        period = {
            f"{view_year:04d}-{view_month:02d}-{d:02d}":
            new_by_day.get(f"{view_year:04d}-{view_month:02d}-{d:02d}", 0)
            for d in range(1, num_days + 1)
        }
        max_val = max(period.values(), default=1) or 1

        fig, ax = plt.subplots(1, 1, figsize=(8, 6))
        draw_month(ax, view_year, view_month, new_by_day, max_val)
        title = (f"{MONTHS_ES[view_month]} {view_year}  ·  "
                 f"{sum(period.values())} contactos nuevos")

    fig.patch.set_facecolor(BG)
    fig.suptitle(title, color=TEXT, fontsize=13, fontweight="bold", y=0.99)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return fig, period


# ══════════════════════════════════════════════════════════════════════════════
#  Clasificación de mensajes
# ══════════════════════════════════════════════════════════════════════════════
CATEGORIAS: dict[str, list[str]] = {
    "Precio / Costo": [
        "precio", "costo", "cuánto cuesta", "cuanto cuesta", "cuánto vale",
        "cuanto vale", "cuánto es", "cuanto es", "cuánto cobran",
        "cuanto cobran", "cuánto sale", "cuanto sale", "tarifa",
        "cobran", "pesos", "dólares", "dolares", "quetzales",
    ],
    "Dónde comprar": [
        "dónde comprar", "donde comprar", "dónde lo venden", "donde lo venden",
        "dónde conseguir", "donde conseguir", "dónde adquirir", "donde adquirir",
        "dónde lo encuentro", "donde lo encuentro", "dónde lo hay",
        "donde lo hay", "dónde venden", "donde venden",
        "punto de venta", "tienda", "farmacia", "supermercado",
    ],
    "Beneficios / Para qué sirve": [
        "para qué sirve", "para que sirve", "beneficios", "propiedades",
        "qué hace", "que hace", "sirve para", "efectos", "resultado",
        "funciona para", "para qué es", "para que es", "qué es", "que es",
        "ayuda con", "ayuda a",
    ],
    "Cómo usar / Dosis": [
        "cómo se toma", "como se toma", "dosis", "cómo usar", "como usar",
        "instrucciones", "modo de uso", "cuánto tomar", "cuanto tomar",
        "cuántas veces", "cuantas veces", "cómo se usa", "como se usa",
        "cómo lo tomo", "como lo tomo", "cada cuánto", "cada cuanto",
    ],
    "Composición / Ingredientes": [
        "ingredientes", "composición", "composicion", "qué contiene",
        "que contiene", "qué tiene", "que tiene", "componentes",
        "natural", "fórmula", "formula",
    ],
    "Distribución / Mayoreo": [
        "distribuidor", "distribuidora", "distribución", "distribucion",
        "mayoreo", "mayorista", "por mayor", "revendedor", "reventa",
        "negocio", "empresa", "representante",
    ],
    "Disponibilidad / Stock": [
        "disponible", "hay stock", "tienen disponible", "agotado",
        "cuándo llega", "cuando llega", "cuándo hay", "cuando hay", "stock",
    ],
    "Efectos secundarios": [
        "efectos secundarios", "contraindicaciones", "reacciones",
        "alergia", "daña", "hace daño", "es seguro", "peligroso",
    ],
}


def classify_message(text: str) -> str:
    t = text.lower()
    for category, keywords in CATEGORIAS.items():
        if any(kw in t for kw in keywords):
            return category
    return "Otro / General"


_CATEGORIAS_LIST = list(CATEGORIAS.keys()) + ["Otro / General"]


def classify_batch_claude(messages: list[str], api_key: str) -> list[str]:
    """
    Clasifica una lista de mensajes usando Claude.
    Retorna una lista de categorías del mismo largo que `messages`.
    """
    import anthropic, json as _json

    cats_str = "\n".join(f"- {c}" for c in _CATEGORIAS_LIST)
    numbered = "\n".join(f"{i+1}. {m}" for i, m in enumerate(messages))

    prompt = f"""Eres un asistente que clasifica mensajes de nuevos clientes de un producto de salud/nutrición llamado Crecelac.

Categorías disponibles (elige EXACTAMENTE una de estas por mensaje):
{cats_str}

Mensajes a clasificar:
{numbered}

Instrucciones:
- Clasifica cada mensaje según la intención principal del cliente.
- Si el mensaje es solo un saludo ("Hola", "Buenos días", etc.) o expresa interés vago ("Me interesa", "Información"), clasifícalo como "Beneficios / Para qué sirve" si parece querer saber qué hace el producto, o como "Precio / Costo" si menciona querer comprarlo.
- Solo usa "Otro / General" si realmente no encaja en ninguna categoría.
- Responde ÚNICAMENTE con un JSON array de strings con exactamente {len(messages)} elementos, en el mismo orden que los mensajes.
- Ejemplo: ["Precio / Costo", "Beneficios / Para qué sirve", "Dónde comprar"]

JSON array:"""

    client = anthropic.Anthropic(api_key=api_key)
    resp = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = resp.content[0].text.strip()
    # Extraer el JSON array de la respuesta
    start = raw.find("[")
    end   = raw.rfind("]") + 1
    categories = _json.loads(raw[start:end])
    # Validar que cada categoría sea válida
    return [
        c if c in _CATEGORIAS_LIST else "Otro / General"
        for c in categories
    ]


def fetch_and_analyze_day(token: str, day_str: str,
                           max_convs: int = 200,
                           anthropic_key: str = "") -> tuple[list, list]:
    """
    Lee los primeros mensajes de nuevos contactos del día indicado y los clasifica.
    Retorna (results, logs)
    results = [{"motivo": str, "cantidad": int, "porcentaje": float}, ...]
    """
    from collections import Counter
    logs: list[str] = []

    day_dt     = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    since_ts   = int(day_dt.timestamp())

    page_token = resolve_page_token(token, logs)

    logs.append(f"Obteniendo conversaciones del {day_str}…")
    try:
        convs_raw = api_paginate(f"{PAGE_ID}/conversations", token, page_token, params={
            "platform": "messenger",
            "fields"  : "id,updated_time,message_count",
            "limit"   : 100,
            "since"   : since_ts,
        })
    except RuntimeError as e:
        # Intentar sin el filtro `since` y filtrar manualmente por fecha
        logs.append(f"  ⚠ Filtro 'since' falló ({e}), obteniendo todas y filtrando…")
        convs_raw = api_paginate(f"{PAGE_ID}/conversations", token, page_token, params={
            "platform": "messenger",
            "fields"  : "id,updated_time,message_count",
            "limit"   : 100,
        })

    MSG_THRESHOLD = 8
    day_convs = [
        c for c in convs_raw
        if c.get("updated_time", "")[:10] == day_str
        and c.get("message_count", 99) <= MSG_THRESHOLD
    ][:max_convs]

    logs.append(f"  → {len(convs_raw)} convs desde ese día · "
                f"{len(day_convs)} nuevos contactos a analizar")

    # Recopilar textos de mensajes
    texts: list[str] = []
    for conv in day_convs:
        try:
            msgs = api_paginate(f"{conv['id']}/messages", token, page_token,
                params={"fields": "message,created_time", "limit": 10})
            # Los mensajes vienen más nuevo→más viejo; los últimos son el inicio de la conv.
            first_msgs = msgs[-3:] if len(msgs) >= 3 else msgs
            combined   = " ".join(m.get("message", "") for m in first_msgs
                                   if m.get("message"))
            if combined.strip():
                texts.append(combined.strip())
        except RuntimeError:
            pass

    if not texts:
        logs.append("  ⚠ No se encontraron mensajes para clasificar")
        return [], logs

    # Clasificar con Claude si hay API key, si no usar palabras clave
    if anthropic_key:
        logs.append(f"  🤖 Clasificando {len(texts)} mensajes con Claude…")
        try:
            classified = classify_batch_claude(texts, anthropic_key)
            logs.append(f"  ✓ Clasificación con IA completada")
        except Exception as e:
            logs.append(f"  ⚠ Claude falló ({e}), usando palabras clave…")
            classified = [classify_message(t) for t in texts]
    else:
        logs.append(f"  📝 Clasificando con palabras clave (sin API key de IA)…")
        classified = [classify_message(t) for t in texts]

    counts = Counter(classified)
    total  = len(classified)
    results = [
        {"motivo": cat, "cantidad": cnt,
         "porcentaje": round(cnt / total * 100, 1)}
        for cat, cnt in sorted(counts.items(), key=lambda x: -x[1])
    ]
    logs.append(f"  ✓ {total} mensajes clasificados · {len(results)} categorías")
    return results, logs


# ══════════════════════════════════════════════════════════════════════════════
#  App Streamlit
# ══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(
        page_title="Contactos Nuevos — Crecelac",
        page_icon="📅",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # CSS complementario (los colores base vienen de .streamlit/config.toml)
    st.markdown("""
    <style>
      /* Tarjetas de métricas */
      [data-testid="stMetric"] {
        background-color: #1a1f2e;
        border-radius: 10px;
        padding: 16px 20px;
        border: 1px solid #2a3450;
      }
      div[data-testid="stMetricValue"] { font-size: 2.2rem; }
      div[data-testid="stMetricLabel"] { font-size: 0.85rem; opacity: 0.75; }

      /* Botones generales */
      .stButton > button {
        border-radius: 6px;
        font-weight: 600;
      }
      /* Botones de navegación de mes (◀ ▶) */
      [data-testid="column"]:first-child .stButton > button,
      [data-testid="column"]:last-child  .stButton > button {
        font-size: 1.6rem !important;
        padding: 6px 0 !important;
        background-color: #1a1f2e !important;
        border: 1px solid #2a3450 !important;
        color: #1e88e5 !important;
      }
      [data-testid="column"]:first-child .stButton > button:hover,
      [data-testid="column"]:last-child  .stButton > button:hover {
        background-color: #1e88e5 !important;
        color: white !important;
      }

      /* Radio buttons — etiquetas más visibles */
      [data-testid="stRadio"] label {
        font-size: 0.95rem !important;
        padding: 4px 0;
      }

      /* Sidebar items */
      [data-testid="stSidebar"] label,
      [data-testid="stSidebar"] p,
      [data-testid="stSidebar"] span {
        color: #e8eaf0 !important;
      }

      /* Select box texto */
      [data-testid="stSelectbox"] div {
        color: #e8eaf0;
      }

      /* Divider */
      hr { border-color: #2a3450; }
    </style>
    """, unsafe_allow_html=True)

    # ── Estado inicial ─────────────────────────────────────────────────────────
    if "data" not in st.session_state:
        cached = cache_load()
        st.session_state.data = cached if cached.get("new_by_day") else None

    for key, default in [("stored_token", ""), ("analysis_results", None),
                          ("analysis_date", None), ("analysis_logs", []),
                          ("anthropic_key", ""), ("sheets_status", None)]:
        if key not in st.session_state:
            st.session_state[key] = default

    # Leer clave de Anthropic desde secrets si existe
    if not st.session_state.anthropic_key:
        try:
            st.session_state.anthropic_key = st.secrets.get("anthropic_api_key", "")
        except Exception:
            pass

    for key, default in [("view_mode", "days"), ("days_n", 30),
                          ("view_year", _now().year), ("view_month", _now().month),
                          ("logs", [])]:
        if key not in st.session_state:
            st.session_state[key] = default

    # ── Sidebar ────────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## 📅 Contactos Nuevos")
        st.caption("Crecelac · Page 1795816893869115")
        st.divider()

        token = st.text_input("🔑 Access Token", type="password",
                               placeholder="Pega tu Page Access Token…",
                               help="Obtén uno en Meta for Developers → Graph API Explorer")

        max_convs = st.selectbox("Máx. convs (fallback)", [50, 100, 200, 500], index=2)

        if st.button("⟳  Cargar datos", use_container_width=True, type="primary"):
            if not token.strip():
                st.error("Pega un token primero.")
            else:
                with st.spinner("Cargando contactos nuevos…"):
                    try:
                        data, logs = fetch_new_contacts(
                            token.strip(), since_days=90, max_convs=max_convs)
                        st.session_state.data         = data
                        st.session_state.logs         = logs
                        st.session_state.stored_token = token.strip()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")

        st.divider()

        # ── Filtros ────────────────────────────────────────────────────────────
        st.markdown("**Período**")
        filtro = st.radio("", ["7 días", "14 días", "21 días", "28 días", "Por mes"],
                          index=2, label_visibility="collapsed")

        if filtro == "Por mes":
            st.session_state.view_mode = "month"
        else:
            st.session_state.view_mode = "days"
            st.session_state.days_n = int(filtro.split()[0])

        st.divider()

        # ── IA Clasificación ───────────────────────────────────────────────────
        st.markdown("**🤖 Clasificación con IA**")
        st.caption("Opcional — mejora la clasificación de mensajes")
        anthropic_input = st.text_input(
            "Anthropic API Key", type="password",
            placeholder="sk-ant-…",
            value=st.session_state.anthropic_key,
            help="Obtén una en console.anthropic.com. Si se configura en Secrets no es necesario pegarlo aquí.",
            key="anthropic_key_input",
        )
        if anthropic_input.strip():
            st.session_state.anthropic_key = anthropic_input.strip()

        st.divider()

        # ── Exportar ───────────────────────────────────────────────────────────
        if st.button("☁  Exportar a Google Sheets", use_container_width=True):
            if not st.session_state.data:
                st.error("Carga los datos primero.")
            else:
                with st.spinner("Exportando…"):
                    try:
                        from sheets_export import export_new_contacts
                        export_logs: list = []
                        export_new_contacts(st.session_state.data,
                                            log_cb=export_logs.append)
                        st.success("✓ Exportado a Google Sheets")
                        for line in export_logs:
                            st.caption(line)
                    except ImportError:
                        st.error("Instala: pip install gspread google-auth")
                    except Exception as e:
                        st.error(f"Error: {e}")

    # ── Área principal ─────────────────────────────────────────────────────────
    if not st.session_state.data:
        st.markdown("## 📅 Contactos Nuevos — Crecelac")
        st.info("Pega tu **Access Token** en la barra lateral y presiona **Cargar datos**.")
        st.markdown("""
        **¿Cómo obtener el token?**
        1. Ve a [Meta for Developers → Graph API Explorer](https://developers.facebook.com/tools/explorer/)
        2. Selecciona tu app → **Obtener token de acceso a la página** → Crecelac
        3. Pega el token aquí
        """)
        return

    data       = st.session_state.data
    new_by_day = data["new_by_day"]

    # Calcular período visible para stats
    today = _now().date()
    if st.session_state.view_mode == "days":
        n_d   = st.session_state.days_n
        start = today - timedelta(days=n_d - 1)
        period = {d: v for d, v in new_by_day.items()
                  if start.strftime("%Y-%m-%d") <= d <= today.strftime("%Y-%m-%d")}
    else:
        yr, mo = st.session_state.view_year, st.session_state.view_month
        _, num_days = _cal.monthrange(yr, mo)
        period = {f"{yr:04d}-{mo:02d}-{d:02d}":
                  new_by_day.get(f"{yr:04d}-{mo:02d}-{d:02d}", 0)
                  for d in range(1, num_days + 1)}

    # ── Header ─────────────────────────────────────────────────────────────────
    col_h, col_u = st.columns([3, 1])
    with col_h:
        st.markdown("## 📅 Contactos Nuevos — Crecelac")
    with col_u:
        st.caption(f"🕐 {data.get('generado_en', '')}")
        st.caption(f"⚙️ Método: {data.get('method', '')}")

    # ── KPIs ───────────────────────────────────────────────────────────────────
    total = sum(period.values())
    n_d   = len(period)
    avg   = round(total / max(n_d, 1), 1)
    best  = max(period, key=period.get, default=None)
    bval  = period.get(best, 0) if best else 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📬 Total nuevos",    total)
    c2.metric("📊 Promedio / día",  avg)
    c3.metric("🏆 Mejor día",       best[5:] if best else "—")
    c4.metric("⬆️ Máx. en un día",  bval)

    st.markdown("---")
    tab_cal, tab_ana = st.tabs(["📅  Calendario", "🔍  Análisis de Mensajes"])

    # ══════════════════════════════════════════════════════════════════════════
    with tab_ana:
        st.markdown("### 🔍 Análisis de mensajes por día")
        st.caption("Lee los primeros mensajes de nuevos contactos del día seleccionado "
                   "y los clasifica por motivo. Se guarda automáticamente en Google Sheets.")

        col_d, col_b = st.columns([2, 1])
        with col_d:
            sel_date = st.date_input(
                "Día a analizar",
                value=_now().date(),
                max_value=_now().date(),
                label_visibility="visible",
            )
        with col_b:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            run_btn = st.button("🔍  Analizar mensajes", use_container_width=True,
                                type="primary", key="btn_analyze")

        if run_btn:
            tok = st.session_state.get("stored_token", "")
            if not tok:
                st.error("Primero carga los datos con tu token (pestaña Calendario).")
            else:
                day_str = sel_date.strftime("%Y-%m-%d")
                try:
                    with st.spinner(f"Leyendo y clasificando mensajes del {day_str}…"):
                        results, ana_logs = fetch_and_analyze_day(
                            tok, day_str,
                            anthropic_key=st.session_state.get("anthropic_key", "")
                        )

                    st.session_state.analysis_results = results
                    st.session_state.analysis_date    = day_str
                    st.session_state.analysis_logs    = ana_logs

                    if results:
                        # Guardar en Sheets automáticamente
                        try:
                            from sheets_export import export_message_analysis
                            sheet_logs: list = []
                            export_message_analysis(day_str, results,
                                                    log_cb=sheet_logs.append)
                            st.session_state.sheets_status = ("ok", "✓ Guardado en Google Sheets (pestaña 'Análisis Mensajes')")
                        except Exception as e:
                            st.session_state.sheets_status = ("error", f"No se pudo guardar en Sheets: {e}")
                    else:
                        st.session_state.sheets_status = None

                    with st.expander("📋 Log de la consulta", expanded=False):
                        st.code("\n".join(ana_logs), language=None)

                except Exception as exc:
                    st.error(f"Error al analizar mensajes: {exc}")
                    st.info("Verifica que el token tenga permisos `pages_messaging` "
                            "y `pages_read_engagement`.")

        # Mostrar resultados guardados en session_state
        if st.session_state.analysis_results and st.session_state.analysis_date:
            results  = st.session_state.analysis_results
            day_str  = st.session_state.analysis_date
            total    = sum(r["cantidad"] for r in results)

            # Estado de la exportación a Sheets (persiste entre rerenders)
            if st.session_state.sheets_status:
                status_type, status_msg = st.session_state.sheets_status
                if status_type == "ok":
                    st.success(status_msg)
                else:
                    st.error(status_msg)

            st.markdown(f"#### Resultados — {day_str}  ·  {total} mensajes analizados")

            # Tabla principal
            st.dataframe(
                [{"Motivo de contacto": r["motivo"],
                  "Cantidad": r["cantidad"],
                  "% del total": f"{r['porcentaje']}%"}
                 for r in results],
                use_container_width=True,
                hide_index=True,
            )

            # Gráfico de barras horizontal
            import pandas as pd
            df = pd.DataFrame(results).set_index("motivo")[["cantidad"]]
            df.index.name = "Motivo"
            st.bar_chart(df, horizontal=True)

        elif not run_btn:
            st.info("Selecciona un día y presiona **Analizar mensajes** para ver "
                    "los motivos de contacto de ese día.")

    # ══════════════════════════════════════════════════════════════════════════
    with tab_cal:
        # ── Navegación de mes ─────────────────────────────────────────────────
        nav_l, nav_c, nav_r = st.columns([1, 4, 1])

        with nav_l:
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            if st.button("◀", key="prev_month", use_container_width=True,
                         help="Mes anterior"):
                st.session_state.view_mode = "month"
                if st.session_state.view_month == 1:
                    st.session_state.view_month = 12
                    st.session_state.view_year -= 1
                else:
                    st.session_state.view_month -= 1
                st.rerun()

        with nav_c:
            mo_name = MONTHS_ES[st.session_state.view_month]
            yr_val  = st.session_state.view_year
            st.markdown(
                f"<div style='text-align:center; font-size:1.6rem; font-weight:700;"
                f"color:#1e88e5; padding:4px 0;'>"
                f"{mo_name} &nbsp; {yr_val}</div>",
                unsafe_allow_html=True,
            )

        with nav_r:
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)
            if st.button("▶", key="next_month", use_container_width=True,
                         help="Mes siguiente"):
                st.session_state.view_mode = "month"
                if st.session_state.view_month == 12:
                    st.session_state.view_month = 1
                    st.session_state.view_year += 1
                else:
                    st.session_state.view_month += 1
                st.rerun()

        # ── Calendario ────────────────────────────────────────────────────────
        fig, _ = build_figure(
            new_by_day,
            st.session_state.view_mode,
            st.session_state.days_n,
            st.session_state.view_year,
            st.session_state.view_month,
        )
        st.pyplot(fig, use_container_width=True)
        plt.close(fig)

        # ── Tabla de datos ────────────────────────────────────────────────────
        with st.expander("📋 Ver datos en tabla"):
            rows = [{"Fecha": d, "Nuevos contactos": v}
                    for d, v in sorted(period.items(), reverse=True) if v > 0]
            if rows:
                st.dataframe(rows, use_container_width=True, hide_index=True)
            else:
                st.info("Sin datos en el período seleccionado.")

        # ── Log de carga ──────────────────────────────────────────────────────
        if st.session_state.logs:
            with st.expander("🔍 Log de la última carga"):
                st.code("\n".join(st.session_state.logs), language=None)


if __name__ == "__main__":
    main()
