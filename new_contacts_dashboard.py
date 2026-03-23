"""
Tablero de Contactos Nuevos — Crecelac
Página: Crecelac | Page ID: 1795816893869115

Calendario con número de contactos nuevos (conversaciones nuevas) por día.
Filtros: últimos 7 · 14 · 21 · 28 días, o navegación por mes.

Uso:
    python new_contacts_dashboard.py
"""

import json
import threading
from datetime import datetime, timedelta, timezone, date
import calendar as _cal

import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib
matplotlib.use("TkAgg")
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.colors as mcolors
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import requests

# ── Configuración ──────────────────────────────────────────────────────────────
PAGE_ID    = "1795816893869115"
BASE_URL   = "https://graph.facebook.com/v19.0"
CACHE_FILE = "new_contacts_cache.json"

_token      = [""]
_page_token = [""]


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
    """Fusiona datos nuevos con los del caché. Los días nuevos sobreescriben."""
    merged = dict(old.get("new_by_day", {}))
    merged.update(new_by_day)          # días frescos sobreescriben
    return merged

# ── Paleta ─────────────────────────────────────────────────────────────────────
BG      = "#0f1117"
PANEL   = "#1a1f2e"
PANEL2  = "#242b3d"
TEXT    = "#e8eaf0"
SUBTEXT = "#6b7280"
ACCENT  = "#1e88e5"
ACCENT2 = "#0288d1"
GREEN   = "#43a047"
YELLOW  = "#f9a825"
RED     = "#e53935"

MONTHS_ES = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
             "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]
DAYS_ES   = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]


# ── API helpers ────────────────────────────────────────────────────────────────
def _now() -> datetime:
    return datetime.now(timezone.utc)


def _tok() -> str:
    pt = _page_token[0].strip()
    if pt: return pt
    ut = _token[0].strip()
    if not ut:
        raise RuntimeError("No se proporcionó Access Token.")
    return ut


def api_get(endpoint: str, params: dict = None) -> dict:
    p = dict(params or {})
    p["access_token"] = _tok()
    r = requests.get(f"{BASE_URL}/{endpoint}", params=p, timeout=30)
    d = r.json()
    if "error" in d:
        raise RuntimeError(d["error"].get("message", str(d["error"])))
    return d


def api_paginate(endpoint: str, params: dict = None) -> list:
    results = []
    p = dict(params or {})
    p["access_token"] = _tok()
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


def resolve_page_token(log_cb):
    user_tok = _token[0].strip()
    log_cb("Resolviendo Page Access Token…")

    # Estrategia 0: ya es Page Token válido
    r0 = requests.get(
        f"{BASE_URL}/{PAGE_ID}/conversations",
        params={"access_token": user_tok, "limit": 1, "platform": "messenger"},
        timeout=30,
    )
    if "error" not in r0.json():
        _page_token[0] = user_tok
        log_cb("  ✓ Token válido como Page Token")
        return

    # Estrategia 1: resolver vía campo access_token del PAGE_ID
    r1 = requests.get(
        f"{BASE_URL}/{PAGE_ID}",
        params={"fields": "access_token,name", "access_token": user_tok},
        timeout=30,
    )
    d1 = r1.json()
    if "access_token" in d1:
        _page_token[0] = d1["access_token"]
        log_cb(f"  ✓ Page Token resuelto para '{d1.get('name', PAGE_ID)}'")
        return

    # Estrategia 2: /me/accounts
    r2 = requests.get(
        f"{BASE_URL}/me/accounts",
        params={"access_token": user_tok, "limit": 100},
        timeout=30,
    )
    for pg in r2.json().get("data", []):
        if pg.get("id") == PAGE_ID:
            _page_token[0] = pg["access_token"]
            log_cb("  ✓ Page Token vía /me/accounts")
            return

    _page_token[0] = user_tok
    log_cb("  ⚠ Usando token original")


# ── Fetch de contactos nuevos ──────────────────────────────────────────────────
def fetch_new_contacts(log_cb, since_days: int = 90, max_convs: int = 300) -> dict:
    """
    Devuelve new_by_day: {"YYYY-MM-DD": count, ...}

    - Carga el caché local al inicio y solo pide a la API los días que faltan
      (desde el último fetch hasta hoy).
    - Estrategia 1 (rápida): Insights API — page_messages_new_conversations_unique
    - Estrategia 2 (fallback): análisis de conversaciones, solo para días nuevos.
    """
    today     = _now().date()
    today_str = today.strftime("%Y-%m-%d")
    since_dt  = _now() - timedelta(days=since_days)
    since_str = since_dt.strftime("%Y-%m-%d")

    # ── Caché ─────────────────────────────────────────────────────────────────
    cache = cache_load()
    cached_days: dict = cache.get("new_by_day", {})

    if cached_days:
        # El caché ya tiene días; solo pedimos desde el último día guardado
        last_cached = max(cached_days.keys())
        # Recargar los últimos 2 días por si cambiaron (datos de ayer pueden llegar tarde)
        fetch_from = (datetime.strptime(last_cached, "%Y-%m-%d").date()
                      - timedelta(days=2)).strftime("%Y-%m-%d")
        log_cb(f"💾 Caché encontrado hasta {last_cached} — solo se pedirán datos desde {fetch_from}")
    else:
        fetch_from = since_str
        log_cb("💾 Sin caché previo — carga completa")

    fetch_from_dt  = datetime.strptime(fetch_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    fetch_since_ts = int(fetch_from_dt.timestamp())
    until_ts       = int(_now().timestamp())

    resolve_page_token(log_cb)

    new_by_day: dict = {}
    method_used = "desconocido"

    # ── Intento 1: Insights API ────────────────────────────────────────────────
    log_cb("Consultando Insights API (nuevas conversaciones únicas)…")
    try:
        ins = api_get(
            f"{PAGE_ID}/insights",
            params={
                "metric": "page_messages_new_conversations_unique",
                "period": "day",
                "since" : fetch_since_ts,
                "until" : until_ts,
            },
        )
        for item in ins.get("data", []):
            for v in item.get("values", []):
                day = v.get("end_time", "")[:10]
                val = v.get("value", 0)
                if isinstance(val, (int, float)) and fetch_from <= day <= today_str:
                    new_by_day[day] = int(val)
        if new_by_day:
            log_cb(f"  ✓ Insights: {sum(new_by_day.values())} contactos nuevos "
                   f"en {len(new_by_day)} días nuevos")
            method_used = "Insights API"
        else:
            log_cb("  ⚠ Insights sin valores — probando fallback")
    except RuntimeError as e:
        log_cb(f"  ⚠ Insights no disponible ({e}) — usando fallback")

    # ── Intento 2: Análisis de conversaciones (solo días nuevos) ──────────────
    if not new_by_day:
        log_cb(f"Fallback: analizando conversaciones desde {fetch_from} (máx. {max_convs})…")

        convs = api_paginate(
            f"{PAGE_ID}/conversations",
            params={
                "platform": "messenger",
                "fields"  : "id,updated_time,message_count",
                "limit"   : 100,
                "since"   : fetch_since_ts,        # ← solo convs del período nuevo
            },
        )[:max_convs]
        log_cb(f"  → {len(convs)} conversaciones en el período pendiente")

        # Heurística rápida: si una conversación tiene pocos mensajes Y fue
        # actualizada dentro del período, casi seguro es un contacto nuevo.
        # Conversaciones con muchos mensajes (> MSG_THRESHOLD) son contactos
        # recurrentes que volvieron a escribir — no se cuentan como nuevos.
        MSG_THRESHOLD = 8   # ajusta según tu caso de uso

        new_convs   = [c for c in convs if c.get("message_count", 99) <= MSG_THRESHOLD]
        old_convs   = [c for c in convs if c.get("message_count", 99) >  MSG_THRESHOLD]
        log_cb(f"  → {len(new_convs)} probablemente nuevos (≤{MSG_THRESHOLD} msgs) · "
               f"{len(old_convs)} recurrentes ignorados")

        # Para los "probablemente nuevos", updated_time ≈ fecha del primer mensaje
        for conv in new_convs:
            day = conv.get("updated_time", "")[:10]
            if fetch_from <= day <= today_str:
                new_by_day[day] = new_by_day.get(day, 0) + 1

        method_used = "heurística message_count"
        log_cb(f"  ✓ {sum(new_by_day.values())} contactos nuevos (sin leer mensajes)")

    # ── Fusionar con caché y rellenar días faltantes con 0 ────────────────────
    merged = cache_merge(cache, new_by_day)

    # Asegurar que todos los días del período tengan entrada (aunque sea 0)
    cur = since_dt.date()
    while cur <= today:
        ds = cur.strftime("%Y-%m-%d")
        merged.setdefault(ds, 0)
        cur += timedelta(days=1)

    merged = dict(sorted(merged.items()))
    total_all = sum(v for d, v in merged.items() if d >= since_str)
    log_cb(f"✓ Total en período: {total_all} contactos nuevos · método: {method_used}")

    result = {
        "new_by_day" : merged,
        "method"     : method_used,
        "since_str"  : since_str,
        "since_days" : since_days,
        "generado_en": _now().strftime("%Y-%m-%d %H:%M UTC"),
    }

    # Guardar caché actualizado
    cache_save(result)
    log_cb(f"  💾 Caché guardado en '{CACHE_FILE}'")

    return result


# ── Dibujo del mes ─────────────────────────────────────────────────────────────
def draw_month(ax, year: int, month: int, new_by_day: dict,
               max_val: int, active_range: tuple = None):
    """
    Dibuja un calendario mensual como heatmap en el Axes dado.
    active_range: (date, date) — solo esos días se colorean como activos.
    """
    ax.set_facecolor(BG)
    ax.set_xlim(0, 7)
    ax.set_ylim(-6.8, 1.4)
    ax.axis("off")

    # Título del mes
    ax.set_title(f"{MONTHS_ES[month]}  {year}",
                 color=TEXT, fontsize=12, fontweight="bold", pad=10)

    # Encabezados de día
    for col, name in enumerate(DAYS_ES):
        color = "#ef9a9a" if col >= 5 else SUBTEXT
        ax.text(col + 0.5, 0.9, name, ha="center", va="center",
                color=color, fontsize=8, fontweight="bold")

    # Colormap: verde oscuro → verde brillante
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

            col = weekday
            row = -(week + 1)
            ds  = f"{year:04d}-{month:02d}-{day:02d}"
            count = new_by_day.get(ds, 0)
            d_obj = date(year, month, day)

            # ¿Está en el rango activo?
            in_range = True
            if active_range:
                in_range = active_range[0] <= d_obj <= active_range[1]

            # Color de fondo del día
            if not in_range:
                bg = PANEL
                alpha = 0.5
            elif count > 0 and max_val > 0:
                intensity = min(count / max_val, 1.0)
                bg = cmap(intensity)
                alpha = 1.0
            else:
                bg = PANEL2
                alpha = 1.0

            is_today = (d_obj == today)
            ec = YELLOW if is_today else ("none" if in_range else PANEL)
            lw = 2.2 if is_today else 0

            rect = patches.FancyBboxPatch(
                (col + 0.06, row + 0.06), 0.88, 0.88,
                boxstyle="round,pad=0.05",
                facecolor=bg, edgecolor=ec,
                linewidth=lw, alpha=alpha,
            )
            ax.add_patch(rect)

            # Número del día (arriba)
            num_color = TEXT if in_range else SUBTEXT
            ax.text(col + 0.5, row + 0.74, str(day),
                    ha="center", va="center",
                    color=num_color, fontsize=7.5,
                    fontweight="bold" if is_today else "normal")

            # Cantidad de nuevos contactos (centro-abajo, grande)
            if count > 0 and in_range:
                cnt_color = "white"
                ax.text(col + 0.5, row + 0.30, str(count),
                        ha="center", va="center",
                        color=cnt_color, fontsize=13, fontweight="bold")
            elif count > 0 and not in_range:
                ax.text(col + 0.5, row + 0.30, str(count),
                        ha="center", va="center",
                        color=SUBTEXT, fontsize=8)

            day += 1


# ══════════════════════════════════════════════════════════════════════════════
#  Dashboard
# ══════════════════════════════════════════════════════════════════════════════
class NewContactsDashboard(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Contactos Nuevos — Crecelac")
        self.configure(bg=BG)
        self.state("zoomed")
        self.data = None

        self._view_mode  = "days"       # "days" | "month"
        self._days_n     = 30
        self._view_year  = _now().year
        self._view_month = _now().month

        self._build_header()
        self._build_filter_bar()
        self._build_stats_bar()
        self._build_chart_area()
        self._build_log()

        # Mostrar caché al abrir (sin llamadas a la API)
        self.after(100, self._load_cache_on_start)

    def _load_cache_on_start(self):
        cached = cache_load()
        if not cached or "new_by_day" not in cached:
            self._log("Sin caché — pega tu token y presiona «Cargar datos».", "info")
            return
        last = cached.get("generado_en", "?")
        self._log(f"💾 Caché cargado (datos del {last}) — actualiza para obtener datos nuevos", "ok")
        self._finish_fetch(cached)

    # ── Header ────────────────────────────────────────────────────────────────
    def _build_header(self):
        hdr = tk.Frame(self, bg=PANEL, height=52)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        tk.Label(hdr, text="  📅  Contactos Nuevos",
                 font=("Segoe UI", 16, "bold"), bg=PANEL, fg=TEXT).pack(side="left", padx=16)
        tk.Label(hdr, text="Crecelac · Page 1795816893869115",
                 font=("Segoe UI", 9), bg=PANEL, fg=SUBTEXT).pack(side="left", padx=4)

        self.btn_load = tk.Button(
            hdr, text="⟳  Cargar datos",
            font=("Segoe UI", 10, "bold"),
            bg=ACCENT, fg="white",
            activebackground="#1565c0", activeforeground="white",
            relief="flat", padx=16, pady=4, cursor="hand2",
            command=self._start_fetch,
        )
        self.btn_load.pack(side="right", padx=4, pady=8)

        self.btn_sheets = tk.Button(
            hdr, text="☁  Sheets",
            font=("Segoe UI", 10, "bold"),
            bg="#1e7e34", fg="white",
            activebackground="#155724", activeforeground="white",
            relief="flat", padx=14, pady=4, cursor="hand2",
            command=self._export_sheets,
        )
        self.btn_sheets.pack(side="right", padx=4, pady=8)

        self.lbl_update = tk.Label(hdr, text="", font=("Segoe UI", 9), bg=PANEL, fg=SUBTEXT)
        self.lbl_update.pack(side="right", padx=8)

        # Barra de token
        tok_bar = tk.Frame(self, bg="#111827", height=36)
        tok_bar.pack(fill="x")
        tok_bar.pack_propagate(False)

        tk.Label(tok_bar, text="  🔑 Access Token:",
                 font=("Segoe UI", 9, "bold"), bg="#111827", fg=YELLOW,
                 ).pack(side="left", padx=(12, 4))

        self.token_var = tk.StringVar()
        te = tk.Entry(tok_bar, textvariable=self.token_var,
                      font=("Consolas", 8), bg="#1e2536", fg=TEXT,
                      insertbackground=TEXT, relief="flat", show="•")
        te.pack(side="left", fill="x", expand=True, padx=4, pady=5)

        self._tok_vis = False
        def toggle_vis():
            self._tok_vis = not self._tok_vis
            te.config(show="" if self._tok_vis else "•")
        tk.Button(tok_bar, text="👁", font=("Segoe UI", 9), bg="#111827", fg=SUBTEXT,
                  relief="flat", cursor="hand2", activebackground="#111827",
                  command=toggle_vis).pack(side="left", padx=2)

        tk.Label(tok_bar, text="  │  Máx. convs (fallback):",
                 font=("Segoe UI", 8), bg="#111827", fg=SUBTEXT).pack(side="left", padx=(16, 2))
        self.maxconv_var = tk.StringVar(value="200")
        ttk.Combobox(tok_bar, textvariable=self.maxconv_var,
                     values=["50", "100", "200", "500"], width=5,
                     state="readonly", font=("Segoe UI", 8)
                     ).pack(side="left", padx=2, pady=5)

    # ── Filtros ───────────────────────────────────────────────────────────────
    def _build_filter_bar(self):
        bar = tk.Frame(self, bg=PANEL2, height=46)
        bar.pack(fill="x")
        bar.pack_propagate(False)

        # Botones de últimos N días
        tk.Label(bar, text="  Últimos:", font=("Segoe UI", 9, "bold"),
                 bg=PANEL2, fg=TEXT).pack(side="left", padx=(12, 2))

        self._day_btns: dict = {}
        for n in [7, 14, 21, 28]:
            btn = tk.Button(
                bar, text=f"{n} días",
                font=("Segoe UI", 9), relief="flat", cursor="hand2",
                padx=14, pady=5,
                command=lambda n=n: self._switch_days(n),
            )
            btn.pack(side="left", padx=3, pady=7)
            self._day_btns[n] = btn

        # Separador
        tk.Label(bar, text="  │", font=("Segoe UI", 10),
                 bg=PANEL2, fg=SUBTEXT).pack(side="left", padx=(12, 4))

        # Navegación por mes
        tk.Label(bar, text="Por mes:", font=("Segoe UI", 9, "bold"),
                 bg=PANEL2, fg=TEXT).pack(side="left", padx=(0, 4))

        tk.Button(bar, text="◀", font=("Segoe UI", 11), bg=PANEL2, fg=TEXT,
                  relief="flat", cursor="hand2", padx=8,
                  activebackground=PANEL, command=self._prev_month
                  ).pack(side="left", padx=2)

        self.lbl_month = tk.Label(bar, text="", width=16, anchor="center",
                                   font=("Segoe UI", 10, "bold"), bg=PANEL2, fg=SUBTEXT)
        self.lbl_month.pack(side="left", padx=2)

        tk.Button(bar, text="▶", font=("Segoe UI", 11), bg=PANEL2, fg=TEXT,
                  relief="flat", cursor="hand2", padx=8,
                  activebackground=PANEL, command=self._next_month
                  ).pack(side="left", padx=2)

        self._update_filter_ui()

    def _update_filter_ui(self):
        for n, btn in self._day_btns.items():
            active = self._view_mode == "days" and self._days_n == n
            btn.config(
                bg=ACCENT  if active else PANEL,
                fg="white" if active else SUBTEXT,
                activebackground=ACCENT if active else PANEL,
                activeforeground="white" if active else TEXT,
            )
        self.lbl_month.config(
            text=f"{MONTHS_ES[self._view_month]} {self._view_year}",
            fg=ACCENT if self._view_mode == "month" else SUBTEXT,
        )

    def _switch_days(self, n: int):
        self._view_mode = "days"
        self._days_n = n
        self._update_filter_ui()
        if self.data:
            self._draw_calendar()

    def _switch_month(self):
        self._view_mode = "month"
        self._update_filter_ui()
        if self.data:
            self._draw_calendar()

    def _prev_month(self):
        self._view_mode = "month"
        if self._view_month == 1:
            self._view_month, self._view_year = 12, self._view_year - 1
        else:
            self._view_month -= 1
        self._update_filter_ui()
        if self.data:
            self._draw_calendar()

    def _next_month(self):
        self._view_mode = "month"
        if self._view_month == 12:
            self._view_month, self._view_year = 1, self._view_year + 1
        else:
            self._view_month += 1
        self._update_filter_ui()
        if self.data:
            self._draw_calendar()

    # ── Estadísticas ──────────────────────────────────────────────────────────
    def _build_stats_bar(self):
        bar = tk.Frame(self, bg=PANEL, height=62)
        bar.pack(fill="x")
        bar.pack_propagate(False)

        self._stat_vars: dict = {}
        defs = [
            ("total",    "Total nuevos",      ACCENT),
            ("avg",      "Promedio / día",     GREEN),
            ("best_day", "Mejor día",          YELLOW),
            ("best_val", "Máx. en un día",     RED),
        ]
        for key, label, color in defs:
            f = tk.Frame(bar, bg=PANEL)
            f.pack(side="left", expand=True, fill="both")
            tk.Frame(f, bg=color, height=3).pack(fill="x")
            tk.Label(f, text=label, font=("Segoe UI", 8),
                     bg=PANEL, fg=SUBTEXT).pack(pady=(6, 0))
            v = tk.StringVar(value="—")
            self._stat_vars[key] = v
            tk.Label(f, textvariable=v, font=("Segoe UI", 17, "bold"),
                     bg=PANEL, fg=color).pack(pady=(0, 6))

    def _update_stats(self, period_days: dict):
        vals = [v for v in period_days.values() if v > 0]
        total = sum(period_days.values())
        n     = len(period_days)
        avg   = round(total / max(n, 1), 1)
        best  = max(period_days, key=period_days.get, default=None)
        bval  = period_days.get(best, 0) if best else 0

        self._stat_vars["total"].set(str(total))
        self._stat_vars["avg"].set(str(avg))
        # Mostrar solo MM-DD para el mejor día
        self._stat_vars["best_day"].set(best[5:] if best else "—")
        self._stat_vars["best_val"].set(str(bval))

    # ── Área de gráfico ────────────────────────────────────────────────────────
    def _build_chart_area(self):
        self.chart_frame = tk.Frame(self, bg=BG)
        self.chart_frame.pack(fill="both", expand=True, padx=6, pady=6)

        self.fig = plt.Figure(facecolor=BG)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_frame)
        self.canvas.get_tk_widget().pack(fill="both", expand=True)

        # Pantalla inicial
        ax = self.fig.add_subplot(1, 1, 1)
        ax.set_facecolor(BG)
        ax.axis("off")
        ax.text(0.5, 0.58, "📅", ha="center", va="center",
                fontsize=72, color=ACCENT, transform=ax.transAxes)
        ax.text(0.5, 0.42, "Pega tu Access Token y presiona «Cargar datos»",
                ha="center", va="center", fontsize=12, color=SUBTEXT,
                transform=ax.transAxes)
        ax.text(0.5, 0.34, "Se mostrará el calendario de contactos nuevos por día",
                ha="center", va="center", fontsize=10, color=SUBTEXT,
                alpha=0.6, transform=ax.transAxes)
        self.canvas.draw()

    # ── Log ───────────────────────────────────────────────────────────────────
    def _build_log(self):
        log_frame = tk.Frame(self, bg=PANEL)
        log_frame.pack(fill="x", side="bottom")

        self._pbar_frame = tk.Frame(log_frame, bg=PANEL, height=4)
        self._pbar_frame.pack(fill="x")
        self._pbar_frame.pack_propagate(False)
        s = ttk.Style()
        s.configure("L.Horizontal.TProgressbar",
                    troughcolor=PANEL, background=ACCENT, thickness=4, borderwidth=0)
        self.pbar = ttk.Progressbar(self._pbar_frame, mode="indeterminate",
                                     style="L.Horizontal.TProgressbar")

        title_bar = tk.Frame(log_frame, bg="#111827", height=26)
        title_bar.pack(fill="x")
        title_bar.pack_propagate(False)

        self.status_var = tk.StringVar(value="Listo — pega tu token y presiona «Cargar datos»")
        tk.Label(title_bar, textvariable=self.status_var, font=("Consolas", 8),
                 bg="#111827", fg=SUBTEXT, anchor="w"
                 ).pack(side="left", fill="x", expand=True, padx=8)

        self._log_visible = True
        btn_tog = tk.Button(title_bar, text="▼ Log", font=("Segoe UI", 8),
                            bg="#111827", fg=SUBTEXT, relief="flat", cursor="hand2",
                            activebackground="#111827", activeforeground=TEXT)
        btn_tog.pack(side="right", padx=6)

        self._log_box = tk.Frame(log_frame, bg="#0b0e17", height=110)
        self._log_box.pack(fill="x")
        self._log_box.pack_propagate(False)

        log_sb = ttk.Scrollbar(self._log_box, orient="vertical")
        log_sb.pack(side="right", fill="y")
        self.log_text = tk.Text(
            self._log_box, font=("Consolas", 9), bg="#0b0e17", fg="#a8d8a8",
            relief="flat", state="disabled", wrap="word", yscrollcommand=log_sb.set,
        )
        self.log_text.pack(fill="both", expand=True, padx=4, pady=2)
        log_sb.config(command=self.log_text.yview)
        self.log_text.tag_config("error", foreground=RED)
        self.log_text.tag_config("ok",    foreground=GREEN)
        self.log_text.tag_config("info",  foreground="#a8d8a8")
        self.log_text.tag_config("step",  foreground=ACCENT2)

        def toggle_log():
            if self._log_visible:
                self._log_box.pack_forget()
                btn_tog.config(text="▲ Log")
            else:
                self._log_box.pack(fill="x")
                btn_tog.config(text="▼ Log")
            self._log_visible = not self._log_visible
        btn_tog.config(command=toggle_log)

        self._log("Sistema listo — pega tu token y presiona «Cargar datos».", "info")

    def _log(self, msg: str, tag: str = "info"):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.config(state="normal")
        self.log_text.insert("end", f"[{ts}] {msg}\n", tag)
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
            from sheets_export import export_new_contacts
            export_new_contacts(self.data, log_cb=log)
        except ImportError:
            log("✗ Instala las dependencias: pip install gspread google-auth")
        except Exception as e:
            log(f"✗ Error al exportar: {e}")
        finally:
            self.after(0, lambda: self.btn_sheets.config(state="normal", text="☁  Sheets"))

    # ── Carga de datos ────────────────────────────────────────────────────────
    def _start_fetch(self):
        tok = self.token_var.get().strip()
        if not tok:
            messagebox.showwarning(
                "Token requerido",
                "Pega un Page Access Token válido en el campo superior.\n\n"
                "Obtén uno en: Meta for Developers → Graph API Explorer\n"
                "→ «Obtener token de acceso a la página» → Crecelac",
            )
            return
        _token[0] = tok
        _page_token[0] = ""
        self.btn_load.config(state="disabled")
        self.pbar.pack(fill="x")
        self.pbar.start(12)
        self._spinner_idx = 0
        self._spin()
        self._log("━━━ Iniciando carga de contactos nuevos ━━━", "step")
        threading.Thread(target=self._fetch_thread, daemon=True).start()

    def _spin(self):
        if self.btn_load["state"] == "disabled":
            chars = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
            self.btn_load.config(text=f"{chars[self._spinner_idx % len(chars)]}  Cargando…")
            self._spinner_idx += 1
            self.after(100, self._spin)

    def _fetch_thread(self):
        def log(msg: str):
            tag = ("error" if any(w in msg.lower() for w in ["error", "fallo", "⚠", "✗"])
                   else "ok" if msg.startswith(("✓", "━", "  ✓", "  →"))
                   else "info")
            self.after(0, self._log, msg, tag)

        try:
            # Se cargan 90 días para poder navegar varios meses atrás
            data = fetch_new_contacts(
                log_cb=log,
                since_days=90,
                max_convs=int(self.maxconv_var.get()),
            )
            self.after(0, self._log, "━━━ Carga completada ━━━", "ok")
            self.after(0, self._finish_fetch, data)
        except Exception as e:
            self.after(0, self._log, f"✗ ERROR: {e}", "error")
            if self.data is None:
                self.after(0, messagebox.showerror, "Error de API", str(e))
        finally:
            self.after(0, self._stop_loading)

    def _stop_loading(self):
        self.pbar.stop()
        self.pbar.pack_forget()
        self.btn_load.config(state="normal", text="⟳  Cargar datos")

    def _finish_fetch(self, data: dict):
        self.data = data
        self.lbl_update.config(text=f"Actualizado: {data['generado_en']}")
        self._draw_calendar()

    # ── Dibujo del calendario ─────────────────────────────────────────────────
    def _get_period_days(self) -> dict:
        """Retorna el subconjunto de días a mostrar según el filtro activo."""
        if not self.data:
            return {}
        nbd = self.data["new_by_day"]
        today = _now().date()

        if self._view_mode == "days":
            start = today - timedelta(days=self._days_n - 1)
            s = start.strftime("%Y-%m-%d")
            t = today.strftime("%Y-%m-%d")
            return {d: v for d, v in nbd.items() if s <= d <= t}
        else:
            yr, mo = self._view_year, self._view_month
            _, num_days = _cal.monthrange(yr, mo)
            return {
                f"{yr:04d}-{mo:02d}-{day:02d}":
                nbd.get(f"{yr:04d}-{mo:02d}-{day:02d}", 0)
                for day in range(1, num_days + 1)
            }

    def _draw_calendar(self):
        if not self.data:
            return

        nbd      = self.data["new_by_day"]
        period   = self._get_period_days()
        today    = _now().date()
        max_val  = max(period.values(), default=1)
        if max_val == 0:
            max_val = 1

        self._update_stats(period)
        self.fig.clear()
        self.fig.patch.set_facecolor(BG)

        if self._view_mode == "days":
            # Determinar qué meses cubre el período
            start_date = today - timedelta(days=self._days_n - 1)
            months = []
            cur = start_date
            while cur <= today:
                m = (cur.year, cur.month)
                if m not in months:
                    months.append(m)
                cur += timedelta(days=1)

            n_months = len(months)
            for i, (yr, mo) in enumerate(months):
                ax = self.fig.add_subplot(1, n_months, i + 1)
                draw_month(ax, yr, mo, nbd, max_val,
                           active_range=(start_date, today))

            label = f"Últimos {self._days_n} días  ·  {sum(period.values())} contactos nuevos"
            self.fig.suptitle(label, color=TEXT, fontsize=12, fontweight="bold", y=0.99)

        else:
            ax = self.fig.add_subplot(1, 1, 1)
            draw_month(ax, self._view_year, self._view_month, nbd, max_val)
            total = sum(period.values())
            label = (f"{MONTHS_ES[self._view_month]} {self._view_year}"
                     f"  ·  {total} contactos nuevos")
            self.fig.suptitle(label, color=TEXT, fontsize=12, fontweight="bold", y=0.99)

        self.fig.tight_layout(rect=[0, 0, 1, 0.97])
        self.canvas.draw()


# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = NewContactsDashboard()
    app.mainloop()
