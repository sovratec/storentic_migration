"""
app.py — Storentic Migration Flask UI
Professional redesign: sidebar layout, Storentic branding, all 14 migration scripts,
config manager, skipped-rows viewer, DB-ping endpoint.
"""

import glob
import json
import os
import queue
import shutil
import subprocess
import sys
import threading
import uuid
from datetime import date, datetime, timedelta
from functools import wraps
from urllib.parse import quote_plus

from dotenv import load_dotenv
from flask import (Flask, Response, flash, jsonify, redirect,
                   render_template, request, send_file, session,
                   stream_with_context, url_for)

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET", "change-me-in-production")

# ── Jinja2 filters ─────────────────────────────────────────────────────────────
@app.template_filter("basename")
def _basename_filter(s):
    return os.path.basename(s) if s else ""

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "data")
ARCHIVE_DIR = os.path.join(DATA_DIR, "Archive")
OUTPUT_DIR  = os.path.join(BASE_DIR, "output")
LOGS_DIR    = os.path.join(BASE_DIR, "logs")
RUNS_FILE   = os.path.join(BASE_DIR, "runs.json")

# Python executable — prefer the venv
_venv_py_win = os.path.join(BASE_DIR, "venv", "Scripts", "python.exe")
_venv_py_nix = os.path.join(BASE_DIR, "venv", "bin", "python3")
if os.path.exists(_venv_py_win):
    PYTHON = _venv_py_win
elif os.path.exists(_venv_py_nix):
    PYTHON = _venv_py_nix
else:
    PYTHON = sys.executable

for d in (DATA_DIR, ARCHIVE_DIR, OUTPUT_DIR, LOGS_DIR):
    os.makedirs(d, exist_ok=True)

# ── Auth ───────────────────────────────────────────────────────────────────────
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


# ── Context processor ─────────────────────────────────────────────────────────
@app.context_processor
def inject_globals():
    return {
        "env_loc":       os.getenv("LOCATION_ID", "4"),
        "env_org":       os.getenv("ORGANIZATION_ID", "1"),
        "active_run_id": _current_run_id,
        "now":           datetime.now(),
    }


# ── Migration type registry (14 scripts) ──────────────────────────────────────
#
# Each entry:
#   label        Display name
#   description  Short tagline shown on the migration card
#   icon         Bootstrap icon class (bi-*)
#   group        Category for grouping in the UI
#   script       Python script filename
#   files        List of file inputs; each has:
#                  key    — form field prefix
#                  label  — displayed label
#                  accept — file accept attribute
#                  arg    — CLI flag to pass to the script
#                  hint   — helper text shown below the input

MIGRATION_TYPES = {
    "units": {
        "label":       "Units",
        "description": "Storage unit inventory",
        "icon":        "bi-box-seam",
        "group":       "Setup",
        "disabled":    True,
        "script":      "migrate_units.py",
        "files": [
            {
                "key":    "file",
                "label":  "Units CSV",
                "accept": ".csv,.xlsx,.xls",
                "arg":    "--file",
                "hint":   "Units.csv exported from SiteLink",
            },
        ],
    },
    "customers": {
        "label":       "Active Customers",
        "description": "Current active tenants",
        "icon":        "bi-person-check",
        "group":       "Tenants",
        "script":      "migrate_customers.py",
        "files": [
            {
                "key":    "file",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
        ],
    },
    "former_customers": {
        "label":       "Former Customers",
        "description": "Historical / past tenants",
        "icon":        "bi-person-dash",
        "group":       "Tenants",
        "script":      "migrate_former_customers.py",
        "files": [
            {
                "key":    "file",
                "label":  "Former Tenants CSV",
                "accept": ".csv",
                "arg":    "--file",
                "hint":   "FormerTenants.csv from SiteLink",
            },
        ],
    },
    "rental_agreements": {
        "label":       "Rental Agreements",
        "description": "Active lease records",
        "icon":        "bi-file-earmark-text",
        "group":       "Agreements",
        "script":      "migrate_rental_agreements.py",
        "files": [
            {
                "key":    "file",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
        ],
    },
    "historical_rental_agreements": {
        "label":       "Historical Leases",
        "description": "Closed / past rental agreements",
        "icon":        "bi-file-earmark-x",
        "group":       "Agreements",
        "script":      "migrate_historical_rental_agreements.py",
        "files": [
            {
                "key":    "file",
                "label":  "Tenants+Units+Ledgers Excel",
                "accept": ".xlsx,.xls",
                "arg":    "--file",
                "hint":   "Tenants+Units+Ledgers.xlsx from SiteLink",
            },
        ],
    },
    "customer_unit": {
        "label":       "Customer–Unit Links",
        "description": "Active unit assignments",
        "icon":        "bi-link-45deg",
        "group":       "Agreements",
        "disabled":    True,
        "script":      "migrate_customer_unit.py",
        "files": [
            {
                "key":    "file",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
        ],
    },
    "historical_customer_unit": {
        "label":       "Historical Unit Links",
        "description": "Past customer–unit assignments",
        "icon":        "bi-link",
        "disabled":    True,
        "group":       "Agreements",
        "script":      "migrate_historical_customer_unit.py",
        "files": [
            {
                "key":    "file",
                "label":  "Tenants+Units+Ledgers Excel",
                "accept": ".xlsx,.xls",
                "arg":    "--file",
                "hint":   "Tenants+Units+Ledgers.xlsx from SiteLink",
            },
        ],
    },
    "billing_schedules": {
        "label":       "Billing Schedules",
        "description": "Recurring billing per unit",
        "icon":        "bi-calendar-check",
        "group":       "Financials",
        "script":      "migrate_billing_schedules.py",
        "files": [
            {
                "key":    "file_tenants",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file-tenants",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
            {
                "key":    "file_rentroll",
                "label":  "Rent Roll CSV",
                "accept": ".csv",
                "arg":    "--file-rentroll",
                "hint":   "Rent_Roll.csv from SiteLink",
            },
        ],
    },
    "payments": {
        "label":       "Payments",
        "description": "Full payment transaction history",
        "icon":        "bi-credit-card",
        "group":       "Financials",
        "script":      "migrate_payments.py",
        "files": [
            {
                "key":    "file_payments",
                "label":  "Payments CSV",
                "accept": ".csv",
                "arg":    "--file-payments",
                "hint":   "Payments.csv from SiteLink",
            },
            {
                "key":    "file_tenants",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file-tenants",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
            {
                "key":      "file_credits",
                "label":    "Credits CSV",
                "accept":   ".csv",
                "arg":      "--file-credits",
                "hint":     "Credits.csv — PaymentIDs here are excluded from payments import",
                "optional": True,
            },
        ],
    },
    "credits": {
        "label":       "Credits",
        "description": "Credit / negative ledger entries",
        "icon":        "bi-arrow-down-circle",
        "group":       "Financials",
        "script":      "migrate_credits.py",
        "files": [
            {
                "key":    "file_credits",
                "label":  "Credits CSV",
                "accept": ".csv",
                "arg":    "--file-credits",
                "hint":   "Credits.csv from SiteLink",
            },
            {
                "key":    "file_tenants",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file-tenants",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
        ],
    },
    "ledger_charges": {
        "label":       "Ledger Charges",
        "description": "All charge / fee entries",
        "icon":        "bi-receipt",
        "group":       "Financials",
        "script":      "migrate_ledger_charges.py",
        "files": [
            {
                "key":    "file_charges",
                "label":  "Charges CSV",
                "accept": ".csv",
                "arg":    "--file-charges",
                "hint":   "Charges.csv from SiteLink",
            },
            {
                "key":    "file_tenants",
                "label":  "Tenants CSV",
                "accept": ".csv",
                "arg":    "--file-tenants",
                "hint":   "Tenants_Units_Ledgers_Access.csv from SiteLink",
            },
            {
                "key":    "file_charge_types",
                "label":  "Charge Types CSV",
                "accept": ".csv",
                "arg":    "--file-charge-types",
                "hint":   "ChargeType.csv from SiteLink",
            },
        ],
    },
    "invoices": {
        "label":       "Invoices",
        "description": "Invoice records",
        "icon":        "bi-file-earmark-ruled",
        "group":       "Financials",
        "script":      "migrate_invoices.py",
        "files": [
            {
                "key":    "file_invoices",
                "label":  "Invoices CSV",
                "accept": ".csv",
                "arg":    "--file-invoices",
                "hint":   "Invoices.csv from SiteLink",
            },
            {
                "key":    "file_payments",
                "label":  "Payments CSV",
                "accept": ".csv",
                "arg":    "--file-payments",
                "hint":   "Payments.csv from SiteLink",
            },
        ],
    },
    "receipts": {
        "label":       "Receipts",
        "description": "Payment receipt records",
        "icon":        "bi-receipt-cutoff",
        "group":       "Financials",
        "script":      "migrate_receipts.py",
        "files": [
            {
                "key":    "file_receipts",
                "label":  "Receipts CSV",
                "accept": ".csv",
                "arg":    "--file-receipts",
                "hint":   "Receipts.csv from SiteLink",
            },
        ],
    },
    "gate_access_logs": {
        "label":       "Gate Access Logs",
        "description": "Entry / exit access events",
        "icon":        "bi-door-open",
        "group":       "Access",
        "script":      "migrate_gate_access_logs.py",
        "files": [
            {
                "key":    "file",
                "label":  "Activities CSV",
                "accept": ".csv,.xlsx,.xls",
                "arg":    "--file",
                "hint":   "Activities.csv from SiteLink",
            },
        ],
    },
    "recalculate_invoice_status": {
        "label":       "Recalculate Invoice Status",
        "description": "Recompute invoice status from payments",
        "icon":        "bi-arrow-repeat",
        "group":       "Utilities",
        "script":      "recalculate_invoice_status.py",
        "files":       [],
        "no_output_arg": True,
    },
}

# Group order for the UI
_GROUP_ORDER = ["Setup", "Tenants", "Agreements", "Financials", "Access", "Utilities"]


def migration_groups() -> dict:
    """Return OrderedDict of group_name → [(key, cfg), ...] for the dashboard."""
    groups = {g: [] for g in _GROUP_ORDER}
    for key, cfg in MIGRATION_TYPES.items():
        grp = cfg.get("group", "Other")
        groups.setdefault(grp, [])
        groups[grp].append((key, cfg))
    return {g: v for g, v in groups.items() if v}


def build_command(mtype: str, file_map: dict, dry_run: bool) -> list:
    """Build the CLI command list for a migration type."""
    cfg = MIGRATION_TYPES[mtype]
    cmd = [PYTHON, os.path.join(BASE_DIR, cfg["script"])]
    for fdef in cfg["files"]:
        path = file_map.get(fdef["key"])
        if path:
            cmd += [fdef["arg"], path]
    if not cfg.get("no_output_arg"):
        cmd += ["--output", "db"]
    if dry_run:
        cmd.append("--dry-run")
    return cmd


# ── Global run state ───────────────────────────────────────────────────────────
_run_queues:    dict  = {}
_migration_lock       = threading.Lock()
_current_run_id       = None


# ── Run history ────────────────────────────────────────────────────────────────
def load_runs() -> list:
    if not os.path.exists(RUNS_FILE):
        return []
    try:
        with open(RUNS_FILE, encoding="utf-8") as f:
            return json.load(f).get("runs", [])
    except Exception:
        return []


def save_run(run: dict):
    runs = load_runs()
    for i, r in enumerate(runs):
        if r["run_id"] == run["run_id"]:
            runs[i] = run
            break
    else:
        runs.insert(0, run)
    with open(RUNS_FILE, "w", encoding="utf-8") as f:
        json.dump({"runs": runs}, f, indent=2, default=str)


def get_run(run_id: str) -> dict | None:
    return next((r for r in load_runs() if r["run_id"] == run_id), None)


def _calc_duration(started_at: str, finished_at: str) -> str:
    try:
        secs = int((datetime.fromisoformat(finished_at) - datetime.fromisoformat(started_at)).total_seconds())
        return f"{secs}s" if secs < 60 else f"{secs // 60}m {secs % 60}s"
    except Exception:
        return ""


# ── File utilities ─────────────────────────────────────────────────────────────
def list_data_files() -> list:
    return sorted(
        name for name in os.listdir(DATA_DIR)
        if os.path.isfile(os.path.join(DATA_DIR, name))
    ) if os.path.exists(DATA_DIR) else []


def archive_old_files():
    cutoff = datetime.now() - timedelta(days=2)
    for name in os.listdir(DATA_DIR):
        full = os.path.join(DATA_DIR, name)
        try:
            if os.path.isfile(full) and datetime.fromtimestamp(os.stat(full).st_mtime) < cutoff:
                shutil.move(full, os.path.join(ARCHIVE_DIR, name))
        except (PermissionError, OSError):
            pass  # file in use or already moved — skip silently


# ── Background migration thread ────────────────────────────────────────────────
def _run_migration_thread(run_id: str, cmd: list, run_record: dict):
    global _current_run_id
    q = _run_queues[run_id]

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            bufsize=1,
            cwd=BASE_DIR,
            env=os.environ.copy(),
        )
        for line in iter(proc.stdout.readline, ""):
            if line.strip():
                q.put(("log", line.rstrip()))
        proc.wait()

        finished_at = datetime.now().isoformat()
        run_record["finished_at"] = finished_at
        run_record["status"]      = "success" if proc.returncode == 0 else "error"
        run_record["exit_code"]   = proc.returncode
        run_record["duration"]    = _calc_duration(run_record["started_at"], finished_at)

        # Attach log artefacts — scope by script name so we never pick up a
        # file from a different migration type that ran around the same time.
        script_base = os.path.splitext(
            MIGRATION_TYPES.get(run_record["migration_type"], {}).get("script", "")
        )[0]   # e.g. "migrate_payments"
        run_started = datetime.fromisoformat(run_record["started_at"]).timestamp()

        for pattern, key in [
            (f"migration_{script_base}_*.log", "log_file"),
            (f"errors_{script_base}_*.csv",    "error_file"),
            (f"skipped_{script_base}_*.csv",   "skipped_file"),
            (f"summary_{script_base}_*.txt",   "summary_file"),
        ]:
            # Pick the newest file created AFTER this run started
            candidates = [
                f for f in glob.glob(os.path.join(LOGS_DIR, pattern))
                if os.path.getmtime(f) >= run_started - 5   # 5s grace for clock skew
            ]
            if candidates:
                run_record[key] = os.path.basename(
                    max(candidates, key=os.path.getmtime)
                )

        if (run_record.get("migration_type") == "former_customers" and
                os.path.exists(os.path.join(OUTPUT_DIR, "active_ledgers_review.xlsx"))):
            run_record["output_file"] = "active_ledgers_review.xlsx"

        save_run(run_record)
        q.put(("status", run_record["status"]))

    except Exception as exc:
        run_record["status"]      = "error"
        run_record["finished_at"] = datetime.now().isoformat()
        save_run(run_record)
        q.put(("log", f"INTERNAL ERROR: {exc}"))
        q.put(("status", "error"))
    finally:
        _current_run_id = None
        _migration_lock.release()


# =============================================================================
# Routes — Auth
# =============================================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("logged_in"):
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        # Re-read env in case credentials changed
        load_dotenv(override=True)
        user = os.getenv("ADMIN_USER", "admin")
        pwd  = os.getenv("ADMIN_PASS", "admin")
        if request.form.get("username") == user and request.form.get("password") == pwd:
            session["logged_in"] = True
            session["username"]  = request.form["username"]
            return redirect(url_for("dashboard"))
        flash("Invalid username or password.", "danger")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# =============================================================================
# Routes — Dashboard
# =============================================================================

@app.route("/")
@login_required
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
@login_required
def dashboard():
    archive_old_files()
    recent_runs = load_runs()[:8]

    # Serialise migration types for JS (only fields needed by the client)
    mt_for_js = {
        k: {
            "label":  v["label"],
            "icon":   v["icon"],
            "files":  v["files"],
        }
        for k, v in MIGRATION_TYPES.items()
    }

    return render_template(
        "dashboard.html",
        migration_types      = MIGRATION_TYPES,
        migration_groups     = migration_groups(),
        migration_types_json = json.dumps(mt_for_js),
        existing_files_json  = json.dumps(list_data_files()),
        recent_runs          = recent_runs,
        busy                 = _current_run_id is not None,
        selected_type        = request.args.get("type"),
    )


# =============================================================================
# Routes — Start run
# =============================================================================

@app.route("/run", methods=["POST"])
@login_required
def run_migration():
    global _current_run_id

    if not _migration_lock.acquire(blocking=False):
        flash("A migration is already running. Please wait for it to complete.", "warning")
        return redirect(url_for("dashboard"))

    mtype   = request.form.get("migration_type")
    dry_run = request.form.get("dry_run") == "1"

    if mtype not in MIGRATION_TYPES:
        _migration_lock.release()
        flash("Invalid migration type selected.", "danger")
        return redirect(url_for("dashboard"))

    cfg      = MIGRATION_TYPES[mtype]
    file_map = {}

    for fdef in cfg["files"]:
        key      = fdef["key"]
        uploaded = request.files.get(f"{key}_upload")
        existing = request.form.get(f"{key}_existing", "").strip()

        if uploaded and uploaded.filename:
            safe = uploaded.filename.replace(" ", "_")
            dest = os.path.join(DATA_DIR, safe)
            try:
                uploaded.save(dest)
            except PermissionError:
                _migration_lock.release()
                flash(
                    f"Cannot save '{safe}' — the file is open in another program "
                    f"(e.g. Excel). Close it and try again.",
                    "danger",
                )
                return redirect(url_for("dashboard"))
            file_map[key] = dest
        elif existing:
            file_map[key] = os.path.join(DATA_DIR, existing)
        elif fdef.get("optional"):
            pass  # optional file — skip silently
        else:
            _migration_lock.release()
            flash(f"Please provide a file for: {fdef['label']}", "danger")
            return redirect(url_for("dashboard"))

    run_id              = str(uuid.uuid4())[:8]
    _run_queues[run_id] = queue.Queue()
    _current_run_id     = run_id
    cmd                 = build_command(mtype, file_map, dry_run)

    run_record = {
        "run_id":          run_id,
        "migration_type":  mtype,
        "label":           cfg["label"],
        "files":           [os.path.basename(v) for v in file_map.values()],
        "dry_run":         dry_run,
        "started_at":      datetime.now().isoformat(),
        "finished_at":     None,
        "status":          "running",
        "exit_code":       None,
        "duration":        None,
        "log_file":        None,
        "skipped_file":    None,
        "error_file":      None,
        "summary_file":    None,
        "output_file":     None,
    }
    save_run(run_record)

    threading.Thread(
        target=_run_migration_thread,
        args=(run_id, cmd, run_record),
        daemon=True,
    ).start()

    return redirect(url_for("view_run", run_id=run_id))


# =============================================================================
# Routes — Progress / SSE
# =============================================================================

@app.route("/run/<run_id>")
@login_required
def view_run(run_id):
    run = get_run(run_id)
    if not run:
        flash("Run not found.", "danger")
        return redirect(url_for("history"))
    return render_template("run.html", run=run, logs_dir=LOGS_DIR, output_dir=OUTPUT_DIR)


@app.route("/stream/<run_id>")
@login_required
def stream(run_id):
    q = _run_queues.get(run_id)

    def generate():
        if q is None:
            run    = get_run(run_id)
            status = run["status"] if run else "unknown"
            yield f"event: status\ndata: {status}\n\n"
            return
        while True:
            try:
                event_type, payload = q.get(timeout=25)
                if event_type == "log":
                    yield f"event: log\ndata: {payload}\n\n"
                elif event_type == "status":
                    yield f"event: status\ndata: {payload}\n\n"
                    break
            except queue.Empty:
                yield "event: heartbeat\ndata: ping\n\n"

    return Response(
        stream_with_context(generate()),
        content_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# =============================================================================
# Routes — History
# =============================================================================

@app.route("/history")
@login_required
def history():
    return render_template("history.html", runs=load_runs())


# =============================================================================
# Routes — Logs / Skipped viewer
# =============================================================================

@app.route("/logs")
@login_required
def logs_list():
    def _meta(path):
        s = os.stat(path)
        return {
            "name":    os.path.basename(path),
            "size_kb": f"{s.st_size / 1024:.1f}",
            "mtime":   datetime.fromtimestamp(s.st_mtime).strftime("%Y-%m-%d %H:%M"),
        }

    skipped_paths = sorted(glob.glob(os.path.join(LOGS_DIR, "skipped_*.csv")),   reverse=True)[:30]
    error_paths   = sorted(glob.glob(os.path.join(LOGS_DIR, "errors_*.csv")),    reverse=True)[:30]
    log_paths     = sorted(glob.glob(os.path.join(LOGS_DIR, "migration_*.log")), reverse=True)[:20]
    output_paths  = sorted(glob.glob(os.path.join(OUTPUT_DIR, "*.xlsx")),        reverse=True)[:20]

    return render_template(
        "logs_viewer.html",
        skipped_files = [_meta(f) for f in skipped_paths],
        error_files   = [_meta(f) for f in error_paths],
        log_files     = [_meta(f) for f in log_paths],
        output_files  = [_meta(f) for f in output_paths],
    )


@app.route("/logs/view/<filename>")
@login_required
def view_log(filename):
    path = os.path.join(LOGS_DIR, os.path.basename(filename))
    if not os.path.exists(path):
        flash("Log file not found.", "danger")
        return redirect(url_for("logs_list"))
    with open(path, encoding="utf-8", errors="replace") as f:
        lines = [line.rstrip() for line in f.readlines()]
    return render_template("log_content.html", filename=os.path.basename(filename), content=lines)


# =============================================================================
# Routes — Downloads
# =============================================================================

@app.route("/download/log/<filename>")
@login_required
def download_log(filename):
    path = os.path.join(LOGS_DIR, os.path.basename(filename))
    if not os.path.exists(path):
        flash("File not found.", "danger")
        return redirect(url_for("logs_list"))
    return send_file(path, as_attachment=True)


@app.route("/download/output/<filename>")
@login_required
def download_output(filename):
    path = os.path.join(OUTPUT_DIR, os.path.basename(filename))
    if not os.path.exists(path):
        flash("File not found.", "danger")
        return redirect(url_for("logs_list"))
    return send_file(path, as_attachment=True)


# =============================================================================
# Routes — Configuration
# =============================================================================

@app.route("/config", methods=["GET", "POST"])
@login_required
def config():
    env_path = os.path.join(BASE_DIR, ".env")

    if request.method == "POST":
        # Fields managed by the UI (passwords are only written if non-empty)
        _FIELDS = [
            "DB_HOST", "DB_PORT", "DB_NAME", "DB_USER",
            "ORGANIZATION_ID", "LOCATION_ID", "CREATED_BY",
            "ADMIN_USER",
        ]
        _PWD_FIELDS = ["DB_PASSWORD", "ADMIN_PASS"]

        updates = {f: request.form.get(f, "").strip() for f in _FIELDS if request.form.get(f, "").strip()}
        for f in _PWD_FIELDS:
            v = request.form.get(f, "").strip()
            if v:
                updates[f] = v

        # Read existing .env lines
        lines: list[str] = []
        if os.path.exists(env_path):
            with open(env_path, encoding="utf-8") as fh:
                lines = [l.rstrip() for l in fh.readlines()]

        # Overwrite matching keys in place; append new ones
        written: set = set()
        new_lines: list[str] = []
        for line in lines:
            if "=" in line and not line.startswith("#"):
                k = line.partition("=")[0].strip()
                if k in updates:
                    new_lines.append(f"{k}={updates[k]}")
                    written.add(k)
                else:
                    new_lines.append(line)
            else:
                new_lines.append(line)
        for k, v in updates.items():
            if k not in written:
                new_lines.append(f"{k}={v}")

        with open(env_path, "w", encoding="utf-8") as fh:
            fh.write("\n".join(new_lines) + "\n")

        load_dotenv(override=True)
        flash("Configuration saved successfully.", "success")
        return redirect(url_for("config"))

    env_vals = {
        "DB_HOST":          os.getenv("DB_HOST", ""),
        "DB_PORT":          os.getenv("DB_PORT", "5432"),
        "DB_NAME":          os.getenv("DB_NAME", ""),
        "DB_USER":          os.getenv("DB_USER", ""),
        "DB_PASSWORD":      "",   # never echo password into form
        "ORGANIZATION_ID":  os.getenv("ORGANIZATION_ID", "1"),
        "LOCATION_ID":      os.getenv("LOCATION_ID", "4"),
        "CREATED_BY":       os.getenv("CREATED_BY", "9"),
        "ADMIN_USER":       os.getenv("ADMIN_USER", "admin"),
        "ADMIN_PASS":       "",   # never echo password
    }
    return render_template("config.html", env=env_vals)


# =============================================================================
# Routes — XLSX → CSV Converter
# =============================================================================

@app.route("/convert")
@login_required
def convert_xlsx():
    return render_template("convert.html", default_folder=DATA_DIR)


@app.route("/api/convert/scan", methods=["POST"])
@login_required
def api_convert_scan():
    """Return a list of .xlsx files in the given folder."""
    folder = request.json.get("folder", "").strip()
    if not folder or not os.path.isdir(folder):
        return jsonify({"ok": False, "error": f"Folder not found: {folder!r}"})
    files = [
        {
            "name": f,
            "size_kb": f"{os.path.getsize(os.path.join(folder, f)) / 1024:.1f}",
        }
        for f in sorted(os.listdir(folder))
        if f.lower().endswith((".xlsx", ".xls")) and os.path.isfile(os.path.join(folder, f))
    ]
    return jsonify({"ok": True, "folder": folder, "files": files})


@app.route("/api/convert/run", methods=["POST"])
@login_required
def api_convert_run():
    """Convert all xlsx/xls in a folder to CSV and delete the originals."""
    import pandas as pd

    folder    = request.json.get("folder", "").strip()
    confirmed = request.json.get("confirmed", False)

    if not confirmed:
        return jsonify({"ok": False, "error": "Not confirmed."})
    if not folder or not os.path.isdir(folder):
        return jsonify({"ok": False, "error": f"Folder not found: {folder!r}"})

    results = []
    for fname in sorted(os.listdir(folder)):
        if not fname.lower().endswith((".xlsx", ".xls")):
            continue
        xlsx_path = os.path.join(folder, fname)
        try:
            xl        = pd.ExcelFile(xlsx_path)
            base      = os.path.splitext(xlsx_path)[0]
            csv_names = []

            if len(xl.sheet_names) == 1:
                csv_path = base + ".csv"
                xl.parse(xl.sheet_names[0]).to_csv(csv_path, index=False, encoding="utf-8")
                csv_names.append(os.path.basename(csv_path))
            else:
                for sheet in xl.sheet_names:
                    safe     = sheet.replace("/", "_").replace("\\", "_")
                    csv_path = f"{base}_{safe}.csv"
                    xl.parse(sheet).to_csv(csv_path, index=False, encoding="utf-8")
                    csv_names.append(os.path.basename(csv_path))

            xl.close()
            os.remove(xlsx_path)
            results.append({"file": fname, "ok": True, "csvs": csv_names})
        except Exception as exc:
            results.append({"file": fname, "ok": False, "error": str(exc)})

    ok_count  = sum(1 for r in results if r["ok"])
    err_count = sum(1 for r in results if not r["ok"])
    return jsonify({"ok": True, "results": results, "ok_count": ok_count, "err_count": err_count})


# =============================================================================
# API Endpoints
# =============================================================================

@app.route("/api/status")
@login_required
def api_status():
    run_id = request.args.get("run_id")
    if run_id:
        run = get_run(run_id)
        if not run:
            return jsonify({"error": "not found"}), 404
        summary_text = None
        sf = run.get("summary_file")
        if sf:
            sf_path = os.path.join(LOGS_DIR, sf)
            if os.path.exists(sf_path):
                with open(sf_path, encoding="utf-8", errors="replace") as fh:
                    summary_text = fh.read()
        return jsonify({
            "run_id":        run["run_id"],
            "status":        run["status"],
            "label":         run["label"],
            "dry_run":       run["dry_run"],
            "log_file":      run.get("log_file"),
            "skipped_file":  run.get("skipped_file"),
            "error_file":    run.get("error_file"),
            "summary_file":  run.get("summary_file"),
            "output_file":   run.get("output_file"),
            "summary_text":  summary_text,
        })
    return jsonify({
        "busy":           _current_run_id is not None,
        "current_run_id": _current_run_id,
    })


@app.route("/api/stats")
@login_required
def api_stats():
    runs     = load_runs()
    total    = len(runs)
    success  = sum(1 for r in runs if r["status"] == "success")
    errors   = sum(1 for r in runs if r["status"] == "error")
    last_run = runs[0] if runs else None
    return jsonify({
        "total":    total,
        "success":  success,
        "errors":   errors,
        "last_run": last_run,
    })


@app.route("/api/run-log/<run_id>")
@login_required
def api_run_log(run_id):
    """Return the full log lines for a completed run so the terminal can be repopulated."""
    run = get_run(run_id)
    if not run or not run.get("log_file"):
        return jsonify({"lines": [], "path": None})
    log_path = os.path.join(LOGS_DIR, run["log_file"])
    if not os.path.exists(log_path):
        return jsonify({"lines": [], "path": None})
    with open(log_path, encoding="utf-8", errors="replace") as fh:
        lines = [l.rstrip() for l in fh.readlines() if l.strip()]
    return jsonify({"lines": lines, "path": log_path})


@app.route("/api/db-ping")
@login_required
def api_db_ping():
    """Lightweight DB connectivity check used by the sidebar and config page."""
    try:
        import psycopg2
        host = os.getenv("DB_HOST", "")
        port = os.getenv("DB_PORT", "5432")
        name = os.getenv("DB_NAME", "")
        user = os.getenv("DB_USER", "")
        pwd  = os.getenv("DB_PASSWORD", "")
        if not (host and name and user):
            return jsonify({"ok": False, "error": "DB credentials not configured"})
        conn = psycopg2.connect(
            host=host, port=int(port), dbname=name,
            user=user, password=pwd, connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT version()")
        ver = cur.fetchone()[0].split(",")[0]
        cur.close()
        conn.close()
        return jsonify({"ok": True, "host": host, "database": name, "version": ver})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
