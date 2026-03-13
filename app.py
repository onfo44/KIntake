"""
Contract Manager — Streamlit POC
Corporate contract intake, tracking, and executed contract management.
"""

import streamlit as st
import sqlite3
import json
import io
from datetime import date, timedelta
import pandas as pd

# ── Page config (must be first Streamlit call) ────────────────────────────────
st.set_page_config(
    page_title="Contract Manager",
    page_icon="📄",
    layout="wide",
)

# ── Constants ─────────────────────────────────────────────────────────────────
PASSWORD       = st.secrets["APP_PASSWORD"]
LAWYERS        = ["Lawyer A", "Lawyer B", "Lawyer C", "Lawyer D", "Lawyer E"]
DB_PATH        = "hs_contracts.db"
MODEL          = "claude-sonnet-4-20250514"
STATUS_OPTIONS = [
    "Pending Review", "Under Review", "Negotiation",
    "Executed", "Rejected", "On Hold",
]

# ── Database helpers ──────────────────────────────────────────────────────────

def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def db_df(sql, params=()):
    """Return a DataFrame from a SELECT query."""
    conn = db_conn()
    try:
        return pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()


def db_write(sql, params=()):
    """Execute a single write statement; return lastrowid."""
    conn = db_conn()
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def db_write_many(ops):
    """Execute multiple (sql, params) write tuples in one transaction."""
    conn = db_conn()
    try:
        for sql, params in ops:
            conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def init_db():
    conn = db_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
            INSERT OR IGNORE INTO settings VALUES ('lawyer_index', '0');

            CREATE TABLE IF NOT EXISTS intake_log (
                intake_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_originator    TEXT,
                date_submitted         TEXT,
                department             TEXT,
                region                 TEXT,
                party_a                TEXT,
                party_b                TEXT,
                description_of_service TEXT,
                contract_value         TEXT,
                w9_received            TEXT,
                assigned_lawyer        TEXT,
                summary                TEXT,
                key_dates              TEXT,
                status                 TEXT DEFAULT 'Pending Review',
                notes                  TEXT,
                filename               TEXT
            );

            CREATE TABLE IF NOT EXISTS executed_contracts (
                id                           INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_originator          TEXT,
                date_submitted               TEXT,
                date_executed                TEXT,
                party_a                      TEXT,
                party_b                      TEXT,
                description_of_service       TEXT,
                region                       TEXT,
                department                   TEXT,
                employee_owners              TEXT,
                start_date                   TEXT,
                end_date                     TEXT,
                termination_requirements     TEXT,
                auto_renewal                 TEXT,
                non_solicit                  TEXT,
                non_solicit_termination_date TEXT,
                notes                        TEXT,
                contract_link                TEXT
            );
        """)
        conn.commit()
    finally:
        conn.close()


def get_next_lawyer():
    conn = db_conn()
    try:
        row = conn.execute(
            "SELECT value FROM settings WHERE key='lawyer_index'"
        ).fetchone()
        idx    = int(row["value"])
        lawyer = LAWYERS[idx]
        conn.execute(
            "UPDATE settings SET value=? WHERE key='lawyer_index'",
            (str((idx + 1) % len(LAWYERS)),),
        )
        conn.commit()
        return lawyer
    finally:
        conn.close()


# ── File parsing ──────────────────────────────────────────────────────────────

# Minimum characters from pdfplumber before we assume the PDF is scanned
_MIN_TEXT_CHARS = 100


def _ocr_pdf(raw_bytes):
    """Convert a scanned PDF to text via pdf2image + pytesseract."""
    try:
        from pdf2image import convert_from_bytes
        import pytesseract
    except ImportError:
        raise ValueError(
            "OCR dependencies missing. Ensure pytesseract and pdf2image are in "
            "requirements.txt, and tesseract-ocr + poppler-utils are in packages.txt."
        )
    try:
        images = convert_from_bytes(raw_bytes, dpi=200)
    except Exception as exc:
        raise ValueError(f"Could not convert PDF pages to images: {exc}") from exc
    try:
        pages = [pytesseract.image_to_string(img) for img in images]
    except Exception as exc:
        raise ValueError(f"OCR failed: {exc}") from exc
    text = "\n".join(pages).strip()
    if not text:
        raise ValueError("OCR produced no text. The PDF may be blank or unreadable.")
    return text


def extract_text(uploaded_file):
    """Return (text, filename) from a PDF or DOCX upload.

    For PDFs: tries pdfplumber first (fast, for text-based PDFs).
    If that yields less than _MIN_TEXT_CHARS, assumes scanned and falls back to OCR.
    """
    filename = uploaded_file.name
    ext      = filename.rsplit(".", 1)[-1].lower()
    raw      = uploaded_file.read()

    if ext == "pdf":
        import pdfplumber
        try:
            with pdfplumber.open(io.BytesIO(raw)) as pdf:
                text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        except Exception as exc:
            raise ValueError(f"Cannot parse PDF: {exc}") from exc

        if len(text.strip()) < _MIN_TEXT_CHARS:
            # Scanned PDF — fall back to OCR
            text = _ocr_pdf(raw)

        return text, filename

    if ext == "docx":
        from docx import Document
        try:
            doc  = Document(io.BytesIO(raw))
            text = "\n".join(para.text for para in doc.paragraphs)
            return text, filename
        except Exception as exc:
            raise ValueError(f"Cannot parse DOCX: {exc}") from exc

    raise ValueError("Unsupported file type. Upload a PDF or DOCX.")


# ── Claude extraction ─────────────────────────────────────────────────────────

_PROMPT_TEMPLATE = """\
You are a contract analysis assistant. Extract the fields below from the \
contract text and return them as a single, valid JSON object — no markdown, \
no code fences, no extra text outside the JSON.

Fields:
- summary                      : 2–3 sentence plain-language summary of the contract purpose
- party_a                      : first contracting party name
- party_b                      : second / counter-party name
- key_dates                    : list of {{"label": "...", "date": "..."}} objects
- contract_value               : total value (dollar amount or descriptive text)
- start_date                   : contract start date (string)
- end_date                     : contract end date or term (string)
- termination_requirements     : how the contract may be terminated
- auto_renewal                 : "Yes" or "No"
- non_solicit                  : "Yes" or "No"
- non_solicit_termination_date : date string if applicable, else null
- description_of_service       : ≤10-word sentence describing the service
- region_inferred              : inferred geographic scope

CONTRACT TEXT:
{text}
"""


def parse_date_str(s, fallback=None):
    """Best-effort parse of a Claude date string to datetime.date."""
    if not s or not isinstance(s, str):
        return fallback or date.today()
    from datetime import datetime as _dt
    for fmt in (
        "%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y",
        "%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y/%m/%d",
    ):
        try:
            return _dt.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return fallback or date.today()


def map_region(s):
    """Map a Claude-inferred region string to a valid selectbox option."""
    if not s:
        return "Other"
    sl = s.lower()
    if any(x in sl for x in ["us", "united states", "america", "u.s."]):
        return "US"
    if any(x in sl for x in ["europe", "eu ", "european"]):
        return "Europe"
    if any(x in sl for x in ["asia", "pacific", "apac"]):
        return "Asia-Pacific"
    if any(x in sl for x in ["global", "worldwide", "international"]):
        return "Global"
    return "Other"


def extract_terms(text):
    """Call Claude API and return a dict of extracted contract terms."""
    try:
        import anthropic
    except ImportError:
        raise ValueError("The 'anthropic' package is not installed.")

    try:
        api_key = st.secrets["ANTHROPIC_API_KEY"]
    except (KeyError, FileNotFoundError):
        raise ValueError(
            "ANTHROPIC_API_KEY not found in st.secrets. "
            "Add it via .streamlit/secrets.toml or Streamlit Cloud secrets."
        )

    try:
        client   = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=MODEL,
            max_tokens=2048,
            messages=[
                {
                    "role":    "user",
                    "content": _PROMPT_TEMPLATE.format(text=text[:15000]),
                }
            ],
        )
        raw = response.content[0].text.strip()
        # Use regex to extract the first {...} block, regardless of
        # surrounding text, code fences, or preamble Claude may add.
        import re
        match = re.search(r"\{.*\}", raw, re.DOTALL)
        if not match:
            preview = raw[:200] if raw else "(empty response)"
            raise ValueError(
                f"No JSON object found in Claude's response. Preview: {preview}"
            )
        return json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Claude returned invalid JSON: {exc}") from exc
    except Exception as exc:
        raise ValueError(f"Claude API error: {exc}") from exc


# ── Authentication ────────────────────────────────────────────────────────────

def require_auth():
    """Return True if authenticated; otherwise render login gate and return False."""
    if st.session_state.get("authenticated"):
        return True

    st.title("🔐 Contract Manager")
    st.caption("Enter the access password to continue.")

    with st.form("login"):
        pwd = st.text_input("Password", type="password")
        if st.form_submit_button("Login", type="primary"):
            if pwd == PASSWORD:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password. Please try again.")

    return False


# ── Page 1 · Contract Intake ──────────────────────────────────────────────────

def page_intake():
    st.header("📄 Contract Intake")
    st.caption("Submit a new contract for AI-powered term extraction and lawyer assignment.")

    # ── Intake form ──────────────────────────────────────────────────────────
    with st.form("intake_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        with c1:
            originator = st.text_input("Contract Originator *")
            department = st.text_input("Responsible Department *")
        with c2:
            region = st.selectbox("Region", ["US", "Europe", "Asia-Pacific", "Global", "Other"])
            w9     = st.radio("Has a W-9 been received?", ["Yes", "No", "N/A"], horizontal=True)

        notes    = st.text_area("Additional context or notes (optional)")
        uploaded = st.file_uploader("Upload Contract (PDF or DOCX) *", type=["pdf", "docx"])
        submit   = st.form_submit_button("Submit Contract ▶", type="primary")

    # ── If form not submitted, show last result if any ────────────────────────
    if not submit:
        if "intake_result" in st.session_state:
            _render_intake_result(st.session_state["intake_result"])
        return

    # ── Validate ──────────────────────────────────────────────────────────────
    errors = []
    if not originator.strip():
        errors.append("Contract Originator is required.")
    if not department.strip():
        errors.append("Responsible Department is required.")
    if uploaded is None:
        errors.append("Please upload a contract file (PDF or DOCX).")
    for err in errors:
        st.error(err)
    if errors:
        return

    # ── Parse document ────────────────────────────────────────────────────────
    with st.spinner("Parsing document…"):
        try:
            text, filename = extract_text(uploaded)
        except ValueError as exc:
            st.error(str(exc))
            return

    # ── Extract terms via Claude ──────────────────────────────────────────────
    with st.spinner("Analyzing contract with Claude…"):
        try:
            terms = extract_terms(text)
        except ValueError as exc:
            st.error(str(exc))
            return

    # ── Assign lawyer (round-robin) ───────────────────────────────────────────
    lawyer    = get_next_lawyer()
    key_dates = terms.get("key_dates", [])
    if not isinstance(key_dates, list):
        key_dates = []

    # ── Save to database ──────────────────────────────────────────────────────
    intake_id = db_write(
        """
        INSERT INTO intake_log (
            contract_originator, date_submitted, department, region,
            party_a, party_b, description_of_service, contract_value,
            w9_received, assigned_lawyer, summary, key_dates,
            status, notes, filename
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            originator, str(date.today()), department, region,
            terms.get("party_a", ""),
            terms.get("party_b", ""),
            terms.get("description_of_service", ""),
            terms.get("contract_value", ""),
            w9, lawyer,
            terms.get("summary", ""),
            json.dumps(key_dates),
            "Pending Review", notes, filename,
        ),
    )

    # Store result for display after rerun
    st.session_state["intake_result"] = {
        "intake_id":  intake_id,
        "terms":      terms,
        "lawyer":     lawyer,
        "key_dates":  key_dates,
        "originator": originator,
    }
    st.rerun()


def _render_intake_result(result):
    terms     = result["terms"]
    lawyer    = result["lawyer"]
    key_dates = result.get("key_dates", [])

    st.success(f"✅ Contract #{result['intake_id']} submitted and saved to intake log.")

    st.subheader("📋 Extracted Terms")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f"**Summary:** {terms.get('summary', '—')}")
        st.markdown(f"**Party A:** {terms.get('party_a', '—')}")
        st.markdown(f"**Party B:** {terms.get('party_b', '—')}")
        st.markdown(f"**Contract Value:** {terms.get('contract_value', '—')}")
        st.markdown(f"**Start Date:** {terms.get('start_date', '—')}")
        st.markdown(f"**End Date:** {terms.get('end_date', '—')}")
        st.markdown(f"**Description of Service:** {terms.get('description_of_service', '—')}")
    with c2:
        st.markdown(f"**Auto Renewal:** {terms.get('auto_renewal', '—')}")
        st.markdown(f"**Non-Solicit:** {terms.get('non_solicit', '—')}")
        st.markdown(
            f"**Non-Solicit End Date:** {terms.get('non_solicit_termination_date', '—')}"
        )
        st.markdown(
            f"**Termination Requirements:** {terms.get('termination_requirements', '—')}"
        )
        st.markdown(f"**Region (inferred):** {terms.get('region_inferred', '—')}")

    if key_dates:
        st.markdown("**Key Dates:**")
        for kd in key_dates:
            st.markdown(f"  • **{kd.get('label', '')}:** {kd.get('date', '')}")

    st.divider()
    st.markdown(f"### 👤 Assigned Lawyer: **{lawyer}**")

    if st.button(f"📧 Send Intake Email to {lawyer}", key="email_btn"):
        st.info(
            "Email functionality is configured for Outlook integration "
            "and is disabled in this POC environment."
        )

    if st.button("↩ Submit Another Contract", key="reset_intake"):
        del st.session_state["intake_result"]
        st.rerun()


# ── Page 2 · Intake Log & Tracker ────────────────────────────────────────────

def page_tracker():
    st.header("📊 Intake Log & Tracker")

    df = db_df("SELECT * FROM intake_log ORDER BY intake_id DESC")

    if df.empty:
        st.info("No intake submissions yet.")
        return

    df["date_submitted"] = pd.to_datetime(df["date_submitted"], errors="coerce")

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### Intake Filters")

        lawyer_opts = ["All"] + sorted(df["assigned_lawyer"].dropna().unique().tolist())
        f_lawyer    = st.selectbox("Assigned Lawyer", lawyer_opts)

        f_status = st.selectbox("Status", ["All"] + STATUS_OPTIONS)

        region_opts = ["All"] + sorted(df["region"].dropna().unique().tolist())
        f_region    = st.selectbox("Region", region_opts)

        valid_dates = df["date_submitted"].dropna()
        min_d = valid_dates.min().date() if not valid_dates.empty else date.today()
        max_d = valid_dates.max().date() if not valid_dates.empty else date.today()
        f_dates = st.date_input(
            "Date Submitted Range", value=(min_d, max_d), key="tracker_dates"
        )

    # ── Apply filters ─────────────────────────────────────────────────────────
    mask = pd.Series(True, index=df.index)
    if f_lawyer != "All":
        mask &= df["assigned_lawyer"] == f_lawyer
    if f_status != "All":
        mask &= df["status"] == f_status
    if f_region != "All":
        mask &= df["region"] == f_region
    if isinstance(f_dates, (list, tuple)) and len(f_dates) == 2:
        mask &= df["date_submitted"].dt.date.between(f_dates[0], f_dates[1])

    filtered = df[mask].copy()

    # ── Format key_dates for readability ─────────────────────────────────────
    def fmt_kd(s):
        try:
            items = json.loads(s) if s else []
            return "; ".join(f"{d.get('label','')}: {d.get('date','')}" for d in items)
        except Exception:
            return s or ""

    filtered["key_dates"] = filtered["key_dates"].apply(fmt_kd)

    display_cols = [
        "intake_id", "contract_originator", "date_submitted", "department",
        "region", "party_a", "party_b", "description_of_service",
        "contract_value", "w9_received", "assigned_lawyer", "summary",
        "key_dates", "status", "notes", "filename",
    ]
    st.dataframe(
        filtered[display_cols],
        use_container_width=True,
        hide_index=True,
    )
    st.caption(f"{len(filtered)} record(s) shown.")

    # ── Status updater ────────────────────────────────────────────────────────
    st.subheader("Update Contract Status")
    ids = filtered["intake_id"].tolist()
    if not ids:
        st.info("No records match the current filters.")
        return

    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        sel_id = st.selectbox("Intake ID", ids)
    with col2:
        cur_status = filtered.loc[
            filtered["intake_id"] == sel_id, "status"
        ].values[0]
        idx_default = (
            STATUS_OPTIONS.index(cur_status) if cur_status in STATUS_OPTIONS else 0
        )
        new_status = st.selectbox("New Status", STATUS_OPTIONS, index=idx_default)
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Update ▶", type="primary"):
            db_write(
                "UPDATE intake_log SET status=? WHERE intake_id=?",
                (new_status, int(sel_id)),
            )
            st.success(f"Intake #{sel_id} → **{new_status}**")
            st.rerun()


# ── Page 3 · Executed Contracts Dashboard ────────────────────────────────────

def page_executed():
    st.header("✅ Executed Contracts Dashboard")

    # ── Helpers (local, used in form) ─────────────────────────────────────────
    def yn(val, default="N"):
        return "Y" if str(val).strip().lower() == "yes" else default

    def ns_map(val):
        v = str(val).strip().lower()
        if v == "yes": return "Y"
        if v == "no":  return "N"
        return "N/A"

    REGION_OPTS = ["US", "Europe", "Asia-Pacific", "Global", "Other"]
    AR_OPTS     = ["N", "Y"]
    NS_OPTS     = ["N/A", "Y", "N"]

    # ── Step 1: Upload & extract (always visible) ─────────────────────────────
    with st.container(border=True):
        st.markdown("#### 📤 Log Executed Contract")
        up_col, ext_col, man_col = st.columns([5, 1, 1])
        with up_col:
            ec_file = st.file_uploader(
                "Upload executed contract (PDF or DOCX)",
                type=["pdf", "docx"],
                key="ec_upload",
            )
        with ext_col:
            st.markdown("<br>", unsafe_allow_html=True)
            extract_clicked = st.button(
                "Extract ▶",
                disabled=(ec_file is None),
                key="ec_extract_btn",
                type="primary",
                help="Upload a file first, then click to auto-fill fields with Claude",
            )
        with man_col:
            st.markdown("<br>", unsafe_allow_html=True)
            if st.button("Manual Entry", key="ec_manual_btn", type="secondary"):
                st.session_state["ec_show_form"] = True
                st.session_state.pop("ec_prefill", None)
                st.rerun()

    if extract_clicked and ec_file is not None:
        with st.spinner("Parsing and analyzing with Claude…"):
            try:
                text, fname = extract_text(ec_file)
                terms = extract_terms(text)
                terms["_filename"] = fname
                st.session_state["ec_prefill"]  = terms
                st.session_state["ec_show_form"] = True
                st.rerun()
            except ValueError as exc:
                st.error(str(exc))

    # ── Step 2: Review / entry form (shown after extraction or manual click) ──
    pf        = st.session_state.get("ec_prefill", {})
    show_form = st.session_state.get("ec_show_form", False)

    if show_form:
        if pf:
            st.success(
                f"Terms extracted from **{pf.get('_filename', 'file')}** — "
                "review and adjust below, then save."
            )

        pf_region = map_region(pf.get("region_inferred", ""))
        pf_ar     = yn(pf.get("auto_renewal", "N"))
        pf_ns     = ns_map(pf.get("non_solicit", "N/A"))
        pf_start  = parse_date_str(pf.get("start_date"),  date.today())
        pf_end    = parse_date_str(pf.get("end_date"),    date.today() + timedelta(days=365))
        pf_nsdate = parse_date_str(
            pf.get("non_solicit_termination_date"), date.today() + timedelta(days=365)
        )

        with st.form("exec_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                f_orig   = st.text_input("Contract Originator",         value=pf.get("party_a", ""))
                f_dsub   = st.date_input("Date Submitted",               value=date.today())
                f_dexec  = st.date_input("Date Executed",                value=date.today())
                f_pa     = st.text_input("Party A",                      value=pf.get("party_a", ""))
                f_pb     = st.text_input("Party B",                      value=pf.get("party_b", ""))
                f_desc   = st.text_input("Description of Service (≤10 words)",
                                         value=pf.get("description_of_service", ""))
                f_region = st.selectbox("Relevant Region", REGION_OPTS,
                                        index=REGION_OPTS.index(pf_region))
                f_dept   = st.text_input("Responsible Department")
                f_emps   = st.text_input("Individual Employee Owners (comma-separated)")
            with c2:
                f_start  = st.date_input("Start Date", value=pf_start)
                f_end    = st.date_input("End Date",   value=pf_end)
                f_term   = st.text_area("Termination Requirements",
                                        value=pf.get("termination_requirements", ""))
                f_ar     = st.selectbox("Auto Renewal", AR_OPTS,
                                        index=AR_OPTS.index(pf_ar))
                f_ns     = st.selectbox("Non-Solicit", NS_OPTS,
                                        index=NS_OPTS.index(pf_ns))
                # NOTE: disabled= is evaluated at render time inside st.form.
                # To enable this field, select "Y" for Non-Solicit, save/cancel,
                # then re-enter the form — this is a Streamlit form limitation.
                f_nsdate = st.date_input("Non-Solicit Termination Date",
                                         value=pf_nsdate, disabled=(f_ns != "Y"))
                f_notes  = st.text_area("Additional Notes/Terms",
                                        value=pf.get("summary", ""))
                f_link   = st.text_input("Link to Contract (URL or file path)",
                                         value=pf.get("_filename", ""))

            save_col, cancel_col, _ = st.columns([1, 1, 5])
            with save_col:
                saved = st.form_submit_button("💾 Save Contract", type="primary")
            with cancel_col:
                cancelled = st.form_submit_button("✕ Cancel", type="secondary")

        if saved:
            ns_date = str(f_nsdate) if f_ns == "Y" else None
            db_write(
                """
                INSERT INTO executed_contracts (
                    contract_originator, date_submitted, date_executed,
                    party_a, party_b, description_of_service, region,
                    department, employee_owners, start_date, end_date,
                    termination_requirements, auto_renewal, non_solicit,
                    non_solicit_termination_date, notes, contract_link
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    f_orig, str(f_dsub), str(f_dexec),
                    f_pa, f_pb, f_desc, f_region,
                    f_dept, f_emps, str(f_start), str(f_end),
                    f_term, f_ar, f_ns, ns_date, f_notes, f_link,
                ),
            )
            st.session_state.pop("ec_prefill",   None)
            st.session_state.pop("ec_show_form", None)
            st.session_state.pop("ec_upload",    None)
            st.success("✅ Contract saved.")
            st.rerun()

        if cancelled:
            st.session_state.pop("ec_prefill",   None)
            st.session_state.pop("ec_show_form", None)
            st.rerun()

    st.divider()

    # ── Load data ─────────────────────────────────────────────────────────────
    df = db_df("SELECT * FROM executed_contracts ORDER BY id DESC")

    if df.empty:
        st.info("No executed contracts on file.")
        return

    DATE_COLS = [
        "date_submitted", "date_executed", "start_date",
        "end_date", "non_solicit_termination_date",
    ]
    for col in DATE_COLS:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # ── Sidebar filters ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("---")
        st.markdown("### Executed Contract Filters")

        reg_opts  = ["All"] + sorted(df["region"].dropna().unique().tolist())
        ef_region = st.selectbox("Region ", reg_opts, key="ef_reg")

        dept_opts = ["All"] + sorted(df["department"].dropna().unique().tolist())
        ef_dept   = st.selectbox("Department ", dept_opts, key="ef_dept")

        ef_ar = st.selectbox("Auto Renewal ", ["All", "Y", "N"], key="ef_ar")
        ef_ns = st.selectbox("Non-Solicit ",  ["All", "Y", "N", "N/A"], key="ef_ns")

        valid_ends = df["end_date"].dropna()
        min_end    = valid_ends.min().date() if not valid_ends.empty else date.today()
        max_end    = valid_ends.max().date() if not valid_ends.empty else date.today() + timedelta(days=730)
        ef_end     = st.date_input("End Date Range", value=(min_end, max_end), key="ef_end")

    # ── Apply filters ─────────────────────────────────────────────────────────
    mask = pd.Series(True, index=df.index)
    if ef_region != "All": mask &= df["region"] == ef_region
    if ef_dept   != "All": mask &= df["department"] == ef_dept
    if ef_ar     != "All": mask &= df["auto_renewal"] == ef_ar
    if ef_ns     != "All": mask &= df["non_solicit"] == ef_ns
    if isinstance(ef_end, (list, tuple)) and len(ef_end) == 2:
        mask &= df["end_date"].dt.date.between(ef_end[0], ef_end[1])

    filtered = df[mask].copy()

    # ── Visual alert badges ───────────────────────────────────────────────────
    today = date.today()

    def badge(row):
        end = row["end_date"]
        if pd.isna(end):
            return ""
        days = (end.date() - today).days
        ar   = str(row.get("auto_renewal", "")).strip().upper()
        if days <= 30:              return "🔴"
        if ar == "Y" and days <= 60: return "🔵"
        if days <= 60:              return "🟡"
        if days <= 90:              return "🟠"
        return ""

    filtered.insert(0, "⚠️", filtered.apply(badge, axis=1))

    # ── Legend ────────────────────────────────────────────────────────────────
    st.markdown(
        "**Visual Flags:** &nbsp;"
        "🔴 End date ≤30 days &nbsp;|&nbsp; "
        "🔵 Auto-renewal Y + end date ≤60 days &nbsp;|&nbsp; "
        "🟡 End date 31–60 days &nbsp;|&nbsp; "
        "🟠 End date 61–90 days"
    )

    # ── Column order ──────────────────────────────────────────────────────────
    COL_ORDER = [
        "⚠️", "id",
        "contract_originator", "date_submitted", "date_executed",
        "party_a", "party_b", "description_of_service",
        "region", "department", "employee_owners",
        "start_date", "end_date", "termination_requirements",
        "auto_renewal", "non_solicit", "non_solicit_termination_date",
        "notes", "contract_link",
    ]
    display = filtered[[c for c in COL_ORDER if c in filtered.columns]].copy()

    # Format date columns as strings for the editor.
    # Pull from df (still datetime) to avoid AttributeError if the column
    # was already converted in a prior loop pass on the same object.
    for col in DATE_COLS:
        if col in display.columns:
            src = df.loc[filtered.index, col] if col in df.columns else display[col]
            display[col] = (
                src.dt.strftime("%Y-%m-%d")
                   .where(src.notna(), other="")
            )

    # ── Inline data editor ────────────────────────────────────────────────────
    edited = st.data_editor(
        display,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        disabled=["⚠️", "id"],
        key="exec_editor",
        column_config={
            "⚠️":    st.column_config.TextColumn("⚠️", width="small"),
            "id":    st.column_config.NumberColumn("ID", width="small"),
            # TextColumn (not LinkColumn) — values may be file paths, not URLs
            "contract_link": st.column_config.TextColumn("contract_link"),
        },
    )

    # ── Action bar: Save Changes + CSV Export ─────────────────────────────────
    col_save, col_csv, _ = st.columns([1, 1, 4])

    with col_save:
        if st.button("💾 Save Changes", type="primary"):
            ops = []
            for _, row in edited.iterrows():
                row_id = row.get("id")
                if pd.isna(row_id):
                    continue
                ops.append((
                    """
                    UPDATE executed_contracts SET
                        contract_originator=?, date_submitted=?, date_executed=?,
                        party_a=?, party_b=?, description_of_service=?,
                        region=?, department=?, employee_owners=?,
                        start_date=?, end_date=?, termination_requirements=?,
                        auto_renewal=?, non_solicit=?, non_solicit_termination_date=?,
                        notes=?, contract_link=?
                    WHERE id=?
                    """,
                    (
                        row.get("contract_originator"),
                        row.get("date_submitted"),
                        row.get("date_executed"),
                        row.get("party_a"),
                        row.get("party_b"),
                        row.get("description_of_service"),
                        row.get("region"),
                        row.get("department"),
                        row.get("employee_owners"),
                        row.get("start_date"),
                        row.get("end_date"),
                        row.get("termination_requirements"),
                        row.get("auto_renewal"),
                        row.get("non_solicit"),
                        row.get("non_solicit_termination_date"),
                        row.get("notes"),
                        row.get("contract_link"),
                        int(row_id),
                    ),
                ))
            if ops:
                db_write_many(ops)
                st.success(f"Saved {len(ops)} record(s).")
                st.rerun()
            else:
                st.info("No changes to save.")

    with col_csv:
        csv_bytes = display.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv_bytes,
            file_name=f"executed_contracts_{date.today()}.csv",
            mime="text/csv",
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    init_db()

    if not require_auth():
        st.stop()

    st.sidebar.title("📁 Contract Manager")
    st.sidebar.markdown("---")

    page = st.sidebar.radio(
        "Navigate",
        [
            "📄 Contract Intake",
            "📊 Intake Log & Tracker",
            "✅ Executed Contracts Dashboard",
        ],
    )

    st.sidebar.markdown("---")
    st.sidebar.caption("Contract Manager · POC v1.0")

    if page.startswith("📄"):
        page_intake()
    elif page.startswith("📊"):
        page_tracker()
    elif page.startswith("✅"):
        page_executed()


if __name__ == "__main__":
    main()
