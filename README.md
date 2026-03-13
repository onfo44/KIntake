# Contract Manager

Proof-of-concept corporate contract intake, tracking, and executed contract management tool built with Streamlit and the Claude API.

---

## Prerequisites

- Python 3.9+
- An Anthropic API key ([console.anthropic.com](https://console.anthropic.com))

---

## Local Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Set the API key

Create the secrets file:

```bash
mkdir -p .streamlit
cp .streamlit/secrets.toml.example .streamlit/secrets.toml
```

Then edit `.streamlit/secrets.toml` and replace the placeholder with your real key:

```toml
ANTHROPIC_API_KEY = "sk-ant-..."
```

> ⚠️ Never commit `secrets.toml` to version control. It is listed in `.gitignore`.

### 3. Run the app

```bash
streamlit run app.py
```

The app will open at `http://localhost:8501`.

---

## Deploy to Streamlit Community Cloud

1. Push this repository to GitHub. Include `app.py`, `requirements.txt`, and `README.md`. Do **not** commit `.streamlit/secrets.toml`.
2. Go to [share.streamlit.io](https://share.streamlit.io) and click **New app**.
3. Connect your GitHub repo, set the main file to `app.py`, and deploy.
4. In the app dashboard → **Settings → Secrets**, add:

```toml
ANTHROPIC_API_KEY = "sk-ant-..."
```

5. Save and reboot the app.

---

## Login

Password: configured in `app.py` → `PASSWORD` constant. Contact your administrator for the access credential.

---

## Database

SQLite database `hs_contracts.db` is created automatically in the working directory on first run. It contains three tables:

- `settings` — persists the round-robin lawyer assignment counter
- `intake_log` — contract intake submissions
- `executed_contracts` — fully executed contract records

---

## Claude Model

The app uses `claude-sonnet-4-20250514`. To change the model, update the `MODEL` constant at the top of `app.py`.

---

## Architecture Notes

| Component | Implementation |
|---|---|
| Auth | `st.session_state` password gate |
| Database | SQLite via `sqlite3` stdlib |
| PDF parsing | `pdfplumber` |
| DOCX parsing | `python-docx` |
| AI extraction | Anthropic Claude API |
| Lawyer assignment | Round-robin counter in `settings` table |
| Inline editing | `st.data_editor` + Save Changes button |
| CSV export | `st.download_button` on filtered view |
