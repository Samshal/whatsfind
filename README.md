# whatsfind_streamlit
Local Streamlit app that imports a WhatsApp chat export ZIP, parses it, and provides full‑text search using SQLite FTS5.

## Quick start
1) Create a virtual env (recommended) and install requirements:
   ```bash
   pip install -r requirements.txt
   ```
2) Run the app:
   ```bash
   streamlit run app.py
   ```
3) In the UI, upload your WhatsApp export ZIP. The app will parse and store into a local SQLite DB in `.whatsfind/whatsfind.db` inside your home directory by default.

## Notes
- Everything is local, no network calls.
- Works with typical WhatsApp text export formats (bracketed or not, 12/24-hour, with/without AM/PM).
- Handles multiline messages and system messages.
- Uses SQLite FTS5 for fast search on message text.
