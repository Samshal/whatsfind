import time
import datetime
from typing import Optional
import streamlit as st
import pandas as pd
from db import ensure_db, connect, upsert_chat, upsert_participant, bulk_insert_messages, list_facets, search, get_thread, get_chat_messages, get_chat_message_count, get_all_chats_with_stats, check_chat_has_messages, clear_chat_messages, clear_all_data
from parser import import_zip_to_rows, store_zip_data, get_media_file, debug_media_files
import base64

st.set_page_config(page_title="WhatsFind (Streamlit)", layout="wide")
st.title("🔎 WhatsFind — WhatsApp chat search (local)")
st.caption("All processing is **local**. Upload your WhatsApp chat export ZIP to begin.")

@st.cache_resource
def _init_db(db_path: Optional[str] = None) -> str:
    return ensure_db(db_path)

db_path = _init_db()
st.sidebar.success(f"Database: {db_path}")

# Add database management section
with st.sidebar.expander("🛠️ Database Management"):
    with connect() as conn:
        existing_chats = get_all_chats_with_stats(conn)
        if existing_chats:
            st.write(f"Current data: {len(existing_chats)} chat(s)")
            total_messages = sum(chat['message_count'] for chat in existing_chats)
            st.write(f"Total messages: {total_messages}")
            
            if st.button("🗑️ Clear All Data", type="secondary"):
                with connect() as clear_conn:
                    clear_conn.execute("BEGIN")
                    try:
                        clear_all_data(clear_conn)
                        clear_conn.execute("COMMIT")
                        st.success("✅ All data cleared successfully!")
                        st.rerun()
                    except Exception as e:
                        clear_conn.execute("ROLLBACK")
                        st.error(f"Failed to clear data: {e}")
        else:
            st.write("No data in database")

def display_media_file(filename: str, media_data: bytes):
    """Display media file based on its type"""
    if not media_data:
        st.error(f"Could not load media file: {filename}")
        return
    
    file_ext = filename.lower().split('.')[-1] if '.' in filename else ''
    
    # Special case: DOC files with no extension (just a dot) are typically PDFs
    if filename.startswith('DOC-') and filename.endswith('.'):
        file_ext = 'pdf'
    
    try:
        if file_ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
            # Display images
            st.image(media_data, caption=filename, use_column_width=True)
        
        elif file_ext == 'pdf':
            # Display PDF download link and viewer
            b64_pdf = base64.b64encode(media_data).decode()
            pdf_display = f'<iframe src="data:application/pdf;base64,{b64_pdf}" width="100%" height="600" type="application/pdf"></iframe>'
            st.markdown(pdf_display, unsafe_allow_html=True)
            st.download_button(
                label=f"📄 Download {filename}",
                data=media_data,
                file_name=filename if not filename.endswith('.') else filename + 'pdf',
                mime="application/pdf"
            )
        
        elif file_ext in ['opus', 'mp3', 'wav', 'ogg', 'm4a']:
            # Display audio files
            st.audio(media_data, format=f'audio/{file_ext}')
            st.download_button(
                label=f"🎵 Download {filename}",
                data=media_data,
                file_name=filename,
                mime=f"audio/{file_ext}"
            )
        
        elif file_ext in ['mp4', 'avi', 'mov', 'mkv']:
            # Display video files
            st.video(media_data)
            st.download_button(
                label=f"🎬 Download {filename}",
                data=media_data,
                file_name=filename,
                mime=f"video/{file_ext}"
            )
        
        elif file_ext in ['doc', 'docx']:
            # Word documents
            st.download_button(
                label=f"📝 Download {filename}",
                data=media_data,
                file_name=filename,
                mime="application/msword"
            )
        
        elif file_ext in ['xls', 'xlsx']:
            # Excel files
            st.download_button(
                label=f"📊 Download {filename}",
                data=media_data,
                file_name=filename,
                mime="application/vnd.ms-excel"
            )
        
        elif file_ext in ['ppt', 'pptx']:
            # PowerPoint files
            st.download_button(
                label=f"📈 Download {filename}",
                data=media_data,
                file_name=filename,
                mime="application/vnd.ms-powerpoint"
            )
        
        else:
            # Generic file download - but if it's a DOC- file, suggest it's a PDF
            if filename.startswith('DOC-'):
                st.info("💡 This appears to be a document file, likely a PDF.")
                st.download_button(
                    label=f"📄 Download {filename} (PDF)",
                    data=media_data,
                    file_name=filename if not filename.endswith('.') else filename + 'pdf',
                    mime="application/pdf"
                )
            else:
                st.download_button(
                    label=f"📎 Download {filename}",
                    data=media_data,
                    file_name=filename,
                    mime="application/octet-stream"
                )
    except Exception as e:
        st.error(f"Error displaying {filename}: {str(e)}")
        # Fallback download for DOC files
        if filename.startswith('DOC-'):
            st.download_button(
                label=f"� Download {filename} (PDF)",
                data=media_data,
                file_name=filename if not filename.endswith('.') else filename + 'pdf',
                mime="application/pdf"
            )
        else:
            st.download_button(
                label=f"�📎 Download {filename}",
                data=media_data,
                file_name=filename,
                mime="application/octet-stream"
            )

with st.expander("Import WhatsApp Export (ZIP)", expanded=True):
    # Import options
    col1, col2 = st.columns(2)
    with col1:
        import_mode = st.radio(
            "Import Mode:",
            ["Skip duplicates (recommended)", "Clear all data first", "Add anyway (may create duplicates)"],
            help="Choose how to handle existing data when importing"
        )
    
    with col2:
        zip_file = st.file_uploader("Upload WhatsApp export ZIP", type=["zip"], accept_multiple_files=False)
    
    # Show existing data warning if applicable
    with connect() as conn:
        existing_chats = get_all_chats_with_stats(conn)
        if existing_chats and import_mode == "Add anyway (may create duplicates)":
            st.warning("⚠️ You have existing chats. Importing with 'Add anyway' may create duplicates.")
        elif existing_chats and import_mode != "Clear all data first":
            st.info(f"ℹ️ You have {len(existing_chats)} existing chat(s). Import mode: {import_mode}")
    
    if zip_file is not None:
        bytes_data = zip_file.read()
        # Store ZIP data for media access
        store_zip_data(bytes_data)
        
        # Debug: Show media files detected
        with st.expander("🔍 Debug: Media Files Detected", expanded=False):
            media_debug = debug_media_files(bytes_data)
            st.write(f"Found {len([f for f in media_debug.keys() if f != 'error'])} media files:")
            
            doc_files = {k: v for k, v in media_debug.items() if k.startswith('DOC-')}
            if doc_files:
                st.write("**Document files:**")
                for filename, info in doc_files.items():
                    status = "✅ Detected" if info.get('matches_pattern') else "❌ Not detected"
                    st.write(f"- {filename} → {status}")
        
        imported = 0
        skipped = 0
        start = time.time()
        with connect() as conn:
            conn.execute("BEGIN")
            try:
                # Handle clear all data mode
                if import_mode == "Clear all data first":
                    st.info("🗑️ Clearing all existing data...")
                    clear_all_data(conn)
                    st.success("✅ All existing data cleared.")
                
                chat_count = 0
                for title, msg_iter in import_zip_to_rows(bytes_data):
                    chat_count += 1
                    chat_id = upsert_chat(conn, title)
                    
                    # Check if chat already has messages (unless we're in "add anyway" mode)
                    if import_mode == "Skip duplicates (recommended)" and check_chat_has_messages(conn, chat_id):
                        st.info(f"⏭️ Skipping chat '{title}' - already has messages")
                        skipped += 1
                        continue
                    elif import_mode == "Clear all data first" and check_chat_has_messages(conn, chat_id):
                        # This shouldn't happen since we cleared data, but just in case
                        clear_chat_messages(conn, chat_id)
                    
                    st.info(f"📥 Processing chat: {title}")
                    
                    batch = []
                    seen_senders = set()
                    message_count = 0
                    for ts_ms, sender, text, msg_type, has_media, media_path in msg_iter:
                        message_count += 1
                        if sender and sender not in seen_senders:
                            upsert_participant(conn, chat_id, sender)
                            seen_senders.add(sender)
                        batch.append((chat_id, ts_ms, sender, msg_type, text, has_media, media_path))
                        if len(batch) >= 1000:
                            bulk_insert_messages(conn, batch)
                            imported += len(batch)
                            batch.clear()
                    if batch:
                        bulk_insert_messages(conn, batch)
                        imported += len(batch)
                    st.success(f"✅ Chat '{title}': imported {message_count} messages")
                
                conn.execute("COMMIT")
            except Exception as e:
                conn.execute("ROLLBACK")
                st.error(f"Import failed: {e}")
                import traceback
                st.error(f"Full traceback: {traceback.format_exc()}")
            else:
                success_msg = f"Imported {imported} messages from {chat_count - skipped} chat(s) in {time.time() - start:.2f}s"
                if skipped > 0:
                    success_msg += f" (Skipped {skipped} duplicate chat(s))"
                st.success(success_msg)

st.divider()
st.header("Browse Chats")

with connect() as conn:
    all_chats = get_all_chats_with_stats(conn)
    
    if not all_chats:
        st.info("No chats available. Import a WhatsApp export first.")
    else:
        # Chat selection
        chat_options = {}
        for chat in all_chats:
            import datetime
            last_msg_date = datetime.datetime.fromtimestamp(chat["last_message_ts"]/1000).strftime("%Y-%m-%d") if chat["last_message_ts"] else "No messages"
            chat_label = f"{chat['title']} ({chat['message_count']} messages, last: {last_msg_date})"
            chat_options[chat_label] = chat["id"]
        
        selected_chat_label = st.selectbox("Select a chat to read:", list(chat_options.keys()))
        selected_chat_id = chat_options[selected_chat_label]
        
        # Get selected chat info
        selected_chat = next(chat for chat in all_chats if chat["id"] == selected_chat_id)
        total_messages = get_chat_message_count(conn, selected_chat_id)
        
        st.write(f"**Chat:** {selected_chat['title']}")
        st.write(f"**Total messages:** {total_messages}")
        st.write(f"**Participants:** {selected_chat['participant_count']}")
        st.info("📅 Messages are displayed from **newest to oldest** - start reading from the top!")
        
        if selected_chat["first_message_ts"] and selected_chat["last_message_ts"]:
            first_date = datetime.datetime.fromtimestamp(selected_chat["first_message_ts"]/1000).strftime("%Y-%m-%d %H:%M")
            last_date = datetime.datetime.fromtimestamp(selected_chat["last_message_ts"]/1000).strftime("%Y-%m-%d %H:%M")
            st.write(f"**Date range:** {first_date} to {last_date}")
        
        # Pagination controls
        messages_per_page = st.slider("Messages per page", min_value=10, max_value=200, value=50, step=10)
        total_pages = (total_messages + messages_per_page - 1) // messages_per_page
        
        if total_pages > 0:
            # Use session state to track page
            if 'current_page' not in st.session_state:
                st.session_state.current_page = 1
            if 'current_chat_id' not in st.session_state:
                st.session_state.current_chat_id = selected_chat_id
            
            # Reset page if chat changed
            if st.session_state.current_chat_id != selected_chat_id:
                st.session_state.current_page = 1
                st.session_state.current_chat_id = selected_chat_id
            
            # Page navigation
            col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
            
            with col1:
                if st.button("⏮️ First") and st.session_state.current_page > 1:
                    st.session_state.current_page = 1
                    st.rerun()
            
            with col2:
                if st.button("◀️ Prev") and st.session_state.current_page > 1:
                    st.session_state.current_page -= 1
                    st.rerun()
            
            with col3:
                new_page = st.number_input("Page", min_value=1, max_value=total_pages, 
                                         value=st.session_state.current_page, step=1, key="page_input")
                if new_page != st.session_state.current_page:
                    st.session_state.current_page = new_page
                    st.rerun()
            
            with col4:
                if st.button("▶️ Next") and st.session_state.current_page < total_pages:
                    st.session_state.current_page += 1
                    st.rerun()
            
            with col5:
                if st.button("⏭️ Last") and st.session_state.current_page < total_pages:
                    st.session_state.current_page = total_pages
                    st.rerun()
            
            current_page = st.session_state.current_page
            offset = (current_page - 1) * messages_per_page
            
            # Calculate message range (newest first)
            start_msg = offset + 1
            end_msg = min(offset + messages_per_page, total_messages)
            st.write(f"Showing page {current_page} of {total_pages} (newest messages {start_msg}-{end_msg} of {total_messages})")
            
            # Get messages for current page
            messages = get_chat_messages(conn, selected_chat_id, messages_per_page, offset)
            
            if messages:
                # Display messages
                st.write("---")
                for i, msg in enumerate(messages):
                    # Add visual separator between messages (except for the first one)
                    if i > 0:
                        st.markdown("---")
                    
                    # Format timestamp
                    msg_time = datetime.datetime.fromtimestamp(msg["ts"]/1000)
                    time_str = msg_time.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # Message header with sender and time
                    if msg["sender"]:
                        st.write(f"**{msg['sender']}** — *{time_str}*")
                    else:
                        st.write(f"**System** — *{time_str}*")
                    
                    # Message content
                    if msg["text"]:
                        # Handle multi-line messages
                        text_lines = msg["text"].split('\n')
                        for line in text_lines:
                            if line.strip():  # Skip empty lines
                                st.write(f"> {line}")
                            else:
                                st.write("")
                    
                    # Media display
                    if msg["has_media"]:
                        if msg["media_path"]:
                            # Try to load and display the actual media file
                            media_data = get_media_file(msg["media_path"])
                            if media_data:
                                st.write("📎 **Media:**")
                                # Special labeling for DOC files
                                if msg["media_path"].startswith('DOC-') and msg["media_path"].endswith('.'):
                                    expander_label = f"View {msg['media_path']} (PDF Document)"
                                else:
                                    expander_label = f"View {msg['media_path']}"
                                    
                                with st.expander(expander_label, expanded=False):
                                    display_media_file(msg["media_path"], media_data)
                            else:
                                st.write(f"📎 *Media file: {msg['media_path']} (not found in archive)*")
                        else:
                            st.write("📎 *Media attached (file not available)*")
            else:
                st.info("No messages found on this page.")

st.divider()
st.header("Search")

with connect() as conn:
    chats, senders, years = list_facets(conn)
    chat_map = {f'{c["title"]} (#{c["id"]})': c["id"] for c in chats}
    colq, colf = st.columns([2,3])
    with colq:
        q = st.text_input("Query (FTS5 syntax)", value="", placeholder='Examples: "power outage" or contract AND (NCC OR regulator)')
    with colf:
        c1, c2, c3, c4, c5 = st.columns(5)
        chat_label = c1.selectbox("Chat", ["Any"] + list(chat_map.keys()))
        sender = c2.selectbox("Sender", ["Any"] + senders)
        year_from = c3.selectbox("From Year", ["Any"] + years)
        year_to = c4.selectbox("To Year", ["Any"] + years)
        has_media = c5.selectbox("Has media", ["Any", "Yes", "No"])
    limit = st.number_input("Limit", min_value=10, max_value=1000, value=200, step=10)
    offset = st.number_input("Offset", min_value=0, max_value=1000000, value=0, step=50)

    def year_to_epoch_ms(y: str, end: bool=False):
        if y == "Any":
            return None
        import calendar
        if not end:
            dt = datetime.datetime(int(y), 1, 1, 0, 0, 0)
        else:
            dt = datetime.datetime(int(y), 12, 31, 23, 59, 59)
        return int(calendar.timegm(dt.timetuple()) * 1000)

    t1 = year_to_epoch_ms(year_from, end=False)
    t2 = year_to_epoch_ms(year_to, end=True)
    if (t1 is not None and t2 is not None) and t2 < t1:
        st.warning("To Year is earlier than From Year; swapping.")
        t1, t2 = t2, t1

    chat_id = None if chat_label == "Any" else chat_map[chat_label]
    sender_val = None if sender == "Any" else sender
    hm = None if has_media == "Any" else (has_media == "Yes")

    if st.button("Run search", type="primary") and q.strip():
        try:
            rows = search(conn, q.strip(), chat_id, sender_val, t1, t2, hm, int(limit), int(offset))
            if not rows:
                st.info("No results.")
            else:
                df = pd.DataFrame([dict(r) for r in rows])
                df["time"] = df["ts"].apply(lambda ms: datetime.datetime.fromtimestamp(ms/1000).isoformat())
                st.dataframe(df[["id","chat_id","time","sender","text","has_media"]], width='stretch', hide_index=True)
                st.caption("Open a specific message ID in a thread view below.")
                selected_id = st.number_input("Message id to open", min_value=int(df["id"].min()), max_value=int(df["id"].max()), value=int(df["id"].iloc[0]))
                if st.button("Open thread"):
                    thread, center = get_thread(conn, int(selected_id), context=25)
                    if not thread:
                        st.info("No thread available for that message ID.")
                    else:
                        st.subheader("Thread view")
                        for r in thread:
                            who = r["sender"] or "System"
                            st.markdown(f"**{who}** — `{r['ts']}`")
                            st.write(r["text"] or "")
                            if r["has_media"] and r["media_path"]:
                                st.caption(f"Media: {r['media_path']}")
                        st.caption("End of thread.")
        except Exception as e:
            st.error(f"Search error: {e}")
