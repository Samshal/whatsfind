import io
import os
import re
import zipfile
from typing import Iterator, Tuple, Optional
import time
import dateparser
from datetime import datetime

# Regex matches common WhatsApp formats (both bracket and dash formats)
HEADER_RE = re.compile(
    r"""^\[(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}),\s+(\d{1,2}:\d{2}(?:\s?[AP]M)?)\]\s*(.*)$|^(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}),\s+(\d{1,2}:\d{2}(?:\s?[AP]M)?)\s*[-–]\s*(.*)$""",
    re.IGNORECASE
)

def parse_whatsapp_date(date_str: str, time_str: str) -> Optional[datetime]:
    """
    Parse WhatsApp date and time strings with proper format detection.
    Handles common formats like DD/MM/YYYY, MM/DD/YYYY, etc.
    """
    from datetime import datetime
    import re
    
    # Clean up the strings
    date_str = date_str.strip()
    time_str = time_str.strip()
    
    # Parse the date components
    date_parts = re.split(r'[/-]', date_str)
    if len(date_parts) != 3:
        return None
    
    try:
        part1, part2, part3 = [int(p) for p in date_parts]
        
        # Determine year (usually the largest number or the 4-digit one)
        if part3 > 31:  # 4-digit year
            year = part3
            # For DD/MM/YYYY vs MM/DD/YYYY, use context clues
            if part1 > 12:  # First part > 12, must be day
                day, month = part1, part2
            elif part2 > 12:  # Second part > 12, must be day  
                month, day = part1, part2
            else:
                # Ambiguous case - try DD/MM/YYYY first (international standard)
                day, month = part1, part2
        elif part1 > 31:  # Year first (YYYY/MM/DD or YYYY/DD/MM)
            year = part1
            if part2 > 12:
                month, day = part3, part2
            else:
                month, day = part2, part3
        else:
            # Assume 2-digit year, make it 20XX
            year = 2000 + part3 if part3 < 50 else 1900 + part3
            # For DD/MM/YY vs MM/DD/YY
            if part1 > 12:
                day, month = part1, part2
            elif part2 > 12:
                month, day = part1, part2
            else:
                # Default to DD/MM format for international WhatsApp
                day, month = part1, part2
        
        # Validate ranges
        if not (1 <= month <= 12 and 1 <= day <= 31 and 1900 <= year <= 2100):
            return None
        
        # Parse time
        time_match = re.match(r'(\d{1,2}):(\d{2})(?:\s*([AaPp][Mm]))?', time_str)
        if not time_match:
            return None
        
        hour, minute, ampm = time_match.groups()
        hour, minute = int(hour), int(minute)
        
        # Handle AM/PM
        if ampm:
            ampm = ampm.upper()
            if ampm == 'PM' and hour != 12:
                hour += 12
            elif ampm == 'AM' and hour == 12:
                hour = 0
        
        # Validate time
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        
        return datetime(year, month, day, hour, minute)
        
    except (ValueError, OverflowError):
        return None

# Pattern to match media file references in WhatsApp messages
MEDIA_PATTERNS = [
    r'(IMG-\d{8}-WA\d{4}\.jpg)',   # Images
    r'(PTT-\d{8}-WA\d{4}\.opus)',  # Voice messages
    r'(VID-\d{8}-WA\d{4}\.mp4)',   # Videos
    r'(AUD-\d{8}-WA\d{4}\.opus)',  # Audio files
    r'(DOC-\d{8}-WA\d{4}\.?[\w]*)', # Documents (with or without extension)
    r'(STK-\d{8}-WA\d{4}\.webp)',  # Stickers
    r'(\w+\.pdf)',                  # PDF files
    r'(\w+\.docx?)',                # Word documents
    r'(\w+\.xlsx?)',                # Excel files
    r'(\w+\.pptx?)',                # PowerPoint files
]

MEDIA_REGEX = re.compile('|'.join(MEDIA_PATTERNS), re.IGNORECASE)

def _parse_header(line: str) -> Optional[Tuple[int, Optional[str], str]]:
    """
    Returns (epoch_ms, sender_or_none, text_after_sender_or_line) or None if not a header.
    We still return text for system lines (no 'sender: ' pattern).
    """
    m = HEADER_RE.match(line.strip())
    if not m:
        return None
    
    groups = m.groups()
    # Handle both bracket format [date, time] text and dash format date, time - text
    if groups[0] is not None:  # Bracket format
        date_str, time_str, rest = groups[0], groups[1], groups[2]
    else:  # Dash format
        date_str, time_str, rest = groups[3], groups[4], groups[5]
    
    # Try split "Sender: Message"
    sender = None
    text = rest
    if ": " in rest:
        possible_sender, possible_text = rest.split(": ", 1)
        sender = possible_sender if possible_text is not None else None
        text = possible_text if possible_text is not None else rest
    
    # Parse timestamp with improved date parsing
    dt = parse_whatsapp_date(date_str, time_str)
    if not dt:
        return None
    epoch_ms = int(time.mktime(dt.timetuple()) * 1000)
    return epoch_ms, sender, text

def iter_messages_from_text(fp: io.TextIOBase, available_media: Optional[set] = None) -> Iterator[Tuple[int, Optional[str], str, str, int, Optional[str]]]:
    """
    Stream messages from a WhatsApp chat txt file.
    Yields tuples: (ts_ms, sender, text, type, has_media, media_path)
    """
    if available_media is None:
        available_media = set()
    
    current = None  # (ts_ms, sender, text, type, has_media, media_path)
    for raw in fp:
        line = raw.rstrip("\n")
        header = _parse_header(line)
        if header is not None:
            # flush previous
            if current:
                yield current
            ts_ms, sender, text = header
            msg_type = "message" if sender else "system"
            
            # Check for media references
            has_media = 0
            media_path = None
            
            # Check for explicit media omitted messages
            if "<Media omitted>" in text or "image omitted" in text.lower():
                has_media = 1
            
            # Check for media file references in the text
            media_match = MEDIA_REGEX.search(text)
            if media_match:
                # Extract the media filename
                potential_media = media_match.group()
                if potential_media in available_media:
                    has_media = 1
                    media_path = potential_media
            
            current = (ts_ms, sender, text, msg_type, has_media, media_path)
        else:
            # continuation of previous
            if current:
                ts_ms, sender, text, msg_type, has_media, media_path = current
                text = (text + "\n" + line) if text else line
                
                # Re-check for media in the updated text
                if not media_path:  # Only if we haven't found media yet
                    media_match = MEDIA_REGEX.search(text)
                    if media_match:
                        potential_media = media_match.group()
                        if potential_media in available_media:
                            has_media = 1
                            media_path = potential_media
                
                current = (ts_ms, sender, text, msg_type, has_media, media_path)
            else:
                continue
    if current:
        yield current

def import_zip_to_rows(zip_bytes: bytes):
    """
    Given a WhatsApp export ZIP (bytes), yields (chat_title, iterator_of_messages) for each *_chat.txt inside.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        # First, collect all available media files
        available_media = set()
        for name in z.namelist():
            if not name.lower().endswith(".txt"):
                filename = os.path.basename(name)
                available_media.add(filename)
        
        # Then process text files
        for name in z.namelist():
            if name.lower().endswith(".txt"):
                title = os.path.splitext(os.path.basename(name))[0]
                f = z.open(name)
                try:
                    text = io.TextIOWrapper(f, encoding="utf-8")
                    # Test read to verify encoding
                    text.read(1)
                    text.seek(0)  # Reset to beginning
                except Exception:
                    f.close()
                    f = z.open(name)
                    text = io.TextIOWrapper(f, encoding="latin-1", errors="replace")
                yield title, iter_messages_from_text(text, available_media)

# Global variable to store the current ZIP data for media access
_current_zip_data = None

def store_zip_data(zip_bytes: bytes):
    """Store ZIP data globally for media file access"""
    global _current_zip_data
    _current_zip_data = zip_bytes

def get_media_file(filename: str) -> Optional[bytes]:
    """Retrieve a media file from the stored ZIP data"""
    global _current_zip_data
    if _current_zip_data is None:
        return None
    
    try:
        with zipfile.ZipFile(io.BytesIO(_current_zip_data)) as z:
            # Try exact filename match first
            for name in z.namelist():
                if os.path.basename(name) == filename:
                    return z.read(name)
            
            # If no exact match, try case-insensitive match
            filename_lower = filename.lower()
            for name in z.namelist():
                if os.path.basename(name).lower() == filename_lower:
                    return z.read(name)
                    
    except Exception:
        pass
    return None

def debug_media_files(zip_bytes: bytes) -> dict:
    """Debug function to show what media files are available"""
    media_files = {}
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            for name in z.namelist():
                if not name.lower().endswith(".txt"):
                    basename = os.path.basename(name)
                    # Check if it matches any of our patterns
                    match = MEDIA_REGEX.search(basename)
                    media_files[basename] = {
                        'full_path': name,
                        'matches_pattern': bool(match),
                        'pattern_match': match.group() if match else None
                    }
    except Exception as e:
        media_files['error'] = str(e)
    return media_files
