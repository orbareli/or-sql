import struct
import os

class MiniEngine:
    # Format: I (unsigned int, 4b), 20s (20-char string), I (unsigned int, 4b)
    # Total size: 4 + 20 + 4 = 28 bytes
    RECORD_FORMAT = "<I20sI" 
    RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

    def __init__(self, db_path):
        self.db_path = db_path
        self.index = {}  # id -> offset
        self._load_index()

    def _load_index(self):
        """Builds index by scanning the file on startup."""
        if not os.path.exists(self.db_path):
            return
        
        with open(self.db_path, "rb") as f:
            offset = 0
            while chunk := f.read(self.RECORD_SIZE):
                # Unpack just the ID (the first 4 bytes)
                record_id = struct.unpack("<I", chunk[:4])[0]
                self.index[record_id] = offset
                offset += self.RECORD_SIZE

    def insert(self, user_id, name, age):
        if user_id in self.index:
            raise ValueError("ID already exists!")

        # Encode name to bytes and pad to 20 chars
        name_bytes = name.encode('utf-8')[:20].ljust(20, b'\x00')
        data = struct.pack(self.RECORD_FORMAT, user_id, name_bytes, age)

        with open(self.db_path, "ab") as f:
            offset = f.tell()
            f.write(data)
            self.index[user_id] = offset

    def select_by_id(self, user_id):
        offset = self.index.get(user_id)
        if offset is None:
            return None

        with open(self.db_path, "rb") as f:
            f.seek(offset)
            chunk = f.read(self.RECORD_SIZE)
            return self._unpack(chunk)

    def select_all(self):
        results = []
        with open(self.db_path, "rb") as f:
            while chunk := f.read(self.RECORD_SIZE):
                results.append(self._unpack(chunk))
        return results

    def _unpack(self, chunk):
        user_id, name_bytes, age = struct.unpack(self.RECORD_FORMAT, chunk)
        return {"id": user_id, "name": name_bytes.decode('utf-8').strip('\x00'), "age": age}

class Page:
    PAGE_SIZE = 4096  # גודל סטנדרטי בתעשייה
    # הדר: 2 בייטים למספר הרשומות, 2 בייטים לתחילת השטח הפנוי
    HEADER_FORMAT = "!HH" 
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, data=None):
        if data:
            self.data = bytearray(data)
            self.num_records, self.free_ptr = struct.unpack(self.HEADER_FORMAT, self.data[:self.HEADER_SIZE])
        else:
            self.data = bytearray(self.PAGE_SIZE)
            self.num_records = 0
            self.free_ptr = self.HEADER_SIZE # מתחילים מיד אחרי ההדר
            self._update_header()

    def _update_header(self):
        struct.pack_into(self.HEADER_FORMAT, self.data, 0, self.num_records, self.free_ptr)

    def add_record(self, record_bytes):
        if self.free_ptr + len(record_bytes) > self.PAGE_SIZE:
            return False # הדף מלא! צריך לפתוח דף חדש (כאן נכנס ה-B-Tree לתמונה)
        
        # כתיבת הנתונים במיקום הפנוי הבא
        self.data[self.free_ptr : self.free_ptr + len(record_bytes)] = record_bytes
        self.free_ptr += len(record_bytes)
        self.num_records += 1
        self._update_header()
        return True
import os

class Pager:
    PAGE_SIZE = 4096

    def __init__(self, db_path):
        self.db_path = db_path
        # פתיחת הקובץ במצב קריאה וכתיבה בינארית (r+b)
        if not os.path.exists(db_path):
            open(db_path, "wb").close()
        self.file = open(db_path, "r+b")
        self.file_size = os.path.getsize(db_path)
        self.num_pages = self.file_size // self.PAGE_SIZE

    def get_page(self, page_num):
        """קורא דף ספציפי מהדיסק למערך בייטים"""
        if page_num >= self.num_pages:
            return None
        self.file.seek(page_num * self.PAGE_SIZE)
        return bytearray(self.file.read(self.PAGE_SIZE))

    def write_page(self, page_num, data):
        """כותב מערך בייטים חזרה למיקום של דף ספציפי"""
        self.file.seek(page_num * self.PAGE_SIZE)
        self.file.write(data)
        self.file.flush() # מוודא שהמידע באמת נכתב לדיסק
        if page_num >= self.num_pages:
            self.num_pages = page_num + 1

    def close(self):
        self.file.close()
class Table:
    def __init__(self, db_path):
        self.pager = Pager(db_path)
        
    def insert(self, record_id, name, age):
        # בגרסה פשוטה: ננסה תמיד להכניס לדף האחרון
        target_page_num = max(0, self.pager.num_pages - 1)
        data = self.pager.get_page(target_page_num)
        
        if data is None: # קובץ חדש לגמרי
            page = Page() 
            target_page_num = 0
        else:
            page = Page(data)

        # יצירת הבינארי של הרשומה (נחזור לפורמט קבוע לצורך הפשטות כרגע)
        record_bytes = struct.pack("<I20sI", record_id, name.encode().ljust(20, b'\x00'), age)

        if not page.add_record(record_bytes):
            # הדף מלא! יוצרים דף חדש לגמרי
            new_page = Page()
            new_page.add_record(record_bytes)
            self.pager.write_page(self.pager.num_pages, new_page.data)
        else:
            # הדף הנוכחי עודכן, נשמור אותו חזרה
            self.pager.write_page(target_page_num, page.data)

    def select_all(self):
        results = []
        for i in range(self.pager.num_pages):
            data = self.pager.get_page(i)
            page = Page(data)
            # כאן נצטרך פונקציה ב-Page שסורקת את כל הרשומות שלו
            results.extend(page.get_all_records()) 
        return results


db = MiniEngine(r"c:\Users\or\Desktop\users.db")

# 1. Insert records

db.insert(1, "Alice", 30)
db.insert(2, "Bob", 25)
# 2. Point lookup (Fast!)
print(f"ID 2: {db.select_by_id(2)}")

# 3. Full scan
print(f"All records: {db.select_all()}")