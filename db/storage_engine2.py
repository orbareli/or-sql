import struct
import os

# --- שכבה 1: ה-Page (ניהול הזיכרון בתוך הבלוק) ---
class Page:
    PAGE_SIZE = 4096
    # פורמט הכותרת: H (unsigned short, 2 בייטים) למספר רשומות, ו-H למצביע לשטח פנוי.
    HEADER_FORMAT = "!HH" 
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, data=None):
        if data:
            self.data = bytearray(data)
            self.num_records, self.free_ptr = struct.unpack(self.HEADER_FORMAT, self.data[:self.HEADER_SIZE])
        else:
            self.data = bytearray(self.PAGE_SIZE)
            self.num_records = 0
            self.free_ptr = self.HEADER_SIZE
            self._update_header()

    def _update_header(self):
        """חורט את המידע המעודכן לתוך בייטים בתחילת הדף"""
        struct.pack_into(self.HEADER_FORMAT, self.data, 0, self.num_records, self.free_ptr)

    def add_record(self, record_bytes):
        """מוסיף רשומה לדף אם יש מקום"""
        if self.free_ptr + len(record_bytes) > self.PAGE_SIZE:
            return False # הדף מלא
        
        # כותב את הרשומה במיקום הפנוי הבא
        self.data[self.free_ptr : self.free_ptr + len(record_bytes)] = record_bytes
        self.free_ptr += len(record_bytes)
        self.num_records += 1
        self._update_header()
        return True

# --- שכבה 2: ה-Pager (ניהול הקשר עם הדיסק) ---
class Pager:
    def __init__(self, db_path):
        self.db_path = db_path
        if not os.path.exists(db_path):
            open(db_path, "wb").close()
        self.file = open(db_path, "r+b")
        # חישוב כמה דפים יש בקובץ לפי הגודל שלו חלקי 4096
        self.num_pages = os.path.getsize(db_path) // Page.PAGE_SIZE

    def get_page(self, page_num):
        if page_num >= self.num_pages:
            return None
        self.file.seek(page_num * Page.PAGE_SIZE)
        return bytearray(self.file.read(Page.PAGE_SIZE))

    def write_page(self, page_num, data):
        self.file.seek(page_num * Page.PAGE_SIZE)
        self.file.write(data)
        self.file.flush()
        # עדכון מונה הדפים אם כתבנו דף חדש בסוף
        if page_num >= self.num_pages:
            self.num_pages = page_num + 1

# --- שכבה 3: ה-Table (ניהול הלוגיקה של הנתונים) ---
class Table:
    # פורמט הרשומה: ID (I), Name (20s), Age (I) = 28 בייטים
    RECORD_FORMAT = "<I20sI"
    RECORD_SIZE = struct.calcsize(RECORD_FORMAT)

    def __init__(self, db_path):
        self.db_path = db_path
        self.pager = Pager(db_path)
        #self.next_id = 1
        self.next_id = self.load_metadata()
    def save_metadata(self):
    # נשמור את next_id בבייטים הראשונים של דף מיוחד (או קובץ נפרד)
        with open(self.db_path + ".meta", "wb") as f:
            f.write(struct.pack("<I", self.next_id))

    def load_metadata(self):
        meta_path = self.db_path + ".meta"
        if os.path.exists(meta_path):
            with open(meta_path, "rb") as f:
                return struct.unpack("<I", f.read(4))[0]
        return 1 # אם אין קובץ, מתחילים מ-1

    def insert(self, name, age):
        new_id = self.next_id
        # מנסה להכניס לדף האחרון הקיים
        target_page_num = max(0, self.pager.num_pages - 1)
        data = self.pager.get_page(target_page_num)
        
        if data is None:
            page = Page()
        else:
            page = Page(data)

        # הכנת הנתונים למבנה בינארי
        name_bytes = name.encode('utf-8')[:20].ljust(20, b'\x00')
        record_bytes = struct.pack(self.RECORD_FORMAT, new_id,name_bytes, age)

        if not page.add_record(record_bytes):
            # אם הדף מלא, יוצר דף חדש לגמרי בסוף הקובץ
            page = Page()
            page.add_record(record_bytes)
            self.pager.write_page(self.pager.num_pages, page.data)
        else:
            # אם הדף לא היה מלא, דורס את הגרסה הישנה שלו עם החדשה
            self.pager.write_page(target_page_num, page.data)
        self.next_id += 1
        self.save_metadata()

    def select_all(self):
        results = []
        for i in range(self.pager.num_pages):
            data = self.pager.get_page(i)
            page = Page(data)
            
            # סריקה של כל הרשומות בתוך הדף
            current_pos = Page.HEADER_SIZE
            for _ in range(page.num_records):
                record_bin = page.data[current_pos : current_pos + self.RECORD_SIZE]
                user_id, name_raw, age = struct.unpack(self.RECORD_FORMAT, record_bin)
                results.append({
                    "id": user_id, 
                    "name": name_raw.decode('utf-8').strip('\x00'), 
                    "age": age
                })
                current_pos += self.RECORD_SIZE
        return results

# --- הרצה ובדיקה ---
if __name__ == "__main__":
    db_file = r"C:\Users\or\Desktop\db\db_files\my_database.db"
    
    # ניקוי הקובץ הישן לצורך הבדיקה (אופציונלי)
    if os.path.exists(db_file): os.remove(db_file)
    
    table = Table(db_file)
    table.insert("Or", 25)
    table.insert("Alice", 30)
    print("--- all records---")
    for row in table.select_all():
        print(row)
    
    print(f"\n the file size in the disk: {os.path.getsize(db_file)} בייטים (בדיוק דף אחד של 4KB)")