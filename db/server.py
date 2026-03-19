# server.py — runs your DB as a server
"""import socket
import json
import sys
sys.path.insert(0, r"C:\Users\or\Desktop\or\or-sql\db")

from catalog import Catalog
from executor import Executor

catalog  = Catalog(r"C:\Users\or\Desktop\or\or-sql\db\db_files")
executor = Executor(catalog)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("localhost", 5555))
server.listen(5)
print("OrSQL server listening on port 5555...")

while True:
    conn, addr = server.accept()
    data = conn.recv(4096).decode()
    
    try:
        result = executor.run(data)
        response = json.dumps({"result": result, "error": None})
    except Exception as e:
        response = json.dumps({"result": None, "error": str(e)})
    
    conn.sendall(response.encode())
    conn.close()"""
import socket
import json
import os
import sys

# Works on both Windows and Linux — uses the file's own location
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from catalog import Catalog
from executor import Executor

# Use environment variable or default to /data (the Docker volume)
DB_DIR = os.environ.get("ORSQL_DB_DIR", os.path.join(BASE_DIR, "db_files"))
os.makedirs(DB_DIR, exist_ok=True)

catalog  = Catalog(DB_DIR)
executor = Executor(catalog)

# 0.0.0.0 = accept connections from anywhere (required for Railway)
# PORT from environment variable (Railway sets this automatically)
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 5555))

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # restart without waiting
server.bind((HOST, PORT))
server.listen(5)
print(f"OrSQL server listening on {HOST}:{PORT}...")

while True:
    conn, addr = server.accept()
    print(f"Connection from {addr}")
    try:
        data = conn.recv(4096).decode().strip()
        if data:
            result = executor.run(data)
            response = json.dumps({"result": result, "error": None})
        else:
            response = json.dumps({"result": None, "error": "Empty query"})
    except Exception as e:
        response = json.dumps({"result": None, "error": str(e)})
    
    conn.sendall(response.encode())
    conn.close()