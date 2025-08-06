# app.py
from flask import Flask
import socket
import threading
from consumer_confluent import start_consumer
app = Flask(__name__)

@app.route("/")
def hello():
    return f"Hello from {socket.gethostname()}"

if __name__ == "__main__":
    t = threading.Thread(target=start_consumer, daemon=True)
    t.start()
    app.run(host="0.0.0.0", port=5000)