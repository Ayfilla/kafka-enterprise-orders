from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # ❤️ ВАЖНО: включает CORS для Dashboard

@app.route("/api/orders")
def orders():
    return jsonify([
        {
            "order_id": 1091,
            "customer_id": 5982,
            "amount": 303.45,
            "country": "IN",
            "status": "CREATED",
            "created_at": "2025-12-08T19:33:32.371897Z"
        },
        {
            "order_id": 1502,
            "customer_id": 8530,
            "amount": 107.84,
            "country": "CA",
            "status": "CANCELLED",
            "created_at": "2025-12-08T19:47:20.909447Z"
        },
        {
            "order_id": 2452,
            "customer_id": 5627,
            "amount": 329.49,
            "country": "GB",
            "status": "CONFIRMED",
            "created_at": "2025-12-08T20:19:16.167818Z"
        }
    ])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
