
from flask import Flask, jsonify
from flask_cors import CORS
import websocket
import threading
import json
import time

app = Flask(__name__)
CORS(app)

API_KEY = 
os.getenv("Api key paste area")

WS_URL = (
    "wss://ws.twelvedata.com/v1/quotes/price"
    "?apikey=" + API_KEY
)


# =========================================================
# IFC ASSETS
# =========================================================

ASSETS = {

    "indian": {

        # Index
        "NIFTY 50": "NIFTY",

        # NSE stocks
        "RELIANCE": "RELIANCE",
        "TCS": "TCS",
        "INFOSYS": "INFY",
        "HDFC BANK": "HDFCBANK",
        "ICICI BANK": "ICICIBANK",
        "SBI": "SBIN",
        "AXIS BANK": "AXISBANK",
        "ITC": "ITC",
        "LT": "LT",
        "BHARTI AIRTEL": "BHARTIARTL",
        "TATA MOTORS": "TATAMOTORS",
        "MARUTI": "MARUTI",
        "ADANI PORTS": "ADANIPORTS",
        "NTPC": "NTPC",
        "POWER GRID": "POWERGRID"

    },

    "forex": {

        "EUR/USD": "EUR/USD",
        "GBP/USD": "GBP/USD",
        "USD/JPY": "USD/JPY",
        "USD/CHF": "USD/CHF",
        "AUD/USD": "AUD/USD",
        "USD/CAD": "USD/CAD",
        "NZD/USD": "NZD/USD",
        "EUR/GBP": "EUR/GBP",
        "EUR/JPY": "EUR/JPY",
        "GBP/JPY": "GBP/JPY",
        "XAU/USD": "XAU/USD",
        "XAG/USD": "XAG/USD"

    },

    "crypto": {

        "BTC/USD": "BTC/USD",
        "ETH/USD": "ETH/USD",
        "SOL/USD": "SOL/USD",
        "BNB/USD": "BNB/USD",
        "XRP/USD": "XRP/USD",
        "ADA/USD": "ADA/USD",
        "DOGE/USD": "DOGE/USD",
        "AVAX/USD": "AVAX/USD",
        "DOT/USD": "DOT/USD",
        "LINK/USD": "LINK/USD"

    }

}


# =========================================================
# CACHE
# =========================================================

CACHE = {}

LOCK = threading.Lock()


for market in ASSETS:

    for name, symbol in ASSETS[market].items():

        CACHE[symbol] = {

            "name": name,

            "symbol": symbol,

            "market": market,

            "price": None,

            "percent_change": None,

            "previous_price": None,

            "live": False,

            "timestamp": None

        }


# =========================================================
# SYMBOL LIST
# =========================================================

ALL_SYMBOLS = []

for market in ASSETS:

    for name, symbol in ASSETS[market].items():

        ALL_SYMBOLS.append(symbol)


# =========================================================
# LIVE PRICE
# =========================================================

def update_live_price(data):

    symbol = data.get("symbol")

    price = data.get("price")

    timestamp = data.get("timestamp")

    if not symbol:
        return

    if price is None:
        return

    try:

        price = float(price)

    except:

        return


    with LOCK:

        if symbol not in CACHE:
            return

        old = CACHE[symbol]["price"]

        CACHE[symbol]["price"] = price

        CACHE[symbol]["live"] = True

        CACHE[symbol]["timestamp"] = (
            timestamp or time.time()
        )


        # Calculate percentage
        # from previous known price

        if old is not None and old != 0:

            change = (
                (price - old)
                / old
            ) * 100

            CACHE[symbol][
                "percent_change"
            ] = change


    print(
        "LIVE:",
        symbol,
        price
    )


# =========================================================
# WEBSOCKET
# =========================================================

def start_websocket():

    print("")
    print(
        "Starting IFC Live WebSocket..."
    )

    print(
        "Symbols:",
        len(ALL_SYMBOLS)
    )


    def on_open(ws):

        print(
            "WEBSOCKET CONNECTED"
        )


        subscribe_message = {

            "action": "subscribe",

            "params": {

                "symbols":
                ",".join(
                    ALL_SYMBOLS
                )

            }

        }


        ws.send(
            json.dumps(
                subscribe_message
            )
        )


        print(
            "SUBSCRIBE SENT"
        )


    def on_message(
        ws,
        message
    ):

        try:

            data = json.loads(
                message
            )


            event = data.get(
                "event"
            )


            if event == "price":

                update_live_price(
                    data
                )


            elif event == "subscribe-status":

                print(
                    "SUBSCRIBE STATUS:",
                    data
                )


            else:

                print(
                    "WS:",
                    data
                )


        except Exception as e:

            print(
                "MESSAGE ERROR:",
                e
            )


    def on_error(
        ws,
        error
    ):

        print(
            "WEBSOCKET ERROR:",
            error
        )


    def on_close(
        ws,
        code,
        message
    ):

        print(
            "WEBSOCKET CLOSED:",
            code,
            message
        )


    while True:

        try:

            ws = websocket.WebSocketApp(

                WS_URL,

                on_open=on_open,

                on_message=on_message,

                on_error=on_error,

                on_close=on_close

            )


            ws.run_forever(

                ping_interval=10,

                ping_timeout=5

            )


        except Exception as e:

            print(
                "CONNECTION ERROR:",
                e
            )


        print(
            "Reconnecting in 5 seconds..."
        )

        time.sleep(5)


# =========================================================
# HOME
# =========================================================

@app.route("/")
def home():

    return "IFC LIVE API WORKING"


# =========================================================
# ALL ASSETS
# =========================================================

@app.route("/api/assets")
def get_assets():

    result = {

        "indian": {},

        "forex": {},

        "crypto": {}

    }


    with LOCK:

        for market in ASSETS:

            for name, symbol in ASSETS[
                market
            ].items():

                item = CACHE.get(
                    symbol
                )

                if item:

                    result[
                        market
                    ][name] = dict(
                        item
                    )


    return jsonify(
        result
    )


# =========================================================
# SINGLE ASSET
# =========================================================

@app.route(
    "/api/asset/<path:symbol>"
)
def get_asset(symbol):

    symbol = symbol.strip()

    with LOCK:

        item = CACHE.get(
            symbol
        )

        if item:

            return jsonify(
                dict(item)
            )


    return jsonify({

        "error":
        "Asset not available",

        "symbol":
        symbol

    }), 404


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    print("")
    print(
        "================================"
    )
    print(
        "        IFC LIVE API"
    )
    print(
        "================================"
    )

    print("")
    print(
        "REST quote polling: DISABLED"
    )

    print(
        "Live source: WebSocket"
    )

    print("")


    worker = threading.Thread(

        target=start_websocket,

        daemon=True

    )

    worker.start()


    print(
        "Server:"
    )

    print(
        "http://127.0.0.1:5000"
    )

    print("")


    app.run(

        host="0.0.0.0",

        port=5000,

        debug=False,

        threaded=True

    )