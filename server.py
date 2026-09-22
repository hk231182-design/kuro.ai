from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import os
import threading
import time

from otcharts import Client


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==========================================
# PRIVATE NETWORK ACCESS FIX
# ==========================================

@app.middleware("http")
async def add_private_network_header(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Private-Network"] = "true"
    return response


# ==========================================
# OTCHARTS
# ==========================================

otc_client = None

latest_prices = {}

active_symbol = "AUDUSD_otc"

stream_thread = None
stream_stop_event = None

stream_lock = threading.Lock()

# Prevent multiple reconnect attempts
stream_running = False


# ==========================================
# FREE OTCHARTS OTC SYMBOLS
# ==========================================

OTC_SYMBOLS = {
    "AUDUSD_otc",
    "BTCUSD_otc",
    "EURUSD_otc",
    "GBPUSD_otc",
    "USDJPY_otc",
}


# ==========================================
# HOME
# ==========================================

@app.get("/")
async def home():

    return {
        "status": "KURO OTC backend is running",
        "source": "OTCharts",
        "active_asset": active_symbol,
        "stream_running": stream_running,
        "prices": latest_prices,
    }


# ==========================================
# NORMALIZE SYMBOL
# ==========================================

def normalize_symbol(asset: str):

    asset = asset.strip()

    if asset.lower().endswith("_otc"):
        asset = asset[:-4]

    return asset.upper() + "_otc"


# ==========================================
# PRICE API
# ==========================================

@app.get("/price/{asset}")
async def get_price(asset: str):

    global active_symbol

    symbol = normalize_symbol(asset)

    if symbol not in OTC_SYMBOLS:

        return {
            "success": False,
            "asset": symbol,
            "message": "Unsupported OTC asset",
        }

    if active_symbol != symbol:

        switch_otc_stream(symbol)

    price = latest_prices.get(symbol)

    if price is None:

        return {
            "success": False,
            "asset": symbol,
            "message": "Waiting for live price...",
        }

    return {
        "success": True,
        "asset": symbol,
        "price": price,
    }


# ==========================================
# MANUAL ASSET SWITCH
# ==========================================

@app.get("/set-asset/{asset}")
async def set_asset(asset: str):

    symbol = normalize_symbol(asset)

    if symbol not in OTC_SYMBOLS:

        return {
            "success": False,
            "message": "Unsupported OTC asset",
            "supported": sorted(OTC_SYMBOLS),
        }

    switch_otc_stream(symbol)

    return {
        "success": True,
        "active_asset": symbol,
        "message": "OTC stream switched",
    }


# ==========================================
# SAVE LIVE PRICE
# ==========================================

def handle_otc_price(symbol, price):

    try:

        if not symbol:
            return

        price = float(price)

        latest_prices[symbol] = price

        print(
            f"[OTC PRICE] {symbol} => {price}",
            flush=True
        )

    except Exception as e:

        print(
            "[PRICE ERROR]",
            e,
            flush=True
        )


# ==========================================
# SINGLE LIVE OTCHARTS STREAM
# ==========================================

def run_otc_stream(symbol, stop_event):

    global otc_client
    global stream_running

    print(
        f"[STREAM] Starting {symbol}...",
        flush=True
    )

    stream = None

    try:

        print(
            f"[STREAM] Connecting {symbol}...",
            flush=True
        )

        stream = otc_client.stream(
            "quotex",
            symbol,
            reconnect=False
        )

        stream_running = True

        print(
            f"[STREAM] LIVE: {symbol}",
            flush=True
        )

        for tick in stream:

            if stop_event.is_set():

                print(
                    f"[STREAM] Stop requested: {symbol}",
                    flush=True
                )

                break

            try:

                price = tick.price

                handle_otc_price(
                    symbol,
                    price
                )

            except Exception as e:

                print(
                    f"[TICK ERROR] {symbol}: {e}",
                    flush=True
                )

    except Exception as e:

        print(
            f"[STREAM ERROR] {symbol}: {e}",
            flush=True
        )

    finally:

        stream_running = False

        if stream is not None:

            try:
                stream.close()
            except Exception:
                pass

        print(
            f"[STREAM] Closed: {symbol}",
            flush=True
        )

    # IMPORTANT:
    # Do NOT automatically reconnect here.
    # Free OTCharts plan allows only one live stream.
    #
    # A failed stream must be manually restarted
    # through switch_otc_stream().


# ==========================================
# SWITCH STREAM
# ==========================================

def switch_otc_stream(symbol):

    global active_symbol
    global stream_thread
    global stream_stop_event

    with stream_lock:

        if symbol not in OTC_SYMBOLS:

            print(
                f"[SWITCH ERROR] Unsupported: {symbol}",
                flush=True
            )

            return False

        # Same stream already running
        if (
            active_symbol == symbol
            and stream_thread is not None
            and stream_thread.is_alive()
        ):

            return True

        old_symbol = active_symbol

        print()
        print("=================================")
        print(
            f"[SWITCH] {old_symbol} -> {symbol}"
        )
        print("=================================")

        # Stop previous stream
        if stream_stop_event is not None:

            stream_stop_event.set()

        old_thread = stream_thread

        if (
            old_thread is not None
            and old_thread.is_alive()
            and old_thread is not threading.current_thread()
        ):

            print(
                "[STREAM] Waiting for previous stream...",
                flush=True
            )

            old_thread.join(timeout=5)

        active_symbol = symbol

        stream_stop_event = threading.Event()

        stream_thread = threading.Thread(
            target=run_otc_stream,
            args=(
                symbol,
                stream_stop_event
            ),
            daemon=True
        )

        stream_thread.start()

        print(
            f"[STREAM STARTED] {symbol}",
            flush=True
        )

        return True


# ==========================================
# OTCHARTS CONNECTION
# ==========================================

def connect_otcharts():

    global otc_client

    try:

        api_key = os.getenv(
            "OTCHARTS_API_KEY"
        )

        if not api_key:

            print(
                "[ERROR] OTCHARTS_API_KEY not found.",
                flush=True
            )

            return False

        otc_client = Client(
            api_key=api_key
        )

        print()
        print("=================================")
        print("OTCHARTS CONNECTED")
        print("=================================")

        print(
            "[OTCHARTS] Free OTC mode enabled.",
            flush=True
        )

        print(
            "[OTCHARTS] One live stream at a time.",
            flush=True
        )

        return True

    except Exception as e:

        print(
            "[OTCHARTS ERROR]",
            e,
            flush=True
        )

        return False


# ==========================================
# STARTUP
# ==========================================

@app.on_event("startup")
async def startup():

    loop = asyncio.get_running_loop()

    connected = await loop.run_in_executor(
        None,
        connect_otcharts
    )

    if not connected:

        print(
            "[STARTUP] OTCharts connection failed.",
            flush=True
        )

        return

    await loop.run_in_executor(
        None,
        lambda: switch_otc_stream(
            active_symbol
        )
    )

    print()
    print("=================================")
    print("KURO AI OTC BACKEND READY")
    print("=================================")

    print(
        f"ACTIVE: {active_symbol}"
    )

    print("SUPPORTED:")

    for symbol in sorted(OTC_SYMBOLS):

        print(
            f"  - {symbol}"
        )

    print("=================================")
    print()
