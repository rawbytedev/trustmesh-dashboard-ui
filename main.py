import asyncio, json
import os
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
from storage_reader import Storage
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(broadcaster())
    yield
app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

storage = Storage()
clients = set()

@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    # Initial server-side render
    latest = storage.get_latest_all(limit=50)
    return templates.TemplateResponse("index.html", {"request": request, "latest": latest})

@app.get("/escrow/{escrow_id}", response_class=HTMLResponse)
def escrow_detail(request: Request, escrow_id: int):
    timeline = storage.get_escrow_by_id(escrow_id)
    return templates.TemplateResponse("escrow.html", {"request": request, "escrow_id": escrow_id, "timeline": timeline})

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        while True:
            await asyncio.sleep(10)  # keep alive
    except Exception:
        clients.discard(ws)

async def broadcaster():
    while True:
        latest = storage.get_latest_all(limit=50)
        payload = json.dumps(latest)
        for ws in list(clients):
            try:
                await ws.send_text(payload)
            except Exception:
                clients.discard(ws)
        await asyncio.sleep(2)

def start_server():
    """Starts the server"""
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

if __name__ == "__main__":
    start_server()