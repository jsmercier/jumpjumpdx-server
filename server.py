#!/usr/bin/env python3
"""
Jump Jump DX — multiplayer relay server.

Real-time co-play for the HTML5 game. Players join a room by code; the host picks the
mode and starts; the server hands everyone a SHARED random seed (so every client builds
the identical tower) and then relays each player's position so everyone sees the others
as live "shadows". Each client runs its own physics for its own player (responsive) and
detects its own win; the server only arbitrates the shared seed + the winner/standings.

Run:  py server.py
Then in the game, connect to  ws://<this-PC-LAN-IP>:8781

For remote friends later, host this file on any Python-capable box (Render/Railway/Fly
free tier, a VPS, etc.) and point the game at that URL.
"""
import asyncio, json, os, random, string, sys
import websockets

PORT = int(os.environ.get("PORT", 8781))   # cloud hosts inject $PORT; default for local dev
ROOMS = {}                      # code -> room
COLORS = ["#ff7a7a", "#5fb4ff", "#7fe0a0", "#ffd34d", "#c89aff", "#ff9ed1"]
CODE_CHARS = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"   # no ambiguous chars


def new_code():
    while True:
        c = "".join(random.choice(CODE_CHARS) for _ in range(4))
        if c not in ROOMS:
            return c


def player_list(room):
    return [{"id": pid, "name": p["name"], "color": p["color"], "host": pid == room["host"]}
            for pid, p in room["players"].items()]


async def send(ws, obj):
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass


async def broadcast(room, obj, exclude=None):
    for p in list(room["players"].values()):
        if p["ws"] is not exclude:
            await send(p["ws"], obj)


async def resolve_race(room):
    # brief grace so a lower-latency-but-earlier-frame finish can still count, then rank by game FRAME
    await asyncio.sleep(0.4)
    room["started"] = False
    st = sorted(room["finishers"], key=lambda f: f["frame"])   # earliest frame wins, not first packet to arrive
    await broadcast(room, {"t": "result", "winner": (st[0]["id"] if st else None),
                           "standings": st, "players": player_list(room)})
    room["resolving"] = False


async def handler(ws):
    pid = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8))
    room = None
    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                continue
            t = msg.get("t")

            if t == "create":
                code = new_code()
                room = {"code": code, "host": pid, "players": {}, "mode": "race",
                        "target": 100, "started": False, "finishers": []}
                ROOMS[code] = room
                room["players"][pid] = {"ws": ws, "name": (msg.get("name") or "Player")[:14], "color": COLORS[0]}
                await send(ws, {"t": "room", "code": code, "you": pid, "host": True,
                                "mode": room["mode"], "target": room["target"], "players": player_list(room)})

            elif t == "join":
                code = (msg.get("code") or "").upper()
                r = ROOMS.get(code)
                if not r:
                    await send(ws, {"t": "error", "msg": "Room not found"}); continue
                if r["started"]:
                    await send(ws, {"t": "error", "msg": "Match already started"}); continue
                if len(r["players"]) >= 6:
                    await send(ws, {"t": "error", "msg": "Room is full"}); continue
                room = r
                color = COLORS[len(room["players"]) % len(COLORS)]
                room["players"][pid] = {"ws": ws, "name": (msg.get("name") or "Player")[:14], "color": color}
                await send(ws, {"t": "room", "code": code, "you": pid, "host": False,
                                "mode": room["mode"], "target": room["target"], "players": player_list(room)})
                await broadcast(room, {"t": "players", "players": player_list(room)}, exclude=ws)

            elif room is None:
                continue   # all messages below require being in a room

            elif t == "setmode" and pid == room["host"]:
                room["mode"] = "time" if msg.get("mode") == "time" else "race"
                try:
                    room["target"] = max(25, min(500, int(msg.get("target") or 100)))
                except Exception:
                    pass
                await broadcast(room, {"t": "mode", "mode": room["mode"], "target": room["target"]})

            elif t == "start" and pid == room["host"]:
                room["started"] = True
                room["finishers"] = []
                room["resolving"] = False
                room["seed"] = random.randint(0, 2**31 - 1)
                await broadcast(room, {"t": "start", "seed": room["seed"], "mode": room["mode"],
                                       "target": room["target"], "players": player_list(room)})

            elif t == "state":   # relay my position to the rest of the room
                await broadcast(room, {"t": "peer", "id": pid,
                                       "x": msg.get("x"), "y": msg.get("y"), "floor": msg.get("floor"),
                                       "face": msg.get("face"), "score": msg.get("score"),
                                       "alive": msg.get("alive", True)}, exclude=ws)

            elif t == "finish":
                if not any(f["id"] == pid for f in room["finishers"]):
                    room["finishers"].append({"id": pid, "name": room["players"][pid]["name"],
                                              "color": room["players"][pid]["color"],
                                              "floor": msg.get("floor", 0), "score": msg.get("score", 0),
                                              "frame": msg.get("frame", 0)})
                if room["mode"] == "race":     # collect near-simultaneous finishes ~0.4s, then rank by frame (fair photo-finish)
                    if not room.get("resolving"):
                        room["resolving"] = True
                        asyncio.create_task(resolve_race(room))
                elif len(room["finishers"]) >= len(room["players"]):   # score rush: everyone reported
                    room["started"] = False
                    await broadcast(room, {"t": "result",
                                           "standings": sorted(room["finishers"], key=lambda f: -f["score"]),
                                           "players": player_list(room)})

            elif t == "leave":
                break
    except Exception:
        pass
    finally:
        if room and pid in room["players"]:
            del room["players"][pid]
            if not room["players"]:
                ROOMS.pop(room["code"], None)
            else:
                if room["host"] == pid:
                    room["host"] = next(iter(room["players"]))   # promote a new host
                await broadcast(room, {"t": "players", "players": player_list(room)})


def health_check(connection, request):
    # Cloud platforms probe the port with a plain HTTP GET; answer 200 so the deploy is marked healthy.
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(200, "Jump Jump DX server OK\n")
    return None


async def main():
    async with websockets.serve(handler, "0.0.0.0", PORT, ping_interval=20,
                                process_request=health_check):
        print(f"Jump Jump DX multiplayer server listening on ws://0.0.0.0:{PORT}")
        sys.stdout.flush()
        await asyncio.Future()   # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
