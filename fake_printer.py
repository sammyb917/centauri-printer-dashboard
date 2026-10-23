#!/usr/bin/env python3
"""Fake SDCP printer for testing printer-dashboard.py (test-only).
Implements the SOURCE-VERIFIED protocol (from danielcherubini/elegoo-homeassistant):
UDP discovery on 3000 (Attributes shape), WS server on 3030 (hand-rolled),
status pushes (CurrentStatus as list, PrintSpeedPct), history 320->321,
canvas 324 (snake_case tray_list), video 386 needs {Enable:1},
controls: pause 129, STOP 130, CONTINUE 131, light via 403.
Records every command to /tmp/fake-received.json."""
import base64, hashlib, json, os, socket, struct, threading, time

MBID = "FAKE1234567890"
CONN_ID = "0a1b2c3d4e"  # discovery connection Id — deliberately != MBID
STRICT = os.environ.get("FAKE_STRICT") == "1"
unlocked = threading.Event()
if not STRICT:
    unlocked.set()
received = []

STATUS = {
    "CurrentStatus": [1],
    "TempOfNozzle": 210.4, "TempTargetNozzle": 210,
    "TempOfHotbed": 59.8, "TempTargetHotbed": 60,
    "TempOfBox": 33.0,
    "CurrenCoord": "120.5,98.2,8.40",
    "CurrentFanSpeed": {"ModelFan": 100, "AuxiliaryFan": 40, "BoxFan": 20},
    "LightStatus": {"SecondLight": 1},
    "PrintInfo": {
        "Status": 13, "Filename": "benchy.gcode", "TaskId": "T1",
        "CurrentLayer": 42, "TotalLayer": 180,
        "CurrentTicks": 600000, "TotalTicks": 3600000, "Progress": 17,
        "PrintSpeedPct": 85,
    },
}

CANVAS = {
    "Ack": 0,
    "canvas_list": [{
        "canvas_id": 0, "connected": 1,
        "tray_list": [
            {"tray_id": 0, "brand": "ELEGOO", "filament_type": "PLA",
             "filament_name": "PLA Marble", "filament_color": "#FFFFFF", "status": 0},
            {"tray_id": 1, "brand": "Generic", "filament_type": "PLA",
             "filament_name": "PLA Basic", "filament_color": "#000000", "status": 1},
            {"tray_id": 2, "brand": "ELEGOO", "filament_type": "PLA",
             "filament_name": "PLA Marble", "filament_color": "#FFFFFF", "status": 2},
            {"tray_id": 3, "brand": "ELEGOO", "filament_type": "PLA",
             "filament_name": "PLA Basic", "filament_color": "#09CC3A", "status": 1},
        ],
    }],
    "active_canvas_id": 0, "active_tray_id": 2, "auto_refill": 1,
}

HISTORY = [
    {"TaskId": "H1", "TaskName": "poop-chute.gcode", "TaskStatus": 1,
     "BeginTime": 1761000000, "EndTime": 1761003600, "Thumbnail": "/thumb_h1.jpg"},
    {"TaskId": "H2", "TaskName": "knob-test.gcode", "TaskStatus": 8,
     "BeginTime": 1760900000, "EndTime": 1760901200, "Thumbnail": "/thumb_h2.jpg"},
]

# V3 dialect — copied field-for-field from a wire capture of a real
# Centauri Carbon (firmware V1.4.46, ProtocolVersion V3.0.0): top-level
# "Status"/"Attributes" keys (no Data wrapper), ticks in SECONDS.
STATUS3 = {
    "CurrentStatus": [1], "TimeLapseStatus": 0, "PlatFormType": 0,
    "AmsConnectStatus": 1,
    "TempOfHotbed": 59.93, "TempOfNozzle": 219.99, "TempOfBox": 27.18,
    "TempTargetHotbed": 60, "TempTargetNozzle": 220, "TempTargetBox": 0,
    "CurrenCoord": "74.79,41.78,21.29",
    "CurrentFanSpeed": {"ModelFan": 80, "AuxiliaryFan": 0, "BoxFan": 68},
    "ZOffset": 0.0, "LightStatus": {"SecondLight": 1, "RgbLight": [0, 0, 0]},
    "PrintInfo": {
        "Status": 13, "CurrentLayer": 255, "TotalLayer": 973,
        "CurrentTicks": 27838.84, "TotalTicks": 53012,
        "Filename": "large_model_0.12_14h44m.gcode",
        "TaskId": "13f5fe64-2c37-4f31-a439-f0487d2940ab",
        "PrintSpeedPct": 100, "Progress": 58,
    },
}
ATTRS3 = {
    "Name": "Centauri Carbon", "MachineName": "Centauri Carbon",
    "BrandName": "ELEGOO", "ProtocolVersion": "V3.0.0",
    "FirmwareVersion": "V1.4.46", "MainboardIP": "127.0.0.1",
    "MainboardID": MBID,
}
V3 = os.environ.get("FAKE_V3") == "1"


def udp_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", 3000))
    while True:
        data, addr = s.recvfrom(4096)
        if data == b"M99999":
            reply = {"Id": CONN_ID if STRICT else MBID, "Data": {"Attributes": {
                     "MainboardID": MBID, "MachineName": "Centauri Carbon",
                     "MainboardIP": "127.0.0.1", "Name": "Centauri Carbon"}}}
            s.sendto(json.dumps(reply).encode(), addr)


def ws_send(conn, text):
    payload = text.encode()
    h = bytearray([0x81])
    if len(payload) < 126: h.append(len(payload))
    elif len(payload) < 65536: h.append(126); h += struct.pack(">H", len(payload))
    else: h.append(127); h += struct.pack(">Q", len(payload))
    conn.sendall(bytes(h) + payload)


def ws_recv(conn, buf):
    def read(n, buf):
        while len(buf) < n:
            chunk = conn.recv(65536)
            if not chunk: raise ConnectionError("closed")
            buf += chunk
        return buf[:n], buf[n:]
    h, buf = read(2, buf)
    ln = h[1] & 0x7F
    if ln == 126: x, buf = read(2, buf); ln = struct.unpack(">H", x)[0]
    elif ln == 127: x, buf = read(8, buf); ln = struct.unpack(">Q", x)[0]
    mask, buf = read(4, buf)
    payload, buf = read(ln, buf)
    payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    return h[0] & 0x0F, payload, buf


def handle(conn):
    req = b""
    while b"\r\n\r\n" not in req:
        req += conn.recv(4096)
    key = [l for l in req.decode("latin1").split("\r\n")
           if l.lower().startswith("sec-websocket-key")][0].split(":")[1].strip()
    accept = base64.b64encode(hashlib.sha1(
        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
    conn.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                  f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").encode())
    buf = req.split(b"\r\n\r\n", 1)[1]
    stop = threading.Event()

    def pusher():
        if os.environ.get("FAKE_NOPUSH") == "1":
            return  # poll-only printer: never sends anything unasked
        while not stop.is_set():
            if not unlocked.wait(timeout=0.2):
                continue
            try:
                if V3:
                    ws_send(conn, json.dumps({"Status": STATUS3, "MainboardID": MBID,
                            "TimeStamp": int(time.time()),
                            "Topic": f"sdcp/status/{MBID}"}))
                elif os.environ.get("FAKE_FLAT") == "1":
                    # flat variant: Data IS the status (no Cmd/Data nesting)
                    ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/status/{MBID}",
                                              "Data": STATUS}))
                else:
                    ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/status/{MBID}",
                                              "Data": {"Cmd": 0, "Data": STATUS}}))
            except Exception:
                return
            time.sleep(1.0)
    threading.Thread(target=pusher, daemon=True).start()
    try:
        while True:
            op, payload, buf = ws_recv(conn, buf)
            if op == 8: break
            if op != 1: continue
            msg = json.loads(payload)
            cmd = msg["Data"]["Cmd"]
            cdata = msg["Data"].get("Data") or {}
            valid = (msg.get("Id") == CONN_ID
                     and msg["Data"].get("MainboardID") == MBID)
            received.append([cmd, cdata, valid])
            with open("/tmp/fake-received.json", "w") as f:
                json.dump(received, f)
            if STRICT and not valid:
                continue  # the real printer silently ignores bad-ID requests
            if cmd == 0:
                unlocked.set()
                if V3:
                    # V3 answers a status request with a bare ack; the data
                    # itself only ever arrives as pushes.
                    ws_send(conn, json.dumps({"Id": CONN_ID, "Data": {
                        "Cmd": 0, "Data": {"Ack": 0},
                        "RequestID": msg["Data"].get("RequestID", ""),
                        "MainboardID": MBID, "TimeStamp": int(time.time())},
                        "Topic": f"sdcp/response/{MBID}"}))
                if os.environ.get("FAKE_NOPUSH") == "1":
                    # poll-model printer: status comes back as a reply
                    ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/response/{MBID}",
                            "Data": {"Cmd": 0, "Data": STATUS}}))
            if cmd == 1:
                if V3:
                    ws_send(conn, json.dumps({"Attributes": ATTRS3,
                            "MainboardID": MBID, "TimeStamp": int(time.time()),
                            "Topic": f"sdcp/attributes/{MBID}"}))
                else:
                    ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/attributes/{MBID}",
                        "Data": {"Cmd": 1, "Data": {"MachineName": "Centauri Carbon",
                        "FirmwareVersion": "1.2.3"}}}))
            elif cmd == 386:
                if cdata.get("Enable") == 1:
                    ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/response/{MBID}",
                            "Data": {"Cmd": 386, "Data": {"Ack": 0,
                            "VideoUrl": "http://127.0.0.1:3031/video"}}}))
            elif cmd == 320:
                ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/response/{MBID}",
                        "Data": {"Cmd": 320, "Data": {"TaskList":
                        [{"Id": "H1"}, {"Id": "H2"}]}}}))
            elif cmd == 321:
                want = (cdata.get("Id") or [""])[0]
                det = [h for h in HISTORY if h["TaskId"] == want]
                if det:
                    ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/response/{MBID}",
                            "Data": {"Cmd": 321, "Data": det[0]}}))
            elif cmd == 324:
                ws_send(conn, json.dumps({"Id": MBID, "Topic": f"sdcp/response/{MBID}",
                        "Data": {"Cmd": 324, "Data": CANVAS}}))
            elif cmd == 403 and V3 and "PrintSpeedPct" in cdata:
                STATUS3["PrintInfo"]["PrintSpeedPct"] = cdata["PrintSpeedPct"]
            elif cmd == 403 and V3 and "CurrentFanSpeed" in cdata:
                STATUS3["CurrentFanSpeed"].update(cdata["CurrentFanSpeed"])
    except Exception:
        pass
    stop.set()
    conn.close()


def main():
    threading.Thread(target=udp_server, daemon=True).start()
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 3030))
    srv.listen(5)
    print("fake printer up", flush=True)
    while True:
        c, _ = srv.accept()
        threading.Thread(target=handle, args=(c,), daemon=True).start()


if __name__ == "__main__":
    main()
