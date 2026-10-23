#!/usr/bin/env python3
"""
Printer Dashboard — standalone monitor for an Elegoo Centauri Carbon
(+ CANVAS 4-color unit), speaking SDCP V3.

Talks to the printer over the LAN using Elegoo's SDCP protocol
(WebSocket on port 3030, UDP discovery on port 3000, camera MJPEG on 3031).
Pure Python standard library — no pip installs.

Run:  python printer-dashboard.py     (serves http://<this-box>:8091/)
Config (OPTIONAL, next to this file): printer-dashboard-config.json
    { "printer_ip": "192.168.1.50", "port": 8091 }
Out of the box the printer is found by LAN auto-discovery. A saved or
configured IP is tried first; if the printer stops answering there, the
dashboard falls back to scanning and adopts the address that answers.
If the scan finds nothing, the dashboard page itself asks for the IP
and saves it to the config (merging, never clobbering).

Build tag: printerdash-v11
"""

import base64
import hashlib
import http.client
import ipaddress
import json
import os
import socket
import struct
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs, quote

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "printer-dashboard-config.json")
BUILD_TAG = "printerdash-v11"

# ---------------------------------------------------------------- SDCP ------
# Command IDs per Elegoo's SDCP v3 spec. (Verified against the protocol docs
# and the open-source SDCP client libraries before shipping v1.)
CMD_STATUS      = 0     # machine status (also pushed ~1/sec by the printer)
CMD_ATTRIBUTES  = 1     # machine attributes (name, firmware, capabilities)
CMD_START_PRINT = 128
CMD_PAUSE_PRINT = 129
CMD_STOP_PRINT  = 130   # NOTE: stop is 130, continue is 131 (SDCP source)
CMD_CONT_PRINT  = 131
CMD_VIDEO       = 386   # request video stream -> response carries VideoUrl
CMD_HISTORY     = 320   # historical task list (ids only)
CMD_HISTORY_ONE = 321   # one historical task's details
CMD_CANVAS      = 324   # CANVAS / AMS status (canvas_list -> tray_list)
CMD_CONTROL     = 403   # device control: light, fans, temps, speed

SDCP_WS_PORT    = 3030
SDCP_UDP_PORT   = 3000
CAMERA_PORT     = 3031  # MJPEG at http://<printer>:3031/video

DEFAULTS = {"printer_ip": "", "port": 8091}


def load_config():
    cfg = dict(DEFAULTS)
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            saved = json.load(f)
        for k in DEFAULTS:
            if k in saved:
                cfg[k] = saved[k]
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[config] could not read {CONFIG_PATH}: {e} (using defaults)")
    return cfg


CONFIG = load_config()

# ---------------------------------------------------------------- state -----
_lock = threading.Lock()
_state = {
    "online": False,
    "last_seen": 0.0,
    "printer_ip": CONFIG.get("printer_ip") or "",
    "mainboard_id": "",
    "connection_id": "",  # top-level Id from the discovery reply
    "machine_name": "",
    "attributes": {},     # raw SDCP attributes payload
    "status": {},         # raw SDCP status payload (latest push)
    "video_url": "",      # from CMD_VIDEO or the :3031 default
    "history_map": {},    # task_id -> detail dict (from CMD_HISTORY_ONE)
    "history_order": [],  # task_ids, newest first (from CMD_HISTORY)
    "canvas_raw": None,   # raw CMD_CANVAS response payload
    "wire": [],           # last raw messages from the printer (diagnostics)
    "fail_count": 0,      # consecutive failed connection attempts
    "error": "",
}
_cmd_queue = []           # (cmd, data_dict) waiting for the SDCP session
_cmd_lock = threading.Lock()
_wake = threading.Event()  # set when settings change -> session retries now


def save_printer_ip(ip):
    """Persist a user-entered printer IP into the app config, MERGING with
    whatever is already there. Only ever called from the page's IP form —
    the app never creates or rewrites the config on its own."""
    cfg = {}
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not isinstance(cfg, dict):
            cfg = {}
    except Exception:
        cfg = {}
    cfg["printer_ip"] = ip
    tmp = CONFIG_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, CONFIG_PATH)


def set_state(**kw):
    with _lock:
        _state.update(kw)


def get_state():
    with _lock:
        return dict(_state)


def queue_command(cmd, data=None):
    with _cmd_lock:
        _cmd_queue.append((cmd, data or {}))


def drain_commands():
    with _cmd_lock:
        out = list(_cmd_queue)
        _cmd_queue.clear()
        return out


# ------------------------------------------------------- minimal WS client --
class WSClient:
    """Just enough WebSocket (RFC 6455) for SDCP: text frames in/out,
    fragmentation reassembly, ping->pong. Client frames are masked."""

    def __init__(self, host, port, path="/websocket", timeout=5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.buf = b""
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.sock.sendall(req.encode())
        resp = self._read_until(b"\r\n\r\n")
        head, _, rest = resp.partition(b"\r\n\r\n")
        if b" 101 " not in head.split(b"\r\n", 1)[0]:
            raise ConnectionError(f"WS handshake failed: {head.splitlines()[0]!r}")
        accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
        ).decode()
        if accept.encode() not in head:
            raise ConnectionError("WS handshake: bad Sec-WebSocket-Accept")
        self.buf = rest

    def _read_until(self, marker):
        while marker not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("socket closed during handshake")
            self.buf += chunk
        idx = self.buf.index(marker) + len(marker)
        out, self.buf = self.buf[:idx], self.buf[idx:]
        return out

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("socket closed")
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def send_text(self, text):
        payload = text.encode("utf-8")
        mask = os.urandom(4)
        header = bytearray([0x81])
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def _send_frame(self, opcode, payload=b""):
        mask = os.urandom(4)
        header = bytearray([0x80 | opcode])
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def recv_message(self, timeout=2.0):
        """Return one complete text message, or None on timeout.
        Raises ConnectionError when the socket dies."""
        self.sock.settimeout(timeout)
        frag = bytearray()
        frag_op = None
        while True:
            try:
                h = self._read_exact(2)
            except socket.timeout:
                return None
            fin = h[0] & 0x80
            op = h[0] & 0x0F
            masked = h[1] & 0x80
            ln = h[1] & 0x7F
            if ln == 126:
                ln = struct.unpack(">H", self._read_exact(2))[0]
            elif ln == 127:
                ln = struct.unpack(">Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else None
            payload = self._read_exact(ln) if ln else b""
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
            if op == 0x9:      # ping
                self._send_frame(0xA, payload)
                continue
            if op == 0x8:      # close
                raise ConnectionError("server closed the websocket")
            if op in (0x1, 0x2, 0x0):
                if frag_op is None:
                    frag_op = op
                frag += payload
                if fin:
                    data = bytes(frag)
                    if frag_op == 0x1:
                        return data.decode("utf-8", "replace")
                    frag = bytearray()
                    frag_op = None
                continue
            # other opcodes (pong etc.) are ignored

    def close(self):
        try:
            self._send_frame(0x8)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass


# ------------------------------------------------------------- discovery ----
def discover_printer(timeout=2.0):
    """SDCP discovery: broadcast 'M99999' on UDP 3000; the printer answers
    with its identity (MainboardID, MachineName, IpAddress...)."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.settimeout(timeout)
        s.sendto(b"M99999", ("255.255.255.255", SDCP_UDP_PORT))
        data, addr = s.recvfrom(65536)
        s.close()
        info = json.loads(data.decode("utf-8", "replace"))
        d = info.get("Data", info)
        attrs = d.get("Attributes") or d
        ip = (attrs.get("MainboardIP") or d.get("IpAddress")
              or d.get("IP") or addr[0])
        return {
            "ip": ip,
            "mainboard_id": attrs.get("MainboardID") or d.get("MainboardID", ""),
            "connection_id": info.get("Id") or "",
            "machine_name": attrs.get("MachineName") or attrs.get("Name", ""),
            "raw": d,
        }
    except Exception:
        return None


# ---------------------------------------------------------- SDCP session ----
def _sdcp_envelope(connection_id, mainboard_id, cmd, data):
    # "Id" is the CONNECTION id handed out by discovery — the printer ignores
    # requests whose Id/MainboardID don't match what it issued.
    return json.dumps({
        "Id": connection_id or mainboard_id or "",
        "Data": {
            "Cmd": cmd,
            "Data": data or {},
            "RequestID": uuid.uuid4().hex[:16],
            "MainboardID": mainboard_id or "",
            "TimeStamp": int(time.time()),
            "From": 0,  # matches the reference SDCP client
        },
        "Topic": f"sdcp/request/{mainboard_id or ''}",
    })


def _looks_like_status(d):
    return isinstance(d, dict) and any(
        k in d for k in ("TempOfNozzle", "TempOfHotbed", "PrintInfo",
                         "CurrentStatus"))


def _handle_message(msg):
    """Route one SDCP message from the printer into shared state."""
    try:
        obj = json.loads(msg)
    except Exception:
        return
    topic = obj.get("Topic", "")
    data = obj.get("Data") or {}
    inner = data.get("Data") if isinstance(data, dict) else None
    # Wire tap: keep the last raw messages so /api/raw can show exactly what
    # dialect this firmware speaks (topics + payload shapes).
    stw = get_state()
    wire = (stw["wire"] + [{"topic": topic, "text": str(msg)[:900]}])[-12:]
    if wire != stw["wire"]:
        set_state(wire=wire)
    # Learn the mainboard ID from any message that carries it — this makes a
    # config-only IP work even where UDP broadcast discovery is blocked.
    if not get_state()["mainboard_id"]:
        for cand in (obj.get("Id"), obj.get("MainboardID"),
                     data.get("MainboardID") if isinstance(data, dict) else None,
                     inner.get("MainboardID") if isinstance(inner, dict) else None):
            if cand:
                set_state(mainboard_id=str(cand))
                break
    if topic.startswith("sdcp/status/"):
        # V3 firmware (protocol V3.0.0): the status rides in
        # a TOP-LEVEL "Status" key — no Data wrapper at all. Older dialect:
        # Data.Data. Flat variant: Data itself. Try them in that order.
        payload = obj.get("Status") if isinstance(obj.get("Status"), dict) else None
        if payload is None and isinstance(inner, dict):
            payload = inner
        if payload is None and _looks_like_status(data):
            payload = data
        if payload is not None:
            set_state(status=payload, online=True, last_seen=time.time(), error="")
    elif topic.startswith("sdcp/attributes/"):
        payload = obj.get("Attributes") if isinstance(obj.get("Attributes"), dict) else None
        if payload is None and isinstance(inner, dict):
            payload = inner
        if payload is not None:
            set_state(attributes=payload)
    elif topic.startswith("sdcp/response/"):
        cmd = data.get("Cmd")
        if cmd == CMD_STATUS:
            payload = inner if _looks_like_status(inner) else None
            if payload is None and _looks_like_status(data):
                payload = data
            if payload is not None:
                # This printer ANSWERS status polls instead of streaming
                # pushes — the reply lands here, not on the status topic.
                set_state(status=payload, online=True,
                          last_seen=time.time(), error="")
                return
        if not isinstance(inner, dict):
            return
        if cmd == CMD_ATTRIBUTES and inner:
            set_state(attributes=inner)
        elif cmd == CMD_VIDEO:
            if inner.get("Ack", 0) in (0, None):
                url = inner.get("VideoUrl") or inner.get("Url") or ""
                if url:
                    set_state(video_url=url)
        elif cmd == CMD_CANVAS:
            if inner.get("Ack", 0) in (0, None) or "canvas_list" in inner \
                    or "CanvasList" in inner:
                set_state(canvas_raw=inner)
        elif cmd == CMD_HISTORY:
            ids = []
            for t in (inner.get("TaskList") or []):
                if isinstance(t, dict) and t.get("Id") is not None:
                    ids.append(str(t["Id"]))
                elif t is not None:
                    ids.append(str(t))
            if ids:
                st = get_state()
                set_state(history_order=ids[:20])
                for tid in ids[:20]:
                    if tid not in st["history_map"]:
                        queue_command(CMD_HISTORY_ONE, {"Id": [tid]})
        elif cmd == CMD_HISTORY_ONE:
            tid = str(inner.get("TaskId") or inner.get("TaskID") or "")
            if tid:
                st = get_state()
                m = dict(st["history_map"])
                m[tid] = inner
                set_state(history_map=m)
    # error/notice topics are ignored in v1 beyond the status feed


def sdcp_session():
    """Owns the printer connection forever: discover -> connect -> pump
    status pushes, send queued commands, refresh history; reconnect on drop."""
    backoff = 2
    last_history = 0.0
    while True:
        try:
            st = get_state()
            ip = st["printer_ip"] or CONFIG.get("printer_ip") or ""
            mbid = st["mainboard_id"]
            conn = st["connection_id"]
            # Discovery hands out the two IDs the printer demands in every
            # request (connection Id + MainboardID). Run it when we have no
            # IP, when either ID is still unknown, or after repeated
            # failures (startup rule: saved IP first, then scan).
            if not ip or not mbid or not conn or st["fail_count"] >= 3:
                if ip and st["fail_count"] >= 3:
                    set_state(online=False,
                              error=f"Printer not answering at {ip} — "
                                    "scanning the network for it…")
                found = discover_printer()
                if found:
                    if found["ip"] != ip:
                        # The printer itself answered — from a new address,
                        # or the first address we ever got. Trust it.
                        if ip and st["fail_count"] >= 3:
                            try:
                                save_printer_ip(found["ip"])
                            except Exception:
                                pass
                        ip = found["ip"]
                        set_state(printer_ip=ip, fail_count=0)
                    mbid = found["mainboard_id"] or mbid
                    conn = found["connection_id"] or conn
                    set_state(mainboard_id=mbid, connection_id=conn,
                              machine_name=found["machine_name"]
                              or st["machine_name"])
            if not ip:
                set_state(online=False, error="Printer not found on the LAN yet "
                                             "(discovery found nothing).")
                _wake.wait(timeout=5)
                _wake.clear()
                continue
            if not ip:
                set_state(online=False, error="Printer not found on the LAN yet "
                                             "(discovery found nothing).")
                _wake.wait(timeout=5)
                _wake.clear()
                continue
            ws = WSClient(ip, SDCP_WS_PORT, "/websocket", timeout=6.0)
            try:
                set_state(online=True, error="", fail_count=0)
                backoff = 2
                connected_at = time.time()
                burst_ids = None
                last_poll = 0.0
                while True:
                    if get_state()["printer_ip"] and get_state()["printer_ip"] != ip:
                        raise ConnectionError("printer IP changed in settings")
                    st = get_state()
                    ids = (st["connection_id"], st["mainboard_id"])
                    if ids != burst_ids:
                        # Ask for everything as soon as we connect, and ask
                        # AGAIN if the real IDs get learned mid-connection —
                        # the printer ignores requests that lack its IDs, so
                        # the first burst may have been a shout into the void.
                        ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_ATTRIBUTES, {}))
                        ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_STATUS, {}))
                        ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_VIDEO, {"Enable": 1}))
                        ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_HISTORY, {}))
                        ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_CANVAS, {}))
                        last_history = time.time()
                        burst_ids = ids
                    if ids[1]:  # controls only make sense with the mainboard ID
                        for cmd, data in drain_commands():
                            ws.send_text(_sdcp_envelope(ids[0], ids[1], cmd, data))
                        if time.time() - last_history > 60:
                            ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_HISTORY, {}))
                            ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_CANVAS, {}))
                            last_history = time.time()
                    if burst_ids is not None and time.time() - last_poll > 30:
                        # The V3 firmware streams pushes on its own; this
                        # slow poll only matters for firmwares that answer
                        # Cmd 0 instead of pushing (harmless either way —
                        # a push-streaming printer just acks it).
                        ws.send_text(_sdcp_envelope(ids[0], ids[1], CMD_STATUS, {}))
                        last_poll = time.time()
                    msg = ws.recv_message(timeout=2.0)
                    if msg:
                        _handle_message(msg)
                    st = get_state()
                    if st["last_seen"] and time.time() - st["last_seen"] > 15:
                        raise ConnectionError("status feed went quiet")
                    if not st["last_seen"] and time.time() - connected_at > 20:
                        raise ConnectionError("connected, but the printer "
                                              "never sent any status")
            finally:
                ws.close()
        except Exception as e:
            set_state(online=False, error=f"Printer connection: {e}",
                      fail_count=get_state()["fail_count"] + 1)
            drain_commands()  # never fire stale commands after a reconnect gap
            _wake.wait(timeout=backoff)
            _wake.clear()
            backoff = min(backoff * 2, 30)


# ------------------------------------------------------------ normalize -----
MACHINE_STATES = {
    0: "Idle", 1: "Printing", 2: "File transfer", 3: "Exposure test",
    4: "Device test", 5: "Leveling", 6: "Input shaping", 7: "Stopping",
    8: "Stopped", 9: "Homing", 10: "Loading/unloading", 11: "PID tuning",
    12: "Recovery",
}
PRINT_STATES = {
    0: "Idle", 1: "Homing", 2: "Dropping", 3: "Exposing", 4: "Printing",
    5: "Pausing", 6: "Paused", 7: "Stopping", 8: "Stopped", 9: "Complete",
    10: "File checking",
    # FDM (Centauri) wire codes confirmed from the SDCP client source:
    13: "Printing", 14: "Error", 18: "Starting print",
}
HISTORY_STATES = {1: "Completed", 2: "Failed", 8: "Stopped"}


def _num(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _parse_canvas(raw):
    """Normalize a CMD_CANVAS (324) response: canvas_list -> tray_list.
    Keys are snake_case on the wire; PascalCase variants accepted too."""
    if not isinstance(raw, dict):
        return None
    units = raw.get("canvas_list") or raw.get("CanvasList") or []
    active_tray = raw.get("active_tray_id", raw.get("ActiveTrayId"))
    slots = []
    for unit in units:
        if not isinstance(unit, dict):
            continue
        trays = unit.get("tray_list") or unit.get("TrayList") or []
        for tray in trays:
            if not isinstance(tray, dict):
                continue
            low = {str(k).lower(): v for k, v in tray.items()}
            tid = low.get("tray_id", low.get("trayid"))
            color = str(low.get("filament_color") or low.get("color") or "")
            if color and not color.startswith("#"):
                color = "#" + color
            present = low.get("status")
            # Wire truth (captured from a real CANVAS): status 1 = loaded, 2 = in use AND
            # in use; the payload's active_tray_id names the in-use tray.
            slots.append({
                "slot": (int(tid) + 1) if isinstance(tid, (int, float)) else len(slots) + 1,
                "type": low.get("filament_name") or low.get("filament_type")
                        or low.get("type") or "",
                "material": low.get("filament_type") or "",
                "brand": low.get("brand") or "",
                "color": color,
                "loaded": present in (1, 2) if present is not None else None,
                "active": (active_tray is not None and tid is not None
                           and int(tid) == int(active_tray)) or present == 2,
                "raw": tray,
            })
    if not units and not slots:
        return None
    return {"found": True, "slots": slots,
            "active_canvas": raw.get("active_canvas_id"),
            "active_tray": raw.get("active_tray_id"),
            "extra_keys": sorted(raw.keys())}


def _find_canvas(status, attributes):
    """The CANVAS 4-color unit reports through the printer's status feed.
    Field naming varies by firmware, so hunt case-insensitively through the
    status + attributes payloads for anything canvas/filament/slot shaped
    and hand the page a clean list of slots."""
    hay = {}

    def walk(obj, prefix=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = f"{prefix}{k}".lower()
                if any(w in key for w in ("canvas", "filament", "material",
                                          "slot", "ams", "cfs", "extruderbox")):
                    hay[key] = v
                walk(v, prefix + k + ".")
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                walk(v, prefix + f"{i}.")

    walk(status)
    walk(attributes)
    slots = []
    raw = None
    for key, val in hay.items():
        if isinstance(val, list) and val and isinstance(val[0], dict):
            raw = val
            break
        if isinstance(val, dict) and any(isinstance(x, dict) for x in val.values()):
            raw = [val[k] for k in sorted(val.keys())]
            break
    if raw:
        for i, item in enumerate(raw):
            if not isinstance(item, dict):
                continue
            low = {str(k).lower(): v for k, v in item.items()}
            color = (low.get("color") or low.get("colour") or low.get("rgb")
                     or low.get("filamentcolor") or "")
            if isinstance(color, list) and len(color) == 3:
                color = "#%02x%02x%02x" % tuple(int(c) for c in color)
            slots.append({
                "slot": i + 1,
                "type": low.get("type") or low.get("filamenttype")
                        or low.get("material") or low.get("name") or "",
                "color": str(color),
                "loaded": low.get("loaded", low.get("state", low.get("status"))),
                "raw": item,
            })
    return {"found": bool(slots) or bool(hay), "slots": slots,
            "extra_keys": sorted(hay.keys())}


def normalize_status():
    st = get_state()
    s = st["status"]
    pi = s.get("PrintInfo") or {}
    pi_status = pi.get("Status")
    cs = s.get("CurrentStatus")
    if isinstance(cs, list):  # WebSocket status carries e.g. [1]
        cs = cs[0] if cs else None
    cur = _num(cs)
    machine_label = MACHINE_STATES.get(int(cur)) if cur is not None else None
    pi_label = PRINT_STATES.get(int(pi_status)) if pi_status is not None else None
    if machine_label == "Printing" or pi_status == 6:
        display_state = pi_label if pi_status == 6 else "Printing"
    else:
        display_state = pi_label or machine_label or ""
    speed = s.get("PrintSpeed")
    speed_unit = "mm/s"
    if speed is None:
        speed = pi.get("PrintSpeedPct")
        speed_unit = "%"
    fans = s.get("CurrentFanSpeed") or {}
    light = s.get("LightStatus") or {}
    cur_ticks = _num(pi.get("CurrentTicks")) or 0
    tot_ticks = _num(pi.get("TotalTicks")) or 0
    # V3 firmware counts ticks in SECONDS (a 14h44m print reports
    # TotalTicks 53012); resin-era firmware counts milliseconds. Anything
    # over ~55 hours in "seconds" must really be milliseconds.
    tick_factor = 1000.0 if tot_ticks > 200000 else 1.0
    remaining = max(tot_ticks - cur_ticks, 0) if tot_ticks else 0
    progress = _num(pi.get("Progress"))
    if progress is None and tot_ticks:
        progress = round(100.0 * cur_ticks / tot_ticks, 1)
    video = st["video_url"]
    if not video and st["printer_ip"]:
        video = f"http://{st['printer_ip']}:{CAMERA_PORT}/video"
    return {
        "online": st["online"],
        "needs_ip": not bool(st["printer_ip"]),
        "error": st["error"],
        "printer_ip": st["printer_ip"],
        "machine_name": (st["machine_name"]
                         or st["attributes"].get("MachineName") or "Centauri Carbon"),
        "firmware": st["attributes"].get("FirmwareVersion", ""),
        "machine_state": machine_label or "",
        "print_state": display_state,
        "filename": pi.get("Filename") or pi.get("FileName") or "",
        "task_id": pi.get("TaskId") or pi.get("TaskID") or "",
        "progress": progress,
        "current_layer": pi.get("CurrentLayer"),
        "total_layer": pi.get("TotalLayer"),
        "print_speed": speed,
        "speed_unit": speed_unit,
        "elapsed_s": int(cur_ticks / tick_factor) if cur_ticks else 0,
        "remaining_s": int(remaining / tick_factor) if remaining else 0,
        "total_s": int(tot_ticks / tick_factor) if tot_ticks else 0,
        "nozzle": _num(s.get("TempOfNozzle")),
        "nozzle_target": _num(s.get("TempTargetNozzle")),
        "bed": _num(s.get("TempOfHotbed")),
        "bed_target": _num(s.get("TempTargetHotbed")),
        "box": _num(s.get("TempOfBox")),
        "coords": s.get("CurrenCoord") or s.get("CurrentCoord") or "",
        "fans": {
            "model": fans.get("ModelFan"),
            "aux": fans.get("AuxiliaryFan"),
            "box": fans.get("BoxFan"),
        },
        "light_on": light.get("SecondLight"),
        "video_url": video,
        "canvas": _parse_canvas(st["canvas_raw"])
                  or _find_canvas(s, st["attributes"]),
        "build": BUILD_TAG,
    }


def normalize_history():
    st = get_state()
    out = []
    for tid in st["history_order"]:
        h = st["history_map"].get(tid)
        if not isinstance(h, dict):
            continue  # detail not fetched yet
        status = h.get("TaskStatus", h.get("Status"))
        begin = h.get("BeginTime") or h.get("StartTime") or 0
        end = h.get("EndTime") or 0
        thumb = (h.get("Thumbnail") or h.get("ThumbUrl") or h.get("Image") or "")
        out.append({
            "task_id": h.get("TaskId") or h.get("TaskID") or "",
            "filename": h.get("TaskName") or h.get("Filename") or h.get("Name") or "?",
            "status": HISTORY_STATES.get(status, str(status) if status is not None else ""),
            "begin": begin,
            "duration_s": int(end - begin) if (end and begin) else
                          int(h.get("PrintTime") or h.get("Time") or 0),
            "thumbnail": thumb,
        })
    return out


# Light control rides CMD_CONTROL (403) with a LightStatus payload —
# verified against the reference SDCP client (set_light_status).
CMD_LIGHT = CMD_CONTROL

# -------------------------------------------------------------- HTTP API ----
class Handler(BaseHTTPRequestHandler):
    server_version = "PrinterDash/1"
    protocol = "HTTP/1.1"

    def log_message(self, fmt, *args):  # keep the console quiet
        pass

    # -- helpers
    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _printer_http(self, url, timeout=10):
        """GET something from the printer itself (host pinned to the printer)."""
        st = get_state()
        ip = st["printer_ip"]
        if not ip:
            return None, None
        u = urlparse(url if url.startswith("http") else f"http://{ip}{url}")
        conn = http.client.HTTPConnection(ip, u.port or 80, timeout=timeout)
        path = u.path or "/"
        if u.query:
            path += "?" + u.query
        conn.request("GET", path, headers={"User-Agent": "PrinterDash/1"})
        resp = conn.getresponse()
        return resp, conn

    # -- routes
    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/":
            body = PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/status":
            self._json(normalize_status())
        elif path == "/api/history":
            self._json({"history": normalize_history()})
        elif path == "/api/raw":
            st = get_state()
            self._json({"status": st["status"], "attributes": st["attributes"],
                        "canvas_raw": st["canvas_raw"],
                        "history_order": st["history_order"],
                        "history_map": st["history_map"],
                        "wire": st["wire"]})
        elif path == "/camera":
            self._proxy_camera()
        elif path == "/thumb":
            self._proxy_thumb()
        else:
            self._json({"error": "not found"}, 404)

    def _proxy_camera(self):
        info = normalize_status()
        url = info["video_url"]
        if not url or not get_state()["online"]:
            self._json({"error": "camera unavailable"}, 503)
            return
        try:
            resp, conn = self._printer_http(url, timeout=15)
            if resp is None or resp.status != 200:
                self._json({"error": "camera unavailable"}, 503)
                return
            ctype = resp.getheader("Content-Type") or \
                "multipart/x-mixed-replace; boundary=frame"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            while True:
                chunk = resp.read(16384)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _proxy_thumb(self):
        qs = parse_qs(urlparse(self.path).query)
        u = (qs.get("u") or [""])[0]
        if not u:
            self._json({"error": "missing u"}, 400)
            return
        try:
            resp, conn = self._printer_http(u, timeout=10)
            if resp is None:
                self._json({"error": "no printer"}, 503)
                return
            data = resp.read()
            ctype = resp.getheader("Content-Type") or "image/jpeg"
            conn.close()
            self.send_response(resp.status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as e:
            self._json({"error": f"thumbnail failed: {e}"}, 502)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/set-ip":
            try:
                n = int(self.headers.get("Content-Length") or 0)
                req = json.loads(self.rfile.read(n) or b"{}")
                ip = str(req.get("ip") or "").strip()
                ipaddress.IPv4Address(ip)  # raises on anything that isn't an IPv4
            except Exception:
                self._json({"ok": False,
                            "message": "That doesn't look like an IP address "
                                       "(example: 192.168.1.50)"}, 400)
                return
            set_state(printer_ip=ip, mainboard_id="", connection_id="",
                      online=False, error="", fail_count=0)
            try:
                save_printer_ip(ip)
            except Exception as e:
                self._json({"ok": False, "message": f"could not save config: {e}"}, 500)
                return
            _wake.set()
            self._json({"ok": True,
                        "message": f"Saved. Connecting to the printer at {ip}…"})
            return
        if path != "/api/control":
            self._json({"error": "not found"}, 404)
            return
        try:
            n = int(self.headers.get("Content-Length") or 0)
            req = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            self._json({"ok": False, "message": "bad request body"}, 400)
            return
        if not get_state()["online"]:
            self._json({"ok": False, "message": "printer is offline"}, 503)
            return
        action = req.get("action")
        if action == "pause":
            queue_command(CMD_PAUSE_PRINT)
            self._json({"ok": True, "message": "Pause sent"})
        elif action == "resume":
            queue_command(CMD_CONT_PRINT)
            self._json({"ok": True, "message": "Resume sent"})
        elif action == "stop":
            if not req.get("confirm"):
                self._json({"ok": False, "message": "stop needs confirm"}, 400)
                return
            queue_command(CMD_STOP_PRINT)
            self._json({"ok": True, "message": "Stop sent"})
        elif action == "light":
            if CMD_LIGHT is None:
                self._json({"ok": False,
                            "message": "Light control isn't wired up in this build yet"})
                return
            info = normalize_status()
            on = 0 if info["light_on"] else 1
            queue_command(CMD_LIGHT, {"LightStatus": {"SecondLight": on}})
            self._json({"ok": True, "message": f"Light {'on' if on else 'off'} sent"})
        elif action == "speed":
            try:
                value = int(req.get("value"))
            except Exception:
                self._json({"ok": False, "message": "speed needs a % value"}, 400)
                return
            if not 10 <= value <= 300:
                self._json({"ok": False, "message": "speed must be 10–300%"}, 400)
                return
            # CMD 403 is the settings command (same one the light uses);
            # the speed value's own wire name is PrintSpeedPct.
            queue_command(CMD_LIGHT, {"PrintSpeedPct": value})
            self._json({"ok": True, "message": f"Speed {value}% sent — "
                        "watch the Speed readout to confirm the printer took it"})
        elif action == "fan":
            which = req.get("fan")
            keys = {"model": "ModelFan", "aux": "AuxiliaryFan", "box": "BoxFan"}
            if which not in keys:
                self._json({"ok": False, "message": "unknown fan"}, 400)
                return
            try:
                value = int(req.get("value"))
            except Exception:
                self._json({"ok": False, "message": "fan needs a % value"}, 400)
                return
            if not 0 <= value <= 100:
                self._json({"ok": False, "message": "fan must be 0–100%"}, 400)
                return
            # Send the complete CurrentFanSpeed bundle (mirrors the status
            # object, same pattern as LightStatus) with the one fan changed.
            cur = (get_state()["status"].get("CurrentFanSpeed") or {})
            bundle = {"ModelFan": cur.get("ModelFan", 0),
                      "AuxiliaryFan": cur.get("AuxiliaryFan", 0),
                      "BoxFan": cur.get("BoxFan", 0)}
            bundle[keys[which]] = value
            queue_command(CMD_LIGHT, {"CurrentFanSpeed": bundle})
            self._json({"ok": True, "message": f"{keys[which]} {value}% sent"})
        else:
            self._json({"ok": False, "message": f"unknown action {action!r}"}, 400)


def main():
    port = int(CONFIG.get("port") or 8091)
    threading.Thread(target=sdcp_session, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Printer Dashboard ({BUILD_TAG}) on http://0.0.0.0:{port}/  "
          f"— printer: {CONFIG.get('printer_ip') or 'auto-discovery'}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


# ------------------------------------------------------------------ page ----
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Printer Dashboard</title>
<style>
  :root{
    --bg:#0d1117; --tile:#161d29; --tile2:#1c2635; --line:#2a3648;
    --txt:#e8eef6; --dim:#8fa3ba; --accent:#4da3ff; --green:#3fb970;
    --amber:#e8a33d; --red:#e5534b;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);
       font-family:'Segoe UI',system-ui,Arial,sans-serif}
  header{display:flex;align-items:center;gap:12px;padding:14px 18px;
         border-bottom:1px solid var(--line);flex-wrap:wrap}
  header h1{font-size:20px;margin:0}
  .dot{width:11px;height:11px;border-radius:50%;background:var(--red)}
  .dot.on{background:var(--green)}
  .badge{padding:3px 10px;border-radius:20px;background:var(--tile2);
         border:1px solid var(--line);font-size:13px}
  .badge.printing{background:#12351f;border-color:var(--green);color:#9fe8bb}
  .badge.paused{background:#3a2c10;border-color:var(--amber);color:#ffd9a0}
  .sub{color:var(--dim);font-size:13px}
  #ipbox{max-width:1120px;margin:14px auto 0;padding:14px 16px;
         background:#2a2110;border:1px solid var(--amber);border-radius:12px;
         display:none}
  #ipbox .row{align-items:center}
  #ipbox input{background:#0a0e14;color:var(--txt);border:1px solid var(--line);
         border-radius:8px;padding:9px 12px;font-size:15px;width:170px}
  #ipmsg{font-size:13px;color:var(--dim)}
  main{max-width:1120px;margin:0 auto;padding:14px;display:grid;
       grid-template-columns:1fr 1fr;gap:14px}
  .tile{background:var(--tile);border:1px solid var(--line);
        border-radius:12px;padding:14px 16px}
  .wide{grid-column:1 / -1}
  .tile h2{margin:0 0 10px;font-size:14px;letter-spacing:.06em;
           text-transform:uppercase;color:var(--dim)}
  .file{font-size:17px;font-weight:600;word-break:break-all}
  .bar{height:14px;background:#0a0e14;border-radius:7px;overflow:hidden;
       border:1px solid var(--line);margin:10px 0 4px}
  .bar > div{height:100%;background:linear-gradient(90deg,#2f7fe0,var(--accent));
             width:0%;transition:width .6s}
  .row{display:flex;gap:18px;flex-wrap:wrap}
  .kv{font-size:13px;color:var(--dim)}
  .kv b{display:block;font-size:16px;color:var(--txt);font-weight:600}
  .temp{margin:10px 0}
  .temp .lbl{display:flex;justify-content:space-between;font-size:13px;
             color:var(--dim)}
  .temp .lbl b{color:var(--txt);font-size:15px}
  .tbar{height:8px;background:#0a0e14;border-radius:4px;overflow:hidden;
        border:1px solid var(--line)}
  .tbar > div{height:100%;background:var(--amber)}
  #camwrap{position:relative;background:#000;border-radius:8px;overflow:hidden}
  #cam{width:100%;display:block;min-height:180px}
  #cammsg{position:absolute;inset:0;display:flex;align-items:center;
          justify-content:center;color:var(--dim);font-size:14px}
  .slots{display:flex;gap:10px;flex-wrap:wrap}
  .slot{flex:1 1 100px;background:var(--tile2);border:1px solid var(--line);
        border-radius:10px;padding:10px;text-align:center}
  .chip{width:34px;height:34px;border-radius:50%;margin:0 auto 6px;
        border:2px solid var(--line);background:#333}
  .slot .t{font-weight:600;font-size:14px}
  .slot .n{color:var(--dim);font-size:12px}
  .slot.active{border-color:var(--green);
        box-shadow:0 0 0 1px var(--green), 0 0 16px rgba(63,185,112,.35)}
  .slot .inuse{color:var(--green);font-size:11px;font-weight:700;
        letter-spacing:.08em;margin-top:4px}
  .spdbtn.sel{border-color:var(--accent);color:var(--accent)}
  .fanrow{display:flex;align-items:center;gap:10px;margin-top:6px}
  .fanrow label{width:84px;color:var(--dim);font-size:13px}
  .fanrow input{flex:1;accent-color:var(--accent)}
  .fanrow .fv{width:44px;text-align:right;font-size:13px}
  details.sec{grid-column:1 / -1;background:var(--tile);
              border:1px solid var(--line);border-radius:12px}
  details.sec > summary{cursor:pointer;padding:14px 16px;font-size:14px;
        letter-spacing:.06em;text-transform:uppercase;color:var(--dim);
        list-style:none;user-select:none}
  details.sec > summary::-webkit-details-marker{display:none}
  details.sec > summary::before{content:'▸ '}
  details.sec[open] > summary::before{content:'▾ '}
  .secbody{padding:0 16px 16px}
  .btns{display:flex;gap:10px;flex-wrap:wrap}
  button{background:var(--tile2);color:var(--txt);border:1px solid var(--line);
         border-radius:8px;padding:10px 18px;font-size:15px;cursor:pointer}
  button:hover{border-color:var(--accent)}
  button:disabled{opacity:.4;cursor:default}
  button.danger{border-color:var(--red);color:#ffb4ae}
  #ctlmsg{margin-top:10px;font-size:13px;color:var(--dim);min-height:16px}
  table{width:100%;border-collapse:collapse;font-size:14px}
  td,th{padding:7px 6px;border-bottom:1px solid var(--line);text-align:left}
  th{color:var(--dim);font-weight:600;font-size:12px;text-transform:uppercase}
  td img{width:44px;height:44px;object-fit:cover;border-radius:6px;
         background:#0a0e14}
  .chipst{padding:2px 8px;border-radius:12px;font-size:12px;
          background:var(--tile2);border:1px solid var(--line)}
  .chipst.ok{border-color:var(--green);color:#9fe8bb}
  .chipst.bad{border-color:var(--red);color:#ffb4ae}
  footer{max-width:1120px;margin:0 auto;padding:4px 16px 18px;
         color:var(--dim);font-size:12px;display:flex;
         justify-content:space-between}
  @media (max-width:760px){ main{grid-template-columns:1fr} }
</style>
</head>
<body>
<header>
  <span style="font-size:22px">🖨️</span>
  <h1 id="pname">Printer</h1>
  <span class="dot" id="dot"></span>
  <span class="badge" id="state">—</span>
  <span class="sub" id="fw"></span>
  <span class="sub" id="err" style="color:var(--red)"></span>
</header>
<div id="ipbox">
  <div class="row">
    <span id="ipask" style="font-size:15px">Printer not found on the network.</span>
    <span class="sub">The printer's IP is on its touchscreen under Network.</span>
  </div>
  <div class="row" style="margin-top:10px">
    <input id="ipinput" placeholder="192.168.1.50" inputmode="decimal">
    <button id="ipsave">Save &amp; connect</button>
    <span id="ipmsg"></span>
  </div>
</div>
<main>
  <section class="tile wide">
    <h2>Now printing</h2>
    <div class="file" id="file">—</div>
    <div class="bar"><div id="prog"></div></div>
    <div class="row">
      <div class="kv">Progress<b id="pct">—</b></div>
      <div class="kv">Layer<b id="layer">—</b></div>
      <div class="kv">Elapsed<b id="elapsed">—</b></div>
      <div class="kv">Remaining<b id="remain">—</b></div>
      <div class="kv">Speed<b id="speed">—</b></div>
      <div class="kv">Position<b id="coords" style="font-size:13px">—</b></div>
    </div>
  </section>

  <section class="tile">
    <h2>Temperatures</h2>
    <div class="temp"><div class="lbl"><span>🌡️ Nozzle</span>
      <b id="noz">—</b></div><div class="tbar"><div id="nozb"></div></div></div>
    <div class="temp"><div class="lbl"><span>🛏️ Bed</span>
      <b id="bed">—</b></div><div class="tbar"><div id="bedb"></div></div></div>
    <div class="temp"><div class="lbl"><span>📦 Chamber</span>
      <b id="box">—</b></div><div class="tbar"><div id="boxb"></div></div></div>
    <div class="row" style="margin-top:8px">
      <div class="kv">Model fan<b id="fmodel">—</b></div>
      <div class="kv">Aux fan<b id="faux">—</b></div>
      <div class="kv">Box fan<b id="fbox">—</b></div>
      <div class="kv">Light<b id="light">—</b></div>
    </div>
  </section>

  <section class="tile">
    <h2>Camera</h2>
    <div id="camwrap"><img id="cam" alt="">
      <div id="cammsg">connecting…</div></div>
    <div style="margin-top:8px"><button id="camreload">↻ Reconnect camera</button></div>
  </section>

  <section class="tile wide">
    <h2>🎨 CANVAS — filament</h2>
    <div id="canvas"><span class="sub">Waiting for the printer's status feed…</span></div>
  </section>

  <details class="sec" id="sec-controls">
    <summary>🎛️ Controls</summary>
    <div class="secbody">
      <div class="btns">
        <button id="bpause">⏸ Pause</button>
        <button id="bresume">▶ Resume</button>
        <button id="bstop" class="danger">⏹ Stop print</button>
        <button id="blight">💡 Light</button>
      </div>
      <div class="btns" style="margin-top:8px;align-items:center">
        <span class="sub">Speed</span>
        <button class="spdbtn" data-speed="50">50%</button>
        <button class="spdbtn" data-speed="75">75%</button>
        <button class="spdbtn" data-speed="100">100%</button>
        <button class="spdbtn" data-speed="125">125%</button>
        <button class="spdbtn" data-speed="150">150%</button>
      </div>
      <div style="margin-top:10px">
        <div class="fanrow"><label>Model fan</label>
          <input type="range" id="fan-model" min="0" max="100" step="5">
          <span class="fv" id="fv-model">—</span></div>
        <div class="fanrow"><label>Aux fan</label>
          <input type="range" id="fan-aux" min="0" max="100" step="5">
          <span class="fv" id="fv-aux">—</span></div>
        <div class="fanrow"><label>Box fan</label>
          <input type="range" id="fan-box" min="0" max="100" step="5">
          <span class="fv" id="fv-box">—</span></div>
      </div>
      <div id="ctlmsg"></div>
    </div>
  </details>

  <details class="sec" id="sec-history">
    <summary>🕘 Print history</summary>
    <div class="secbody">
      <table><thead><tr><th></th><th>File</th><th>Status</th>
        <th>When</th><th>Duration</th></tr></thead>
      <tbody id="hist"><tr><td colspan="5" class="sub">Loading…</td></tr></tbody></table>
    </div>
  </details>
</main>
<footer><span id="build"></span><span>SDCP · standalone · port 8091</span></footer>

<script>
var $ = function(id){ return document.getElementById(id); };
function fmtT(s){
  if(!s && s !== 0) return '—';
  s = Math.round(s); var h = Math.floor(s/3600), m = Math.floor((s%3600)/60),
      x = s%60;
  return (h ? h+'h ' : '') + (h || m ? m+'m ' : '') + x + 's';
}
function fmtClock(s){ return s ? fmtT(s) : '—'; }

/* collapsible sections: remember open/closed, both start closed */
['controls','history'].forEach(function(k){
  var d = $('sec-' + k);
  try {
    if (localStorage.getItem('pdash-' + k) === 'open') d.open = true;
  } catch(e){}
  d.addEventListener('toggle', function(){
    try { localStorage.setItem('pdash-' + k, d.open ? 'open' : 'closed'); }
    catch(e){}
    if (k === 'history' && d.open) loadHistory();
  });
});

var camStarted = false;
function startCam(){
  var img = $('cam');
  img.onload = function(){ $('cammsg').style.display = 'none'; };
  img.onerror = function(){ $('cammsg').style.display = 'flex';
    $('cammsg').textContent = 'Camera not available right now'; };
  img.src = '/camera?t=' + Date.now();
  camStarted = true;
}
$('camreload').onclick = startCam;

function renderCanvas(c){
  var el = $('canvas');
  if (!c || !c.found){
    el.innerHTML = '<span class="sub">No CANVAS / filament data in the ' +
      'printer status feed yet. (If the CANVAS is on and loaded, its data ' +
      'shows up here automatically.)</span>';
    return;
  }
  if (!c.slots.length){
    el.innerHTML = '<span class="sub">CANVAS data present (' +
      c.extra_keys.join(', ') + ') — slot layout not recognized yet.</span>';
    return;
  }
  el.innerHTML = '<div class="slots">' + c.slots.map(function(s){
    var col = (s.color && s.color.charAt(0) === '#') ? s.color : '#666';
    return '<div class="slot' + (s.active ? ' active' : '') + '">' +
      '<div class="chip" style="background:' + col +
      '"></div><div class="t">' + (s.type || 'Filament') + '</div>' +
      '<div class="n">Slot ' + s.slot + (s.color ? ' · ' + s.color : '') +
      '</div>' + (s.active ? '<div class="inuse">● IN USE</div>' : '') +
      '</div>';
  }).join('') + '</div>';
}

function render(d){
  $('pname').textContent = d.machine_name || 'Printer';
  $('dot').className = 'dot' + (d.online ? ' on' : '');
  $('fw').textContent = d.firmware ? 'Firmware ' + d.firmware : '';
  $('err').textContent = d.online ? '' : (d.error || 'offline');
  var st = d.print_state || d.machine_state || '—';
  var badge = $('state');
  badge.textContent = d.online ? st : 'Offline';
  badge.className = 'badge' + (st === 'Printing' ? ' printing' :
                    (st === 'Paused' ? ' paused' : ''));
  $('file').textContent = d.filename || (d.online ? 'Nothing printing' : '—');
  var p = (d.progress === null || d.progress === undefined) ? null : d.progress;
  $('prog').style.width = (p === null ? 0 : p) + '%';
  $('pct').textContent = p === null ? '—' : p + '%';
  $('layer').textContent = (d.current_layer || d.current_layer === 0) &&
    d.total_layer ? d.current_layer + ' / ' + d.total_layer : '—';
  $('elapsed').textContent = fmtClock(d.elapsed_s);
  $('remain').textContent = fmtClock(d.remaining_s);
  $('speed').textContent = d.print_speed ? d.print_speed + ' ' + (d.speed_unit || 'mm/s') : '—';
  $('coords').textContent = d.coords || '—';
  function temp(id, bid, cur, tgt, max){
    $(id).textContent = (cur === null || cur === undefined) ? '—' :
      Math.round(cur) + '°' + (tgt ? ' / ' + Math.round(tgt) + '°' : '');
    $(bid).style.width = (cur ? Math.min(100, 100 * cur / max) : 0) + '%';
  }
  temp('noz','nozb', d.nozzle, d.nozzle_target, 320);
  temp('bed','bedb', d.bed, d.bed_target, 110);
  temp('box','boxb', d.box, null, 80);
  function fan(id, v){ $(id).textContent =
    (v === null || v === undefined) ? '—' : v + '%'; }
  fan('fmodel', d.fans.model); fan('faux', d.fans.aux); fan('fbox', d.fans.box);
  syncFan('model', d.fans.model); syncFan('aux', d.fans.aux);
  syncFan('box', d.fans.box);
  $('light').textContent = d.light_on === null || d.light_on === undefined ?
    '—' : (d.light_on ? 'On' : 'Off');
  renderCanvas(d.canvas);
  var spd = document.querySelectorAll('.spdbtn');
  for (var i = 0; i < spd.length; i++){
    var on = d.speed_unit === '%' && d.print_speed !== null &&
      d.print_speed !== undefined &&
      parseInt(spd[i].dataset.speed, 10) === Math.round(d.print_speed);
    spd[i].className = 'spdbtn' + (on ? ' sel' : '');
  }
  $('build').textContent = d.build || '';
  var box = $('ipbox');
  if (!d.online){
    box.style.display = 'block';
    $('ipask').textContent = d.needs_ip ?
      'Printer not found on the network — enter its IP address:' :
      'Cannot reach the printer — check its IP address:';
    var inp = $('ipinput');
    if (document.activeElement !== inp && d.printer_ip) inp.value = d.printer_ip;
  } else {
    box.style.display = 'none';
  }
  var printing = (st === 'Printing');
  setTimeout(poll, printing ? 2000 : 5000);
  if (d.online && !camStarted) startCam();
}

function poll(){
  fetch('/api/status').then(function(r){ return r.json(); })
    .then(render)
    .catch(function(){ setTimeout(poll, 5000); });
}

function loadHistory(){
  fetch('/api/history').then(function(r){ return r.json(); })
  .then(function(d){
    var tb = $('hist');
    if (!d.history || !d.history.length){
      tb.innerHTML = '<tr><td colspan="5" class="sub">No history yet</td></tr>';
      return;
    }
    tb.innerHTML = d.history.map(function(h){
      var when = h.begin ? new Date(h.begin * 1000).toLocaleString() : '—';
      var cls = h.status === 'Completed' ? 'ok' :
                (h.status === 'Failed' || h.status === 'Stopped') ? 'bad' : '';
      var img = h.thumbnail ? '<img src="/thumb?u=' +
        encodeURIComponent(h.thumbnail) + '" alt="">' : '';
      return '<tr><td>' + img + '</td><td>' + h.filename + '</td>' +
        '<td><span class="chipst ' + cls + '">' + (h.status || '?') +
        '</span></td><td>' + when + '</td><td>' + fmtT(h.duration_s) +
        '</td></tr>';
    }).join('');
  }).catch(function(){});
}

function ctl(action, confirmIt){
  if (action === 'stop' &&
      !confirm('Stop the current print? This cannot be undone.')) return;
  $('ctlmsg').textContent = 'Sending…';
  fetch('/api/control', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({action: action, confirm: !!confirmIt || action === 'stop'})
  }).then(function(r){ return r.json(); })
    .then(function(d){ $('ctlmsg').textContent = d.message || ''; })
    .catch(function(){ $('ctlmsg').textContent = 'Request failed'; });
}
$('bpause').onclick = function(){ ctl('pause'); };
function ctlSpeed(n){
  $('ctlmsg').textContent = 'Sending…';
  fetch('/api/control', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({action: 'speed', value: n})
  }).then(function(r){ return r.json(); })
    .then(function(d){ $('ctlmsg').textContent = d.message || ''; })
    .catch(function(){ $('ctlmsg').textContent = 'Request failed'; });
}
Array.prototype.forEach.call(document.querySelectorAll('.spdbtn'),
  function(b){ b.onclick = function(){ ctlSpeed(parseInt(b.dataset.speed, 10)); }; });
function ctlFan(which, val){
  $('ctlmsg').textContent = 'Sending…';
  fetch('/api/control', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({action: 'fan', fan: which, value: val})
  }).then(function(r){ return r.json(); })
    .then(function(d){ $('ctlmsg').textContent = d.message || ''; })
    .catch(function(){ $('ctlmsg').textContent = 'Request failed'; });
}
['model','aux','box'].forEach(function(w){
  var s = $('fan-' + w);
  s.addEventListener('input', function(){ $('fv-' + w).textContent = s.value + '%'; });
  s.addEventListener('change', function(){ ctlFan(w, parseInt(s.value, 10)); });
});
function syncFan(w, v){
  var s = $('fan-' + w);
  if (document.activeElement !== s && v !== null && v !== undefined){
    s.value = v; $('fv-' + w).textContent = v + '%';
  }
}
$('ipsave').onclick = function(){
  var ip = $('ipinput').value.trim();
  if (!ip){ $('ipmsg').textContent = 'Type the IP first'; return; }
  $('ipmsg').textContent = 'Saving…';
  fetch('/api/set-ip', {method:'POST',
    headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ip: ip})
  }).then(function(r){ return r.json(); })
    .then(function(d){ $('ipmsg').textContent = d.message || ''; })
    .catch(function(){ $('ipmsg').textContent = 'Request failed'; });
};
$('bresume').onclick = function(){ ctl('resume'); };
$('bstop').onclick = function(){ ctl('stop', true); };
$('blight').onclick = function(){ ctl('light'); };

poll();
loadHistory();
setInterval(function(){ if ($('sec-history').open) loadHistory(); }, 60000);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
