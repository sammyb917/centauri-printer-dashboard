#!/usr/bin/env python3
"""
Printer Dashboard (multi-page) — standalone monitor for an Elegoo
Centauri Carbon (+ CANVAS 4-color unit).

Talks to the printer over the LAN using Elegoo's SDCP protocol
(WebSocket on port 3030, UDP discovery on port 3000, camera MJPEG on 3031).
Pure Python standard library — no pip installs.

Run:  python printer-dashboard-multi.py   (serves http://<this-box>:8092/)
Config (OPTIONAL, next to this file): printer-dashboard-config.json
    { "printer_ip": "", "port": 8092 }
Out of the box the printer is found by LAN auto-discovery. A saved or
configured IP is tried first; if the printer stops answering there, the
dashboard falls back to scanning and adopts the address that answers.
If the scan finds nothing, the dashboard page itself asks for the IP
and saves it to the config (merging, never clobbering).

Build tag: printerdash-multi-v17
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
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
# v18 ships with NO config file (a blank config once wiped Sam's keys).
# Printer IP/port live in the shared configs folder next to the v17 config,
# and the Thingiverse token lives in the shared multi-config file there.
# Both are read at runtime; nothing secret is ever baked into this file.
# FALLBACK (Nov 9, 2026 fix): the shared folder only exists in the dev
# workspace — on Sam's media PC it does not. So when the shared files are
# missing (or hold no printer IP), v18 falls back to
# printer-dashboard-config.json / printer-dashboard-multi-config.json
# NEXT TO this file itself — exactly like v17 does. The IP Sam types into
# the page's "Printer not found yet" box is saved to whichever config file
# the IP was loaded from (or next to this file if none existed), so it
# persists across restarts.
CONFIG_PATH = os.path.normpath(os.path.join(
    HERE, "..", "printer-dashboard-multi-configs",
    "printer-dashboard-config.json"))
MULTI_CONFIG_PATH = os.path.normpath(os.path.join(
    HERE, "..", "printer-dashboard-multi-configs",
    "printer-dashboard-multi-config.json"))
LOCAL_CONFIG_PATH = os.path.join(HERE, "printer-dashboard-config.json")
LOCAL_MULTI_CONFIG_PATH = os.path.join(HERE,
                                       "printer-dashboard-multi-config.json")
# Small single-purpose token file (Nov 9, 2026): thingiverse-token.json
# next to this file, {"thingiverse_token": "..."}. Sam's token never made
# it onto the media PC, so v18 also looks here — no JSON editing needed.
THINGIVERSE_TOKEN_PATH = os.path.join(HERE, "thingiverse-token.json")
BUILD_TAG = "printerdash-multi-v18"

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

DEFAULTS = {"printer_ip": "", "port": 8092}


def _read_json_config(path):
    """Parsed JSON dict at path; None if the file is absent, {} if it
    exists but is unreadable or not a JSON object."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else {}
    except FileNotFoundError:
        return None
    except Exception as e:
        print(f"[config] could not read {path}: {e} (using defaults)")
        return {}


def load_config():
    """Shared configs folder first, then the file next to this script
    (v17-style) filling in anything still empty. Returns (cfg, source)
    where source is the file the printer IP came from — that is where
    save_printer_ip writes, so a typed-in IP persists across restarts.
    If no file supplied an IP, source is the local file next to this
    script."""
    cfg = dict(DEFAULTS)
    filled = set()
    source = LOCAL_CONFIG_PATH
    for path in (CONFIG_PATH, LOCAL_CONFIG_PATH):
        saved = _read_json_config(path)
        if not saved:
            continue
        for k in DEFAULTS:
            if k in saved and saved[k] not in (None, "") and k not in filled:
                cfg[k] = saved[k]
                filled.add(k)
                if k == "printer_ip":
                    source = path
    return cfg, source


CONFIG, CONFIG_SOURCE = load_config()

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
    whatever is already there. Writes to the file the IP was loaded from
    (CONFIG_SOURCE) — the shared folder when it exists, otherwise the
    config next to this file. Only ever called from the page's IP form or
    when discovery finds the printer at a new address — the app never
    creates or rewrites the config on its own."""
    cfg = {}
    try:
        with open(CONFIG_SOURCE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        if not isinstance(cfg, dict):
            cfg = {}
    except Exception:
        cfg = {}
    cfg["printer_ip"] = ip
    tmp = CONFIG_SOURCE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, CONFIG_SOURCE)
    CONFIG["printer_ip"] = ip


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

# ------------------------------------------------------- 3D model search --
# Server-side search across the free model sites, so the page never needs
# keys in JavaScript. Thingiverse's app token is read from the shared
# multi-config file at runtime (never printed, never baked into this file).
# Cults3D has no API key on file yet, so it is skipped server-side until
# Sam adds one (see search_3d); the UI says so and links out instead.
# All three live endpoints below were verified with plain curl Nov 2026:
#   MakerWorld  GET  api.bambulab.com/v1/search-service/select/design2
#   Printables  POST api.printables.com/graphql/  (searchPrints2, items{})
#   Thingiverse GET  api.thingiverse.com/search/<term>?access_token=...
# These are the sites' own undocumented endpoints: they can change or
# break without notice. Every site call degrades to ok:false on failure
# so one dead site never breaks the search page.

def load_thingiverse_token():
    for path in (MULTI_CONFIG_PATH, LOCAL_MULTI_CONFIG_PATH,
                 THINGIVERSE_TOKEN_PATH):
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            tok = (cfg or {}).get("thingiverse_token") or ""
            if tok and isinstance(tok, str):
                return tok
        except Exception:
            continue
    return ""


SEARCH_UA = {"User-Agent": "printer-dashboard-v18 (Elegoo Centauri Carbon)"}
SEARCH_TIMEOUT = 12


def _search_get_json(url):
    req = urllib.request.Request(url, headers=SEARCH_UA)
    with urllib.request.urlopen(req, timeout=SEARCH_TIMEOUT) as resp:
        return json.load(resp)


def _search_post_json(url, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data,
        headers=dict(SEARCH_UA, **{"Content-Type": "application/json"}))
    with urllib.request.urlopen(req, timeout=SEARCH_TIMEOUT) as resp:
        return json.load(resp)


def _search_makerworld(q, limit=12):
    try:
        d = _search_get_json(
            "https://api.bambulab.com/v1/search-service/select/design2?keyword="
            + quote(q) + "&limit=%d&offset=0" % limit)
        out = []
        for h in (d.get("hits") or [])[:limit]:
            mid, slug = h.get("id"), h.get("slug") or ""
            if not mid:
                continue
            out.append({"site": "MakerWorld", "id": str(mid),
                        "name": h.get("title") or "Untitled",
                        "url": "https://makerworld.com/en/models/%s-%s" % (mid, slug),
                        "thumb": h.get("cover") or "",
                        "free": True, "price": ""})
        return {"site": "MakerWorld", "ok": True, "results": out}
    except Exception as e:
        return {"site": "MakerWorld", "ok": False, "error": str(e), "results": []}


def _search_printables(q, limit=12):
    try:
        d = _search_post_json("https://api.printables.com/graphql/", {
            "query": "query Search($q:String!,$l:Int!){searchPrints2(query:$q,"
                     "limit:$l,ordering:best_match){totalCount items{id name "
                     "slug premium price image{filePath}}}}",
            "variables": {"q": q, "l": limit}})
        items = ((d.get("data") or {}).get("searchPrints2") or {}).get("items") or []
        out = []
        for it in items[:limit]:
            pid, slug = it.get("id"), it.get("slug") or ""
            if not pid:
                continue
            img = it.get("image") or {}
            fp = img.get("filePath") or ""
            price = it.get("price")
            paid = bool(it.get("premium")) or price not in (None, "", 0, "0")
            out.append({"site": "Printables", "id": str(pid),
                        "name": it.get("name") or "Untitled",
                        "url": "https://www.printables.com/model/%s-%s" % (pid, slug),
                        "thumb": ("https://media.printables.com/" + fp) if fp else "",
                        "free": not paid,
                        "price": ("" if price is None else str(price))})
        return {"site": "Printables", "ok": True, "results": out}
    except Exception as e:
        return {"site": "Printables", "ok": False, "error": str(e), "results": []}


def _search_thingiverse(q, limit=12):
    token = load_thingiverse_token()
    if not token:
        return {"site": "Thingiverse", "ok": False,
                "error": "no Thingiverse token in config", "results": []}
    try:
        d = _search_get_json(
            "https://api.thingiverse.com/search/" + quote(q)
            + "?access_token=" + quote(token) + "&per_page=%d" % limit)
        out = []
        for h in (d.get("hits") or [])[:limit]:
            tid = h.get("id")
            if not tid:
                continue
            # Search hits carry no paid flag on Thingiverse; treat as free.
            out.append({"site": "Thingiverse", "id": str(tid),
                        "name": h.get("name") or "Untitled",
                        "url": h.get("public_url") or
                               ("https://www.thingiverse.com/thing:%s" % tid),
                        "thumb": h.get("thumbnail") or "",
                        "free": True, "price": ""})
        return {"site": "Thingiverse", "ok": True, "results": out}
    except Exception as e:
        return {"site": "Thingiverse", "ok": False, "error": str(e), "results": []}


def search_3d(q, sites, free_only=True):
    """Run the enabled site searches in parallel and merge the results.
    sites: subset of makerworld/printables/thingiverse/cults3d.
    Cults3D has no key yet: it is reported as unavailable, never faked."""
    q = (q or "").strip()
    if not q:
        return {"ok": False, "error": "empty query", "results": [], "sites": []}
    want = [s for s in ("makerworld", "printables", "thingiverse", "cults3d")
            if s in (sites or [])]
    if not want:
        want = ["makerworld", "printables", "thingiverse"]
    fns = {"makerworld": _search_makerworld,
           "printables": _search_printables,
           "thingiverse": _search_thingiverse}
    results, site_reports = [], []
    threads = []

    def run(fn, site):
        site_reports.append(fn(q))

    for s in want:
        if s == "cults3d":
            site_reports.append({"site": "Cults3D", "ok": False,
                                 "error": "no Cults3D API key yet — add one at "
                                          "cults3d.com/en/api/keys and put it in "
                                          "the shared config to enable it",
                                 "results": []})
            continue
        t = threading.Thread(target=run, args=(fns[s], s), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join(timeout=SEARCH_TIMEOUT + 5)
    for rep in site_reports:
        for r in rep.get("results") or []:
            if free_only and not r.get("free"):
                continue
            results.append(r)
    return {"ok": True, "query": q, "results": results, "sites": site_reports}


# ---------------------------------------------------------------- filament --
# The Filament tab's seed: ONLY the real spool from Sam's filament app.
# (Inland PLA Black, 1000 g, full Orca settings. The Elegoo PLA Blue demo
# spool is deliberately NOT imported.) Added spools live in the browser's
# localStorage; this seed is the starting point, not a live sync.
FILAMENT_SEED = [{
    "brand": "Inland", "material": "PLA", "colorName": "Black",
    "colorHex": "#232323", "capacityG": 1000, "remainingG": 1000, "spools": 1,
    "settings": {"nozzleTempC": 210, "firstLayerNozzleTempC": 215,
                 "bedTempC": 60, "firstLayerBedTempC": 65, "fanPercent": 100,
                 "flowRatio": 0.98, "pressureAdvance": 0.03,
                 "maxVolumetricSpeed": 22, "filamentDensity": 1.24,
                 "filamentDiameterMm": 1.75, "retractionMm": 0.8,
                 "retractionSpeedMms": 40, "zHopMm": 0.4,
                 "printSpeedMms": 120, "adaptivePressureAdvance": False},
}]


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
        elif path == "/api/search3d":
            qs = parse_qs(urlparse(self.path).query)
            q = (qs.get("q") or [""])[0]
            sites = (qs.get("sites") or [""])[0]
            free = (qs.get("free") or ["1"])[0] not in ("0", "false", "no")
            site_list = [s.strip().lower() for s in sites.split(",") if s.strip()] \
                if sites else []
            self._json(search_3d(q, site_list, free_only=free))
        elif path == "/api/filament":
            self._json({"ok": True, "seed": FILAMENT_SEED,
                        "note": "Seed only: the Inland PLA Black spool from "
                                "the filament app. Added spools live in the "
                                "browser."})
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
            try:
                if conn.sock is not None:
                    conn.sock.settimeout(10)
            except Exception:
                pass
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
                                       "(use dotted-quad form)"}, 400)
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
            # NOTE: v17 sent {"CurrentFanSpeed": ...} here, but that is the
            # STATUS read-back field name. The verified SDCP reference
            # (pycentauri docs/PROTOCOL.md, reverse-engineered from Elegoo's
            # elegoo-link SDK, confirmed live 2026-06-03) says the Cmd 403
            # fan SET payload is {"TargetFanSpeed": {...}}. Switched to the
            # verified key — LIVE TEST: move a fan slider and watch the fan
            # value in the dashboard to confirm the printer took it.
            queue_command(CMD_LIGHT, {"TargetFanSpeed": bundle})
            self._json({"ok": True, "message": f"{keys[which]} {value}% sent"})
        elif action == "temp":
            # CMD 403 target temps — verified against pycentauri's PROTOCOL.md
            # (elegoo-link SDK + live wire captures, confirmed live 2026-06-03):
            #   {"TempTargetNozzle": <C>, "TempTargetHotbed": <C>, "TempTargetBox": <C>}
            # The firmware only changes the keys present, so send just the
            # one the user edited. LIVE TEST: set a temp mid-print and watch
            # the target shown on the tile — the page shows the printer's
            # actual reported target, so you can see whether it took.
            payload = {}
            try:
                if req.get("nozzle") is not None:
                    v = int(req.get("nozzle"))
                    if not 0 <= v <= 320:
                        raise ValueError("nozzle must be 0-320")
                    payload["TempTargetNozzle"] = v
                if req.get("bed") is not None:
                    v = int(req.get("bed"))
                    if not 0 <= v <= 120:
                        raise ValueError("bed must be 0-120")
                    payload["TempTargetHotbed"] = v
                if req.get("box") is not None:
                    v = int(req.get("box"))
                    if not 0 <= v <= 60:
                        raise ValueError("chamber must be 0-60")
                    payload["TempTargetBox"] = v
            except (TypeError, ValueError) as e:
                self._json({"ok": False, "message": f"bad temp: {e}"}, 400)
                return
            if not payload:
                self._json({"ok": False, "message": "temp needs a value"}, 400)
                return
            queue_command(CMD_CONTROL, payload)
            self._json({"ok": True, "message": "Target temp sent — watch the "
                        "tile's target value to confirm the printer took it"})
        else:
            self._json({"ok": False, "message": f"unknown action {action!r}"}, 400)


def main():
    port = int(CONFIG.get("port") or 8092)
    threading.Thread(target=sdcp_session, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Printer Dashboard ({BUILD_TAG}) on http://0.0.0.0:{port}/  "
          f"— printer: {CONFIG.get('printer_ip') or 'auto-discovery'}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


# ------------------------------------------------------------------ page ----
PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Printer Dashboard</title>
<style>
  :root{
    --bg:#0b0e13; --tile:#161c27; --tile2:#1a2130; --line:#242e3f;
    --txt:#eef1f6; --dim:#8b94a3; --accent:#5b8cff; --green:#4be38a;
    --amber:#f2c14e; --red:#ff5f56; --blue:#57a8ff; --purple:#b48cff;
    --svc:#5b8cff;
  }
  *{box-sizing:border-box}
  body{margin:0;color:var(--txt);
       font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
       background:#0b0e13;
       background-image:radial-gradient(1000px 500px at 50% -8%, #182233 0%, #0b0e13 55%),
         radial-gradient(800px 400px at 100% 0%, rgba(91,140,255,.08), transparent 60%),
         radial-gradient(700px 500px at 0% 100%, rgba(180,140,255,.06), transparent 60%);
       background-attachment:fixed;-webkit-font-smoothing:antialiased}
  header{display:flex;align-items:center;gap:12px;padding:14px 18px;
         border-bottom:1px solid var(--line);flex-wrap:wrap}
  header h1{font-size:22px;font-weight:750;letter-spacing:.2px;margin:0;
         background:linear-gradient(92deg,#8fb0ff 0%,#c09aff 55%,#7fd8ff 100%);
         background-size:200% 100%;-webkit-background-clip:text;background-clip:text;
         -webkit-text-fill-color:transparent;color:transparent;
         animation:hueshift 8s ease-in-out infinite alternate}
  @keyframes hueshift{from{background-position:0% 0}to{background-position:100% 0}}
  .dot{width:11px;height:11px;border-radius:50%;background:var(--red)}
  .dot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dotpulse 1.2s infinite}
  @keyframes dotpulse{50%{opacity:.35}}
  .badge{padding:3px 10px;border-radius:20px;background:var(--tile2);
         border:1px solid var(--line);font-size:13px}
  .badge.printing{background:#12351f;border-color:var(--green);color:#9fe8bb;box-shadow:0 0 10px rgba(75,227,138,.25)}
  .badge.paused{background:#3a2c10;border-color:var(--amber);color:#ffd9a0}
  .sub{color:var(--dim);font-size:13px}
  #mutebtn,#fsbtn{background:var(--tile2);color:var(--txt);
           border:1px solid var(--line);border-radius:8px;padding:6px 12px;
           font-size:16px;cursor:pointer;transition:background .15s ease,transform .08s ease}
  #mutebtn:hover,#fsbtn:hover{background:#232d40}
  #mutebtn:active,#fsbtn:active{transform:scale(.94)}
  #fsbtn{margin-left:auto}
  nav{display:flex;gap:6px;padding:10px 18px;flex-wrap:wrap;
      border-bottom:1px solid var(--line);background:rgba(11,14,19,.92);
      backdrop-filter:blur(8px);
      position:sticky;top:0;z-index:5}
  nav a{color:var(--dim);text-decoration:none;padding:7px 14px;
        border-radius:20px;border:1px solid transparent;font-size:14px;
        transition:background .15s ease,color .15s ease,border-color .15s ease}
  nav a:hover{color:var(--txt);background:rgba(91,140,255,.12)}
  nav a.on{color:#fff;background:rgba(91,140,255,.25);border-color:var(--accent);
        box-shadow:0 0 12px rgba(91,140,255,.25)}
  #ipbox{margin:14px auto 0;padding:14px 16px;
         background:#2a2110;border:1px solid var(--amber);border-radius:12px;
         display:none}
  #ipbox input{background:#0a0e14;color:var(--txt);border:1px solid var(--line);
         border-radius:8px;padding:9px 12px;font-size:15px;width:170px}
  #ipmsg{font-size:13px;color:var(--dim)}
  main{margin:0 auto;padding:16px 24px}
  .page{display:none}
  .page.on{display:block;animation:pagein .25s ease}
  @keyframes pagein{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:none}}
  .grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px;grid-auto-rows:minmax(32px,auto);grid-auto-flow:dense}
  @media (max-width:760px){ .grid{grid-template-columns:1fr} }
  @media (min-width:1920px){
    main{padding:20px 32px}
    .grid{grid-template-columns:repeat(auto-fill,minmax(380px,1fr));gap:20px}
  }
  @media (min-width:2560px){
    main{padding:24px 40px}
    .grid{grid-template-columns:repeat(auto-fill,minmax(420px,1fr));gap:22px}
  }
  .tile{--svc:#5b8cff;position:relative;overflow:hidden;
        background:linear-gradient(180deg,#161c27 0%,#11151d 100%);
        border:1px solid var(--line);border-radius:16px;padding:16px 16px 14px;
        box-shadow:0 6px 20px rgba(0,0,0,.35);
        transition:transform .15s ease,box-shadow .15s ease,border-color .15s ease}
  .tile::before{content:"";position:absolute;top:0;left:0;right:0;height:3px;
        background:linear-gradient(90deg,var(--svc),transparent 85%)}
  .tile:hover{transform:translateY(-2px);box-shadow:0 10px 28px rgba(0,0,0,.45);border-color:#2a3547}
  .tile h2{cursor:grab;touch-action:none}
  .tile h2:active{cursor:grabbing}
  .tile.dragging{opacity:.55;transform:none;box-shadow:0 16px 40px rgba(0,0,0,.6)}
  .tile-ghost{border:2px dashed var(--accent);background:rgba(91,140,255,.08);min-height:80px}
  .resize-handle{position:absolute;right:4px;bottom:4px;width:28px;height:28px;cursor:nwse-resize;opacity:.85;touch-action:none;
    background:linear-gradient(135deg,transparent 50%,#5b8cff 50%,#5b8cff 58%,transparent 58%),
      linear-gradient(135deg,transparent 66%,#5b8cff 66%,#5b8cff 74%,transparent 74%),
      linear-gradient(135deg,transparent 82%,#5b8cff 82%,#5b8cff 90%,transparent 90%)}
  .resize-handle:hover{opacity:1}
  .tile.resizing{transform:none;transition:none;box-shadow:0 16px 40px rgba(0,0,0,.6)}
  .rs-badge{position:absolute;top:8px;right:10px;z-index:5;background:#5b8cff;color:#fff;font-size:12px;font-weight:700;padding:3px 10px;border-radius:20px;box-shadow:0 4px 14px rgba(0,0,0,.5);pointer-events:none}
  #hm-map{height:220px}
        display:flex;flex-direction:column;background:#0d1117;border:1px solid var(--line);
        border-radius:14px;box-shadow:0 16px 48px rgba(0,0,0,.65);overflow:hidden}
        background:#161c27;cursor:grab;touch-action:none;user-select:none;
        font-size:13px;font-weight:700;letter-spacing:.04em;text-transform:uppercase;color:#fff}
    background:linear-gradient(135deg,transparent 50%,#5b8cff 50%,#5b8cff 58%,transparent 58%),
      linear-gradient(135deg,transparent 66%,#5b8cff 66%,#5b8cff 74%,transparent 74%),
      linear-gradient(135deg,transparent 82%,#5b8cff 82%,#5b8cff 90%,transparent 90%)}
  #nw-map{height:480px}
  /* ---------- v18: stat tiles, ring, temp editor, search, filament, maintenance ---------- */
  #page-home .grid{grid-template-columns:repeat(4,minmax(0,1fr))}
  #page-home .tile.wide{grid-column:span 4}
  @media (max-width:760px){ #page-home .grid{grid-template-columns:1fr} #page-home .tile.wide{grid-column:span 1} }
  .tile.stat-tile{padding:14px}
  .tile.stat-tile .big{font-size:26px;font-weight:750;line-height:1.1}
  .tile.stat-tile.light-tile{cursor:pointer}
  .tile.stat-tile.light-tile:hover{border-color:#3a4763}
  .tile.temp-tile{cursor:pointer}
  .tile.temp-tile:hover{border-color:#3a4763}
  .gaugebox.temp-tile{cursor:pointer}
  .gaugebox.temp-tile:hover{outline:1px solid #3a4763;border-radius:10px}
  .temp-editor{margin-top:10px;border-top:1px solid var(--line);padding-top:10px;text-align:left}
  .temp-editor input[type="number"]{width:96px;padding:8px 10px;font-size:16px;border:1.5px solid var(--line);border-radius:8px;background:#0a0e14;color:var(--txt)}
  .temp-editor .btn{padding:8px 14px;font-size:14px}
  .temp-error{font-size:13px;color:var(--red);font-weight:600;margin:6px 0 0}
  .ring-wrap{position:relative;width:200px;height:200px;margin:0 auto}
  .ring-wrap canvas{width:100%;height:100%;display:block}
  .ring-center{position:absolute;inset:0;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;pointer-events:none}
  .ring-pct{font-size:44px;font-weight:750;line-height:1}
  .ring-lab{font-size:12px;color:var(--dim);margin-top:4px;max-width:170px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .minichart{width:100%;height:40px;display:block;margin-top:8px}
  .minichart .tline{fill:none;stroke-width:2;stroke-linejoin:round;stroke-linecap:round}
  .minichart .tarea{stroke:none;opacity:.13}
  .minichart .ttarget{stroke:#8fa3ba;stroke-width:1;stroke-dasharray:4 4;opacity:.75}
  .chip{appearance:none;border:1px solid var(--line);background:var(--tile2);color:var(--dim);border-radius:20px;padding:7px 14px;font-size:14px;font-weight:700;cursor:pointer}
  .chip.on{color:#fff;background:rgba(91,140,255,.25);border-color:var(--accent)}
  #layout-lock{margin-left:auto;border-radius:20px;padding:7px 14px;font-size:14px;background:var(--tile2);color:var(--txt);border:1px solid var(--line);cursor:pointer}
  .camwrap.lights-off img{filter:brightness(.38) saturate(.8)}
  .search-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:14px}
  .search-card{background:var(--tile2);border:1px solid var(--line);border-radius:12px;overflow:hidden}
  .search-card img{width:100%;height:140px;object-fit:cover;display:block;background:#0a0e14}
  .search-card .sc-body{padding:10px 12px}
  .search-card .sc-name{font-weight:700;font-size:15px;line-height:1.25}
  .search-card .sc-site{font-size:12px;color:var(--dim);margin-top:2px}
  .search-card .sc-tag{display:inline-block;background:rgba(91,140,255,.25);border:1px solid var(--accent);color:#fff;font-size:11px;font-weight:700;padding:2px 8px;border-radius:10px}
  .search-card .sc-link{display:inline-block;margin-top:8px;font-size:14px;font-weight:700;color:#8fb0ff}
  .search-card .sc-paid{color:var(--amber);font-size:13px;font-weight:700}
  .maint-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:14px;margin-top:12px}
  .maint-card{background:var(--tile2);border:1px solid var(--line);border-radius:12px;padding:14px}
  .maint-card h3{margin:0 0 8px;font-size:16px}
  .maint-card h3 a{color:var(--txt);text-decoration:none}
  .maint-card h3 a:hover{text-decoration:underline;color:#8fb0ff}
  .maint-badge{display:inline-block;font-size:12px;font-weight:700;padding:2px 10px;border-radius:20px}
  .maint-badge.overdue{background:#3a1414;color:#ff8a80;border:1px solid #7a2a2a}
  .maint-badge.due{background:#3a2c10;color:#ffd9a0;border:1px solid #7a5c1e}
  .maint-badge.ok{background:#12351f;color:#9fe8bb;border:1px solid #2a6b44}
  .maint-badge.todo{background:#1a2130;color:#8fa3ba;border:1px solid #2a3547}
  .maint-bar{height:8px;border-radius:4px;background:#22303f;margin:10px 0;overflow:hidden}
  .maint-bar i{display:block;height:100%;border-radius:4px;background:var(--accent)}
  .fi-group{background:var(--tile2);border:1px solid var(--line);border-radius:12px;padding:14px;margin-bottom:12px}
  .fi-group .fi-head{display:flex;align-items:center;gap:10px}
  .fi-group .fi-swatch{width:26px;height:26px;border-radius:50%;border:1px solid var(--line);flex:none}
  .fi-group .fi-sets{font-size:13px;color:var(--dim);margin-top:8px}
  body.layout-locked .tile h2{cursor:default}
  body.layout-locked .resize-handle{display:none}
  .pin-label-wrap{background:transparent;border:none;box-shadow:none}
  .pin-label-wrap:before{display:none}
  .pin-label{background:rgba(10,14,20,.85);color:#dbe2ec;font-size:.68rem;font-weight:600;padding:2px 8px;border-radius:8px;border:1px solid #2a3547;white-space:nowrap}
  .flow-playing{stroke-dasharray:9 9;animation:dashflow 1.1s linear infinite}
  .flow-buffering{stroke-dasharray:4 7;animation:dashflow 2.4s linear infinite}
  @keyframes dashflow{to{stroke-dashoffset:-18}}
  .wide{grid-column:1 / -1}
  .tile h2{margin:0 0 12px;font-size:15px;font-weight:650;letter-spacing:.04em;
           text-transform:uppercase;color:#fff}
  #page-printer .tile{--svc:#4be38a}
  #page-home .tile{--svc:#5b8cff}
  #page-library .tile{--svc:#b48cff}
  #page-requests .tile{--svc:#c09aff}
  #page-network .tile{--svc:#7fd8ff}
  #page-arr .tile{--svc:#35c5f4}
  #page-search .tile{--svc:#f2c14e}
  #page-options .tile{--svc:#8b94a3}
  .temp{display:flex;align-items:center;gap:14px;margin:12px 0}
  .temp .lbl{flex:1}
  .temp .lbl b{font-size:26px}
  .temp .lbl span{color:var(--dim);font-size:13px}
  .tbar{flex:2;height:14px;background:var(--tile2);border-radius:7px;overflow:hidden;
        box-shadow:inset 0 1px 3px rgba(0,0,0,.5)}
  .tbar > div{height:100%;background:var(--amber);border-radius:7px;
              transition:width .4s}
  @keyframes thpulse{0%,100%{opacity:1}50%{opacity:.35}}
  @keyframes thflash{0%,100%{opacity:1}50%{opacity:.2}}
  .th-heat{color:#ff5347 !important;animation:thpulse 1.1s ease-in-out infinite}
  .th-heat.nopulse{animation:none}
  .th-at{color:#35d07f !important}
  .th-over,.th-cool{color:#57a8ff !important}
  .th-flash{animation:thflash .5s ease-in-out 3}
  .pbar{height:16px;background:var(--tile2);border-radius:8px;overflow:hidden;
        box-shadow:inset 0 1px 3px rgba(0,0,0,.5)}
  .pbar > div{position:relative;height:100%;border-radius:8px;
              background:linear-gradient(90deg,#2f9bff,#4be38a);
              transition:width .4s;overflow:hidden}
  .pbar > div::after{content:"";position:absolute;inset:0;
              background:linear-gradient(90deg,transparent,rgba(255,255,255,.35),transparent);
              background-size:200% 100%;animation:shimmer 2s linear infinite}
  @keyframes shimmer{from{background-position:200% 0}to{background-position:-200% 0}}
  .shimmer{background:linear-gradient(90deg,#1a2130 25%,#232d40 50%,#1a2130 75%);
              background-size:200% 100%;animation:shimmer 1.5s linear infinite}
  .big{font-size:30px;font-weight:750}
  .row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}
  button,.btn{background:var(--tile2);color:var(--txt);border:1px solid var(--line);
         border-radius:10px;padding:9px 16px;font-size:14px;cursor:pointer;
         transition:background .15s ease,transform .08s ease,border-color .15s ease}
  button:hover,.btn:hover{background:#232d40;border-color:var(--accent)}
  button:active,.btn:active{transform:scale(.96)}
  button.on{background:#12351f;border-color:var(--green);color:#9fe8bb}
  button.warn{background:#3a1d1d;border-color:var(--red)}
  input[type=range]{width:100%;accent-color:var(--accent)}
  input[type=text],input[type=number],input[type=password],select{background:#0a0e14;color:var(--txt);
         border:1px solid var(--line);border-radius:8px;padding:9px 12px;
         font-size:15px}
  input[type=text]:focus,input[type=number]:focus,input[type=password]:focus,select:focus{
         outline:none;border-color:var(--accent);box-shadow:0 0 0 2px rgba(91,140,255,.25)}
  input[type=checkbox]{transform:scale(1.3);margin-right:8px;accent-color:var(--accent)}
  label.opt{display:flex;align-items:center;gap:6px;margin:12px 0;font-size:15px}
  .camwrap{position:relative;background:#000;border-radius:12px;overflow:hidden;
           border:1px solid var(--line);box-shadow:0 6px 20px rgba(0,0,0,.4)}
  .camwrap img{width:100%;height:auto;display:block;cursor:zoom-in}
  #camzoom{position:fixed;inset:0;background:rgba(0,0,0,.92);display:none;align-items:center;justify-content:center;z-index:9999;cursor:zoom-out}
  #camzoom.on{display:flex}
  #camzoom img{max-width:96vw;max-height:96vh;width:auto;height:auto}
  #fi-slots{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px}
  .slot{display:flex;gap:12px;align-items:center;padding:12px;
        background:var(--tile2);border:1px solid var(--line);border-radius:12px;
        margin:0;transition:transform .15s ease,box-shadow .15s ease}
  .slot:hover{transform:translateY(-2px);box-shadow:0 8px 20px rgba(0,0,0,.35)}
  .slot.active{border-color:var(--green);box-shadow:0 0 0 1px var(--green) inset,0 0 16px rgba(75,227,138,.25)}
  .slot .chip, #fi-swatch{width:38px;height:38px;border-radius:50%;border:2px solid #00000055;
        flex:none;box-shadow:0 2px 8px rgba(0,0,0,.5)}
  table{width:100%;border-collapse:collapse;font-size:14px}
  th{color:var(--dim);text-align:left;font-weight:600;padding:8px 10px;
     border-bottom:1px solid var(--line)}
  td{padding:10px 8px;border-bottom:1px solid #22303f;vertical-align:middle}
  tbody tr{transition:background .15s ease}
  tbody tr:hover{background:rgba(91,140,255,.06)}
  .gaugebox{text-align:center}
  .gaugebox canvas{max-width:100%;filter:drop-shadow(0 4px 10px rgba(0,0,0,.4))}
  .minigrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px}
  .gaugebox.mini h3{font-size:13px;color:var(--dim);margin:0 0 4px}
  .gaugebox.mini .gvalue{font-size:20px}
  .glabel{color:var(--dim);font-size:13px;margin-top:2px}
  .gvalue{font-size:24px;font-weight:700}
  /* v15 printer page: title quiet, readouts large + bold (colors untouched) */
  #page-printer #ov-file{font-size:17px;font-weight:500}
  #page-printer #ov-pct,#page-printer #ov-layer,#page-printer #ov-time{font-size:22px;font-weight:800}
  #page-printer .temp .lbl b{font-size:34px;font-weight:800}
  #page-printer #ov2-gv-noz,#page-printer #ov2-gv-bed,#page-printer #ov2-gv-box,#page-printer #ov2-gv-prog{font-size:32px;font-weight:800}
  .chart{width:100%;height:260px;display:block}
  .legend{display:flex;gap:16px;flex-wrap:wrap;font-size:13px;color:var(--dim);
          margin-top:6px}
  .legend i{display:inline-block;width:12px;height:12px;border-radius:3px;
            margin-right:5px;vertical-align:-1px}
  .navbadge{display:none;min-width:18px;height:18px;line-height:18px;
        text-align:center;padding:0 4px;border-radius:9px;background:var(--red);
        color:#fff;font-size:11px;font-weight:700;vertical-align:2px}
  .navbadge.show{display:inline-block;box-shadow:0 0 8px rgba(255,95,86,.6)}
  .poster{width:120px;height:180px;object-fit:cover;border-radius:8px;flex:none;
        background:var(--tile2);box-shadow:0 4px 14px rgba(0,0,0,.5);
        transition:transform .15s ease}
  .poster-sm{width:48px;height:72px;object-fit:cover;border-radius:6px;flex:none;
        background:var(--tile2);box-shadow:0 2px 8px rgba(0,0,0,.5)}
  tr:hover .poster{transform:scale(1.04)}
  .tag{display:inline-block;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:700;
       letter-spacing:.02em;
       background:var(--tile2);border:1px solid var(--line);margin-right:5px}
  .tag.inqueue{border-color:var(--accent);color:#bcd8ff}
  .tag.grabbed{border-color:var(--green);color:#9fe8bb}
  .tag.waiting{border-color:var(--amber);color:#ffd9a0}
  .tag.stuck{border-color:var(--red);color:#ff9d97}
  .tag.local{border-color:var(--green);color:#9fe8bb}
  .tag.remote{border-color:var(--accent);color:#bcd8ff}
  .tile h2{cursor:pointer}
  .tile h2::after{content:" ▾";color:var(--dim);font-size:12px}
  .tile.collapsed h2::after{content:" ▸"}
  .tile.collapsed > :not(h2){display:none}
  .qbadge{display:inline-block;padding:2px 9px;border-radius:10px;font-size:12px;font-weight:700;
       background:var(--tile2);border:1px solid var(--line);margin-right:5px}
  .qbadge.q4k{border-color:#b07fd4;color:#d8b8f0}
  .qbadge.q1080{border-color:var(--accent);color:#bcd8ff}
  .qbadge.q720{border-color:var(--amber);color:#ffd9a0}
  #nw-svcs{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px}
  .svc{display:flex;gap:10px;align-items:center;padding:12px;
       background:var(--tile2);border:1px solid var(--line);border-radius:12px;margin:0;
       transition:transform .15s ease,box-shadow .15s ease}
  .svc:hover{transform:translateY(-2px);box-shadow:0 8px 20px rgba(0,0,0,.35)}
  .svc .sdot{width:11px;height:11px;border-radius:50%;background:var(--red);flex:none}
  .svc .sdot.on{background:var(--green);box-shadow:0 0 8px var(--green);animation:dotpulse 1.2s infinite}
  .svc .sdot.warn{background:var(--amber)}
  .note{color:var(--dim);font-size:13px}
  footer{margin:0 auto;padding:14px 18px 30px;color:var(--dim);
         font-size:12px;display:flex;justify-content:space-between;flex-wrap:wrap}
</style>
</head>
<body>
<header>
  <span id="dot" class="dot"></span>
  <h1>Printer Dashboard</h1>
  <span id="mach" class="sub"></span>
  <span id="statebadge" class="badge"></span>
  <button id="fsbtn" title="Fullscreen">⛶</button>
  <button id="mutebtn" title="Toggle print-finished chime">🔊</button>
</header>
<nav id="nav">
  <a href="#/home" data-page="home">Home</a>
  <a href="#/printer" data-page="printer">Printer <span class="navbadge" id="nb-printer"></span> <span class="navbadge" id="nb-tray"></span></a>
  <a href="#/search" data-page="search">Search</a>
  <a href="#/filament" data-page="filament">Filament</a>
  <a href="#/maintenance" data-page="maintenance">Maintenance</a>
  <a href="#/options" data-page="options">Options</a>
  <button id="layout-lock" title="Lock or unlock moving and stretching tiles">🔒 Layout locked</button>
</nav>
<div id="ipbox">
  <div class="row"><b>Printer not found yet.</b>
    <span class="note">Enter its IP address and the dashboard will connect and remember it.</span>
  </div>
  <div class="row" style="margin-top:10px">
    <input id="ipinput" placeholder="Printer IP address" inputmode="decimal">
    <button id="ipsave">Connect</button>
    <span id="ipmsg"></span>
  </div>
</div>
<main>

<section class="page" id="page-printer">
  <div class="grid">
    <div class="tile wide">
      <h2>Print</h2>
      <div class="row"><span id="ov-file" class="big">—</span>
        <span id="ov-state" class="sub"></span></div>
      <div class="pbar" style="margin:12px 0 6px"><div id="ov-prog"></div></div>
      <div class="row"><span id="ov-pct" class="sub"></span>
        <span id="ov-layer" class="sub"></span>
        <span id="ov-time" class="sub"></span>
        <span id="ov-speed" class="sub"></span></div>
    </div>
    <div class="tile">
      <h2>Temperatures</h2>
      <div class="temp"><div class="lbl"><b id="ov-noz">—</b><br><span>Nozzle</span></div>
        <div class="tbar"><div id="ov-nozb"></div></div></div>
      <div class="temp"><div class="lbl"><b id="ov-bed">—</b><br><span>Bed</span></div>
        <div class="tbar"><div id="ov-bedb"></div></div></div>
      <div class="temp"><div class="lbl"><b id="ov-box">—</b><br><span>Chamber</span></div>
        <div class="tbar"><div id="ov-boxb"></div></div></div>
      <p class="note" style="margin-top:10px"><span style="color:#ff5f56">●</span> Heating &nbsp;<span style="color:#4be38a">●</span> At target &nbsp;<span style="color:#57a8ff">●</span> Cooling</p>
    </div>
    <div class="tile">
      <h2>Camera</h2>
      <div class="camwrap" id="ov-camwrap">
        <img id="ov-cam" alt="Printer camera" title="Click to enlarge">
      </div>
      <p class="note" id="ov-camnote" style="display:none"></p>
    </div>
    <div class="tile wide">
      <h2>Gauges</h2>
      <div class="minigrid">
        <div class="gaugebox mini temp-tile" data-temp="nozzle" title="Tap to change the nozzle target temperature"><h3>Nozzle</h3>
          <canvas id="ov2-g-noz" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-noz">—</div><div class="glabel">target <span id="ov2-gt-noz">—</span></div>
          <svg class="minichart" id="ov2-mc-noz" viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden="true"></svg>
          <div class="temp-editor" hidden></div></div>
        <div class="gaugebox mini temp-tile" data-temp="bed" title="Tap to change the bed target temperature"><h3>Bed</h3>
          <canvas id="ov2-g-bed" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-bed">—</div><div class="glabel">target <span id="ov2-gt-bed">—</span></div>
          <svg class="minichart" id="ov2-mc-bed" viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden="true"></svg>
          <div class="temp-editor" hidden></div></div>
        <div class="gaugebox mini" title="Chamber temperature (display only)"><h3>Chamber</h3>
          <canvas id="ov2-g-box" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-box">—</div><div class="glabel">target <span id="ov2-gt-box">—</span></div>
          <svg class="minichart" id="ov2-mc-box" viewBox="0 0 100 40" preserveAspectRatio="none" aria-hidden="true"></svg></div>
        <div class="gaugebox mini"><h3>Print progress</h3>
          <div class="ring-wrap"><canvas id="ov2-g-prog" width="200" height="200"></canvas>
          <div class="ring-center"><div class="ring-pct" id="ov2-gv-prog">—</div><div class="ring-lab" id="ov2-gl-prog"></div></div></div></div>
        <div class="gaugebox mini"><h3>Model fan</h3>
          <canvas id="ov2-g-fanm" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-fanm">—</div></div>
        <div class="gaugebox mini"><h3>Aux fan</h3>
          <canvas id="ov2-g-fana" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-fana">—</div></div>
        <div class="gaugebox mini"><h3>Box fan</h3>
          <canvas id="ov2-g-fanb" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-fanb">—</div></div>
        <div class="gaugebox mini" id="ov2-gb-speed"><h3>Print head speed</h3>
          <canvas id="ov2-g-speed" width="240" height="150"></canvas>
          <div class="gvalue" id="ov2-gv-speed">—</div></div>
      </div>
    </div>
    <div class="tile wide">
      <h2>Charts</h2>
      <h3>Temperatures over time</h3>
      <canvas id="ov2-c-temp" class="chart"></canvas>
      <p class="note" id="ov2-c-tempnote"></p>
      <h3>Fan speeds over time</h3>
      <canvas id="ov2-c-fan" class="chart"></canvas>
      <p class="note" id="ov2-c-fannote"></p>
      <h3>Print head speed over time</h3>
      <canvas id="ov2-c-speed" class="chart"></canvas>
      <p class="note" id="ov2-c-speednote"></p>
    </div>
    <div class="tile wide">
      <h2>CANVAS trays</h2>
      <div id="ov2-fi-slots"></div>
      <p class="note" id="ov2-fi-refill"></p>
      <p class="note" id="ov2-fi-note" style="display:none"></p>
    </div>
    <div class="tile"><h2>Printer Controls</h2>
      <div class="row">
        <button id="ov2-ctl-pause">Pause</button>
        <button id="ov2-ctl-resume">Resume</button>
        <button id="ov2-ctl-stop" class="warn">Stop</button>
      </div>
      <p class="note" id="ov2-ctl-msg" style="margin-top:12px"></p>
      <div class="row" style="margin-top:12px"><button id="ov2-ctl-light">Toggle light</button>
        <span id="ov2-ctl-lightstate" class="sub"></span></div>
      <p class="note" style="margin-top:12px">Print speed</p>
      <div class="row" id="ov2-ctl-speeds">
        <button data-speed="50" data-name="Quiet">Quiet</button>
        <button data-speed="75" data-name="Slower">Slower</button>
        <button data-speed="100" data-name="Normal">Normal</button>
        <button data-speed="125" data-name="Faster">Faster</button>
        <button data-speed="150" data-name="Ludicrous">Ludicrous</button>
      </div>
      <p class="note" style="margin-top:10px">Current: <span id="ov2-ctl-speedcur">—</span></p>
      <div style="margin:10px 0"><span class="sub">Model fan: <b id="ov2-fanm-val">—</b></span>
        <input type="range" id="ov2-fanm" min="0" max="100" step="1"></div>
      <div style="margin:10px 0"><span class="sub">Aux fan: <b id="ov2-fana-val">—</b></span>
        <input type="range" id="ov2-fana" min="0" max="100" step="1"></div>
      <div style="margin:10px 0"><span class="sub">Box fan: <b id="ov2-fanb-val">—</b></span>
        <input type="range" id="ov2-fanb" min="0" max="100" step="1"></div>
      <p class="note">Fan changes send when you release the slider.</p>
    </div>
  </div>
</section>

<section class="page" id="page-options">
  <div class="grid">
    <div class="tile"><h2>Printer connection</h2>
      <div class="row"><input type="text" id="opt-ip" placeholder="Printer IP address">
        <button id="opt-ipsave">Save IP</button></div>
      <p class="note" id="opt-ipmsg" style="margin-top:10px"></p>
      <p class="note">Saving an IP reconnects the dashboard to that printer and remembers it. Leaving it blank keeps auto-discovery.</p></div>
    <div class="tile"><h2>Polling</h2>
      <label class="opt">Refresh while printing, every
        <input type="number" id="opt-poll" min="1" max="10" step="1" style="width:70px"> seconds</label>
      <p class="note">Idle refresh stays at 5 seconds. Faster polling makes gauges and charts feel more live.</p></div>
    <div class="tile"><h2>Sound and tunes</h2>
      <label class="opt"><input type="checkbox" id="opt-tune-start"> Start-of-print jingle</label>
      <label class="opt"><input type="checkbox" id="opt-tune-end"> End-of-print tune (replaces the old chime)</label>
      <p class="note">The tunes obey the speaker button in the header. Browsers only allow sound after you have clicked somewhere on the page once. The end tune only plays when a print finishes — never for a stopped print.</p></div>
    <div class="tile"><h2>Chamber light</h2>
      <p class="note">The light is controlled by the slicer start/end G-code now (M8213 on, M8212 off). Use the light tile to toggle it by hand.</p>
      <label class="opt"><input type="checkbox" id="opt-cam"> Show camera on Printer</label></div>
    <div class="tile"><h2>3D model search</h2>
      <label class="opt"><input type="checkbox" id="opt-freeonly"> Free prints only</label>
      <p class="note">When on, paid designs are removed from the search results entirely. Cults3D search needs a free API key (cults3d.com/en/api/keys) — until one is added to the shared config, Cults3D shows a link out instead of results.</p></div>
    <div class="tile"><h2>Layout</h2>
      <p class="note">Tiles are movable and stretchable while the layout is unlocked — drag a tile by its heading, stretch it from the corner grip. Use the lock button in the top bar to lock or unlock.</p>
      <button id="opt-layoutreset">Reset tile layout</button>
      <button id="opt-reset" style="margin-left:8px">Reset saved options</button>
      <p class="note" id="opt-resetmsg" style="margin-top:10px"></p></div>
    <div class="tile"><h2>About</h2>
      <p class="note">Printer-only dashboard, SDCP V3, pure Python standard library. Build: <span id="opt-build"></span></p>
      <p class="note">Keyboard shortcuts: H home · P printer · S search · F filament · M maintenance · O options. Shortcuts do not fire while typing in a field.</p></div>
  </div>
</section>
<section class="page" id="page-home">
  <div class="grid">
    <div class="tile stat-tile temp-tile" data-temp="nozzle" id="hm-t-noz" title="Tap to change the nozzle target temperature">
      <h2>Nozzle temp</h2>
      <div class="big" id="hm-t-noz-v">—</div>
      <div class="sub">target <span id="hm-t-noz-t">—</span></div>
      <div class="temp-editor" hidden></div>
    </div>
    <div class="tile stat-tile temp-tile" data-temp="bed" id="hm-t-bed" title="Tap to change the bed target temperature">
      <h2>Bed temp</h2>
      <div class="big" id="hm-t-bed-v">—</div>
      <div class="sub">target <span id="hm-t-bed-t">—</span></div>
      <div class="temp-editor" hidden></div>
    </div>
    <div class="tile stat-tile" id="hm-t-box">
      <h2>Chamber</h2>
      <div class="big" id="hm-t-box-v">—</div>
      <div class="sub">target <span id="hm-t-box-t">—</span></div>
    </div>
    <div class="tile stat-tile light-tile" id="hm-t-light" tabindex="0" role="button" title="Tap to turn the chamber light on or off">
      <h2>Chamber light</h2>
      <div class="big" id="hm-t-light-v">—</div>
      <div class="sub" id="hm-t-light-s">Tap to toggle</div>
    </div>
    <div class="tile wide"><h2>Print</h2>
      <div class="row"><span id="hm-file" class="big">—</span>
        <span id="hm-state" class="sub"></span></div>
      <div class="pbar" style="margin:12px 0 6px"><div id="hm-prog"></div></div>
      <div class="row"><span id="hm-pct" class="sub"></span>
        <span id="hm-layer" class="sub"></span>
        <span id="hm-time" class="sub"></span></div>
      <p class="note" id="hm-printernote"></p></div>
    <div class="tile wide"><h2>Camera</h2>
      <div class="camwrap" id="hm-camwrap">
        <img id="hm-cam" alt="Printer camera" title="Click to enlarge">
      </div>
      <p class="note" id="hm-camnote" style="display:none"></p></div>
    <div class="tile wide"><h2>CANVAS trays</h2>
      <div id="hm-fi-slots"></div>
      <p class="note" id="hm-fi-refill"></p>
      <p class="note" id="hm-fi-note" style="display:none"></p></div>
  </div>
</section>

<section class="page" id="page-search">
  <div class="grid">
    <div class="tile wide"><h2>3D model search</h2>
      <div class="row"><input type="text" id="se3-q" placeholder="Search MakerWorld, Printables, Thingiverse…" style="flex:1;min-width:200px">
        <button id="se3-go">Search</button></div>
      <div class="row" id="se3-sites" style="margin-top:10px;gap:8px">
        <button class="chip on" data-site="makerworld">MakerWorld</button>
        <button class="chip on" data-site="printables">Printables</button>
        <button class="chip on" data-site="thingiverse">Thingiverse</button>
        <button class="chip on" data-site="cults3d">Cults3D</button>
      </div>
      <p class="note" id="se3-note" style="margin-top:10px">Searches the model sites — each result shows a picture, the model name and a link to get the STL.</p>
      <p class="note" id="se3-count"></p>
      <div id="se3-results" class="search-grid" style="margin-top:10px"></div></div>
  </div>
</section>

<section class="page" id="page-filament">
  <div class="grid">
    <div class="tile wide"><h2>Filament</h2>
      <p class="note">One home for your spools, grouped by brand, material and colour — combined weight and spool count per group.</p>
      <div id="fi-groups"></div></div>
    <div class="tile"><h2>Add a spool</h2>
      <div style="margin:8px 0"><span class="sub">Brand</span><br>
        <input type="text" id="fi-brand" placeholder="e.g. Inland" style="width:100%"></div>
      <div style="margin:8px 0"><span class="sub">Material</span><br>
        <select id="fi-material" style="width:100%"><option>PLA</option><option>PETG</option><option>ABS</option><option>ASA</option><option>TPU</option><option>PLA-CF</option><option>PETG-CF</option><option>Other</option></select></div>
      <div style="margin:8px 0"><span class="sub">Spool colour — snap a photo of the spool</span><br>
        <input type="file" id="fi-photo" accept="image/*" capture="environment">
        <canvas id="fi-photocanvas" style="display:none"></canvas>
        <div class="row" style="margin-top:8px;gap:8px;align-items:center"><span class="chip" id="fi-swatch" style="background:#888888;min-width:34px">&nbsp;</span>
        <input type="text" id="fi-color" placeholder="Colour name" style="flex:1;min-width:120px">
        <input type="color" id="fi-colorpick" value="#888888" title="Pick the colour"></div>
        <p class="note">The photo is sampled for its colour and the name is guessed — correct it if the light fooled it.</p></div>
      <div style="margin:8px 0"><span class="sub">Weight on the spool (g)</span><br>
        <input type="number" id="fi-grams" value="1000" min="0" step="1" style="width:120px"></div>
      <div class="row" style="margin-top:10px"><button id="fi-add">Add spool</button>
        <span class="note" id="fi-addmsg"></span></div></div>
  </div>
</section>

<section class="page" id="page-maintenance">
  <div class="grid">
    <div class="tile wide"><h2>Maintenance</h2>
      <p class="note" id="mt-summary">—</p>
      <p class="note">Total print hours (from the printer's history): <b id="mt-hours">—</b></p>
      <div id="mt-cards" class="maint-grid"></div>
      <p class="note">Tap a task title to open the Elegoo how-to on the Centauri Carbon wiki. Tap Mark done after you do a task — the next due date is worked out from its interval.</p></div>
  </div>
</section>
</main>
<footer><span id="build"></span><span>SDCP · standalone · port 8092</span></footer>
<div id="camzoom" title="Click to close"><img id="camzoom-img" alt="Printer camera enlarged"></div>
<script>
function $(id){ return document.getElementById(id); }
function lsGet(k, d){ try { var v = localStorage.getItem(k); return v === null ? d : v; } catch(e){ return d; } }
function lsSet(k, v){ try { localStorage.setItem(k, v); } catch(e){} }
function fmtDur(s){
  s = Math.round(s || 0);
  if (s < 60) return s + "s";
  if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s";
  return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m";
}
function fmtDate(epoch){
  if (!epoch) return "—";
  return new Date(epoch * 1000).toLocaleString();
}
function heatState(cur, tgt){
  if (cur === null || cur === undefined) return "off";
  tgt = tgt || 0;
  if (tgt > 0){
    var c = Math.round(cur), t = Math.round(tgt);
    if (c < t) return "heat";
    if (c > t) return "over";
    return "at";
  }
  return cur > 40 ? "cool" : "off";
}
var TEMP_COLORS = {heat: "#ff5347", at: "#35d07f", over: "#57a8ff",
                   cool: "#57a8ff", off: ""};
var ACTIVE_PRINT = {Printing: 1, Pausing: 1, Paused: 1, "Starting print": 1};
var tempPrev = {};
var tempFlashAt = {};
var printActive = false;
function paintTemp(id, bid, cls){
  var el = $(id), bar = $(bid);
  el.className = cls === "off" ? "" :
    "th-" + cls + (cls === "heat" && printActive ? " nopulse" : "");
  el.style.color = TEMP_COLORS[cls] || "";
  var nowMs = Date.now();
  if (cls === "at" && (tempPrev[id] === "heat" || tempPrev[id] === "over") &&
      nowMs - (tempFlashAt[id] || 0) > 60000){
    tempFlashAt[id] = nowMs;
    el.classList.add("th-flash");
    setTimeout(function(){ el.classList.remove("th-flash"); }, 1500);
  }
  tempPrev[id] = cls;
}
function tempBarWidth(cur, tgt, scale){
  if (cur === null || cur === undefined) return 0;
  if (tgt > 0) return Math.min(100, Math.max(0, cur / tgt * 100));
  return Math.min(100, Math.max(0, cur / scale * 100));
}
function showTemp(id, bid, cur, tgt, scale){
  var el = $(id);
  if (cur === null || cur === undefined){ el.textContent = "—"; $(bid).style.width = "0%"; paintTemp(id, bid, "off"); return; }
  el.textContent = Math.round(cur) + "° / " + Math.round(tgt || 0) + "°";
  $(bid).style.width = tempBarWidth(cur, tgt, scale) + "%";
  $(bid).style.background = TEMP_COLORS[heatState(cur, tgt)] || "";
  paintTemp(id, bid, heatState(cur, tgt));
}

// --- chime ---
var audioCtx = null;
function ensureAudio(){
  if (!audioCtx){
    try { audioCtx = new (window.AudioContext || window.webkitAudioContext)(); } catch(e){}
  }
  if (audioCtx && audioCtx.state === "suspended") audioCtx.resume();
}
document.addEventListener("pointerdown", ensureAudio, {once: false});
function playTone(freq, t0, dur, type, vol){
  if (!audioCtx) return;
  var osc = audioCtx.createOscillator(), gain = audioCtx.createGain();
  osc.type = type || "triangle"; osc.frequency.value = freq;
  gain.gain.setValueAtTime(0.0001, t0);
  gain.gain.exponentialRampToValueAtTime(vol || 0.22, t0 + 0.02);
  gain.gain.exponentialRampToValueAtTime(0.0001, t0 + dur);
  osc.connect(gain); gain.connect(audioCtx.destination);
  osc.start(t0); osc.stop(t0 + dur + 0.05);
}
// Start-of-print jingle: upbeat "giddy up and go" run. Sam hears it first
// and can ask for a swap.
function playStartJingle(){
  if (lsGet("pd_multi_muted", "0") === "1") return;
  if (lsGet("pd_multi_tune_start", "1") !== "1") return;
  ensureAudio(); if (!audioCtx) return;
  var notes = [523.25, 659.25, 783.99, 1046.5, 783.99, 1046.5, 1318.5];
  var t0 = audioCtx.currentTime + 0.02;
  notes.forEach(function(f, i){ playTone(f, t0 + i * 0.11, 0.22, "triangle", 0.22); });
  playTone(261.63, t0, 0.5, "sine", 0.12);
}
// End-of-print tune: a cute little tune replacing the old plain chime.
// Only ever plays when a print finishes — never for a stopped print.
function playEndTune(){
  if (lsGet("pd_multi_muted", "0") === "1") return;
  if (lsGet("pd_multi_tune_end", "1") !== "1") return;
  ensureAudio(); if (!audioCtx) return;
  var notes = [659.25, 783.99, 880, 1046.5, 880, 783.99];
  var t0 = audioCtx.currentTime + 0.02;
  notes.forEach(function(f, i){ playTone(f, t0 + i * 0.16, 0.32, "sine", 0.2); });
}

function refreshMuteBtn(){
  $("mutebtn").textContent = lsGet("pd_multi_muted", "0") === "1" ? "🔇" : "🔊";
}
$("mutebtn").onclick = function(){
  var muted = lsGet("pd_multi_muted", "0") === "1";
  lsSet("pd_multi_muted", muted ? "0" : "1");
  refreshMuteBtn();
  if (muted) playEndTune();
};
refreshMuteBtn();

// --- router ---
var PAGES = ["home", "printer", "search", "filament", "maintenance", "options"];
// camera retry state (top level: shared by Overview + Home tiles)
var camRetry = {};
function camRetryImg(img, note){
  var st = camRetry[img.id] || (camRetry[img.id] = {n: 0, timer: null});
  if (st.timer) return;
  var delays = [3000, 5000, 10000];
  var dly = delays[Math.min(st.n, delays.length - 1)];
  st.n++;
  if (note){ note.style.display = "block"; note.textContent = "Camera reconnecting…"; }
  st.timer = setTimeout(function(){
    st.timer = null;
    img.setAttribute("src", "/camera?t=" + Date.now());
  }, dly);
}
function camRetryClear(img, note){
  var st = camRetry[img.id];
  if (st){ if (st.timer){ clearTimeout(st.timer); st.timer = null; } st.n = 0; }
  if (note) note.style.display = "none";
}
var currentPage = "home";
function showPage(name){
if (name === "overview") name = "printer";
  if (name === "gauges") name = "printer";
  if (PAGES.indexOf(name) < 0) name = "printer";
  currentPage = name;
  pageGrids($('page-' + name)).forEach(function(g){ packGrid(g); });
  PAGES.forEach(function(p){
    $("page-" + p).className = "page" + (p === name ? " on" : "");
  });
  var links = document.querySelectorAll("#nav a");
  links.forEach(function(a){
    a.className = a.getAttribute("data-page") === name ? "on" : "";
  });
  if (name === "printer"){ loadRaw(); drawAll(); }
  if (name === "home"){ loadRaw(); }
  if (name === "filament"){ renderFilamentTab(); }
  if (name === "maintenance"){ renderMaintenance(); }
}

window.addEventListener("hashchange", function(){
  showPage(location.hash.replace("#/", ""));
});
function goDefault(){
  var h = location.hash.replace("#/", "");
  showPage(h || "home");
}

// --- samples (rolling 30-minute buffer) ---
var samples = [];
var SAMPLE_AGE = 30 * 60 * 1000;
function pushSample(d){
  samples.push({t: Date.now(), noz: d.nozzle, bed: d.bed, box: d.box,
    fanm: d.fans ? d.fans.model : null, fana: d.fans ? d.fans.aux : null,
    fanb: d.fans ? d.fans.box : null, speed: d.print_speed, prog: d.progress});
  var cutoff = Date.now() - SAMPLE_AGE;
  while (samples.length && samples[0].t < cutoff) samples.shift();
}

// --- canvas helpers ---
function fitCanvas(cv, h){
  var dpr = window.devicePixelRatio || 1;
  var w = cv.clientWidth || cv.width;
  var hh = h || cv.clientHeight || 150;
  if (cv.width !== Math.round(w * dpr) || cv.height !== Math.round(hh * dpr)){
    cv.width = Math.round(w * dpr); cv.height = Math.round(hh * dpr);
  }
  var ctx = cv.getContext("2d");
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return ctx;
}
function drawGauge(cv, value, max, color){
  var ctx = fitCanvas(cv, 150);
  var w = cv.clientWidth || 240, h = 150;
  var cx = w / 2, cy = h - 18, r = Math.min(w / 2 - 12, 100);
  var a0 = Math.PI * 0.75, a1 = Math.PI * 2.25;
  ctx.lineWidth = 14; ctx.lineCap = "round";
  ctx.strokeStyle = "#22303f";
  ctx.beginPath(); ctx.arc(cx, cy, r, a0, a1); ctx.stroke();
  var frac = (value === null || value === undefined) ? 0 :
    Math.min(1, Math.max(0, value / max));
  if (frac > 0){
    ctx.strokeStyle = color || "#4da3ff";
    ctx.beginPath(); ctx.arc(cx, cy, r, a0, a0 + (a1 - a0) * frac); ctx.stroke();
  }
  ctx.fillStyle = "#8fa3ba"; ctx.font = "11px sans-serif"; ctx.textAlign = "center";
  ctx.fillText("0", cx - r * 0.92, cy + 14);
  ctx.fillText(String(max), cx + r * 0.92, cy + 14);
}
function drawRing(cv, pct, color){
  var ctx = fitCanvas(cv, 200);
  var w = cv.clientWidth || 200, h = 200;
  var cx = w / 2, cy = h / 2, r = Math.min(w, h) / 2 - 14;
  ctx.lineWidth = 16; ctx.lineCap = "round";
  ctx.strokeStyle = "#22303f";
  ctx.beginPath(); ctx.arc(cx, cy, r, 0, Math.PI * 2); ctx.stroke();
  var frac = (pct === null || pct === undefined) ? 0 : Math.min(1, Math.max(0, pct / 100));
  if (frac > 0){
    ctx.strokeStyle = color || "#3fb970";
    ctx.beginPath(); ctx.arc(cx, cy, r, -Math.PI / 2, -Math.PI / 2 + Math.PI * 2 * frac); ctx.stroke();
  }
  // The % is HTML overlaid on the ring now (see .ring-center): drawing it
  // inside the canvas is what let it get half-covered in v17.
}
function drawChart(cv, series, fixedMax, noteId){
  var ctx = fitCanvas(cv, 220);
  var w = cv.clientWidth || 800, h = 220;
  var padL = 44, padR = 12, padT = 12, padB = 22;
  var plotW = w - padL - padR, plotH = h - padT - padB;
  ctx.clearRect(0, 0, w, h);
  var vals = [];
  series.forEach(function(s){ s.data.forEach(function(v){ if (v !== null && v !== undefined) vals.push(v); }); });
  if (vals.length < 2){ $(noteId).textContent = "Collecting samples — charts fill in as the dashboard polls."; return; }
  $(noteId).textContent = "";
  var min = Math.min.apply(null, vals), max = Math.max.apply(null, vals);
  if (fixedMax !== null){ min = 0; max = fixedMax; }
  else { var span = max - min; if (span < 10) span = 10; min = Math.floor(min - span * 0.1); max = Math.ceil(max + span * 0.1); }
  if (max === min) max = min + 1;
  var t0 = samples[0].t, t1 = samples[samples.length - 1].t;
  if (t1 === t0) t1 = t0 + 1;
  ctx.strokeStyle = "#22303f"; ctx.lineWidth = 1;
  ctx.fillStyle = "#8fa3ba"; ctx.font = "11px sans-serif"; ctx.textAlign = "right";
  for (var g = 0; g <= 4; g++){
    var y = padT + plotH * g / 4;
    var gv = max - (max - min) * g / 4;
    ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(w - padR, y); ctx.stroke();
    ctx.fillText(String(Math.round(gv)), padL - 6, y + 4);
  }
  ctx.textAlign = "center";
  ctx.fillText("30m ago", padL + 20, h - 6);
  ctx.fillText("now", w - padR - 14, h - 6);
  series.forEach(function(s){
    ctx.strokeStyle = s.color; ctx.lineWidth = 2; ctx.beginPath();
    var started = false;
    s.data.forEach(function(v, i){
      if (v === null || v === undefined) return;
      var x = padL + plotW * (samples[i].t - t0) / (t1 - t0);
      var y = padT + plotH * (1 - (v - min) / (max - min));
      if (!started){ ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });
}
function seriesFor(key){
  return samples.map(function(s){ return s[key]; });
}
function drawAll(){
  var d = lastStatus;
  if (!d) return;
  // Gauges and charts render on the Printer command center (ov2- prefix).
  var gp = null, cp = null;
  if (currentPage === "printer"){ gp = "ov2-g-"; cp = "ov2-c-"; }
  if (gp){
    drawGauge($(gp + "noz"), d.nozzle, 300, TEMP_COLORS[heatState(d.nozzle, d.nozzle_target)] || "#4da3ff");
    drawGauge($(gp + "bed"), d.bed, 120, TEMP_COLORS[heatState(d.bed, d.bed_target)] || "#4da3ff");
    drawGauge($(gp + "box"), d.box, 80, TEMP_COLORS[heatState(d.box, d.box_target)] || "#4da3ff");
    drawRing($(gp + "prog"), d.progress, "#3fb970");
    drawGauge($(gp + "fanm"), d.fans ? d.fans.model : null, 100, "#4da3ff");
    drawGauge($(gp + "fana"), d.fans ? d.fans.aux : null, 100, "#4da3ff");
    drawGauge($(gp + "fanb"), d.fans ? d.fans.box : null, 100, "#4da3ff");
    // Head-speed gauge shows ONLY for a genuine mm/s value. The printer
    // often reports only a % feed-rate value, which is not a head speed,
    // so the whole gauge box hides in that case (and the arc is cleared
    // so nothing stale lingers). It returns on its own if mm/s arrives.
    var spdBox = $("ov2-gb-speed");
    var hasMmSpeed = (d.print_speed !== null && d.print_speed !== undefined && d.speed_unit === "mm/s");
    if (spdBox) spdBox.style.display = hasMmSpeed ? "" : "none";
    if (hasMmSpeed){
      drawGauge($(gp + "speed"), d.print_speed, 300, "#b07fd4");
    } else {
      var spdCv = $(gp + "speed");
      if (spdCv) spdCv.getContext("2d").clearRect(0, 0, spdCv.width, spdCv.height);
    }
  }
  if (cp){
    drawChart($(cp + "temp"), [
      {data: seriesFor("noz"), color: "#ff5347"},
      {data: seriesFor("bed"), color: "#57a8ff"},
      {data: seriesFor("box"), color: "#e8a33d"}], null, cp + "tempnote");
    drawChart($(cp + "fan"), [
      {data: seriesFor("fanm"), color: "#4da3ff"},
      {data: seriesFor("fana"), color: "#35d07f"},
      {data: seriesFor("fanb"), color: "#e8a33d"}], 100, cp + "fannote");
    drawChart($(cp + "speed"), [
      {data: seriesFor("speed"), color: "#b07fd4"}], null, cp + "speednote");
  }
  drawMiniCharts(d);
}

// --- render + poll ---
var lastStatus = null;
var lastHistory = [];
var prevPrintState = "";
var prevProgress = 0;
var lastChimedTask = "";
// Print-state transitions: start jingle + end tune only.
// Chamber light is owned by the slicer start/end G-code now (M8213 on,
// M8212 off) — the dashboard never touches the light on print changes.
function maybeTunes(d){
  var st = d.print_state || "";
  var task = d.task_id || "";
  if (st !== prevPrintState){
    if (st === "Printing" && !ACTIVE_PRINT[prevPrintState]){
      playStartJingle();
    } else if (st === "Complete" && ACTIVE_PRINT[prevPrintState] && task !== lastChimedTask){
      lastChimedTask = task; playEndTune();
    } else if (d.progress !== null && d.progress >= 99.5 && ACTIVE_PRINT[prevPrintState] && task !== lastChimedTask){
      lastChimedTask = task; playEndTune();
    }
  }
  prevPrintState = st;
  prevProgress = d.progress;
}

function badgeClass(st){
  if (st === "Printing") return "badge printing";
  if (st === "Paused" || st === "Pausing") return "badge paused";
  return "badge";
}
function render(d){
  lastStatus = d;
  printActive = (d.print_state === "Printing" || d.print_state === "Paused" || d.print_state === "Pausing");
  $("dot").className = "dot" + (d.online ? " on" : "");
  $("mach").textContent = (d.machine_name || "") + (d.firmware ? " · " + d.firmware : "");
  var bd = $("statebadge");
  bd.textContent = d.print_state || d.machine_state || (d.online ? "Online" : "Offline");
  bd.className = badgeClass(d.print_state);
  $("build").textContent = d.build || "";
  $("opt-build").textContent = d.build || "";
  $("ipbox").style.display = d.needs_ip ? "block" : "none";
  if (document.activeElement !== $("ipinput") && d.printer_ip) $("ipinput").value = d.printer_ip;
  if (document.activeElement !== $("opt-ip") && d.printer_ip) $("opt-ip").value = d.printer_ip;
  // printer
  $("ov-file").textContent = d.filename || "—";
  $("ov-state").textContent = (d.print_state || "") + (d.task_id ? " · " + d.task_id : "");
  var pct = (d.progress === null || d.progress === undefined) ? 0 : d.progress;
  $("ov-prog").style.width = Math.min(100, Math.max(0, pct)) + "%";
  $("ov-pct").textContent = Math.round(pct) + "%";
  $("ov-layer").textContent = (d.current_layer !== null && d.current_layer !== undefined && d.total_layer)
    ? ("Layer " + d.current_layer + " / " + d.total_layer) : "";
  $("ov-time").textContent = (d.elapsed_s ? ("Elapsed " + fmtDur(d.elapsed_s)) : "") +
    (d.remaining_s ? (" · Remaining " + fmtDur(d.remaining_s)) : "");
  $("ov-speed").textContent = (d.print_speed !== null && d.print_speed !== undefined)
    ? ("Speed " + d.print_speed + (d.speed_unit || "%")) : "";
  showTemp("ov-noz", "ov-nozb", d.nozzle, d.nozzle_target, 300);
  showTemp("ov-bed", "ov-bedb", d.bed, d.bed_target, 120);
  showTemp("ov-box", "ov-boxb", d.box, d.box_target || 0, 60);
  // camera
  var camOn = lsGet("pd_multi_cam", "1") === "1";
  var camWrap = $("ov-camwrap"), camNote = $("ov-camnote");
  var cam = $("ov-cam");
  if (!camOn){ camWrap.style.display = "none"; camNote.style.display = "block"; camNote.textContent = "Camera is off — enable it in Options."; }
  else if (!d.online){ camWrap.style.display = "none"; camNote.style.display = "block"; camNote.textContent = "Camera unavailable while the printer is offline."; }
  else {
    camWrap.style.display = "block";
    if ((cam.getAttribute("src") || "").indexOf("/camera") !== 0) cam.setAttribute("src", "/camera");
    if (cam.naturalWidth > 0) camRetryClear(cam, camNote);
    else { var cst = camRetry[cam.id]; if (!cst || !cst.timer) camNote.style.display = "none"; }
  }
  cam.onerror = function(){ camRetryImg(cam, camNote); };
  cam.onload = function(){ camRetryClear(cam, camNote); };
  // gauge values (printer page primaries)
  $("ov2-gv-noz").textContent = (d.nozzle === null || d.nozzle === undefined) ? "—" : Math.round(d.nozzle) + "°";
  $("ov2-gv-noz").style.color = TEMP_COLORS[heatState(d.nozzle, d.nozzle_target)] || "";
  $("ov2-gt-noz").textContent = (d.nozzle_target === null || d.nozzle_target === undefined) ? "—" : Math.round(d.nozzle_target) + "°";
  $("ov2-gv-bed").textContent = (d.bed === null || d.bed === undefined) ? "—" : Math.round(d.bed) + "°";
  $("ov2-gv-bed").style.color = TEMP_COLORS[heatState(d.bed, d.bed_target)] || "";
  $("ov2-gt-bed").textContent = (d.bed_target === null || d.bed_target === undefined) ? "—" : Math.round(d.bed_target) + "°";
  $("ov2-gv-box").textContent = (d.box === null || d.box === undefined) ? "—" : Math.round(d.box) + "°";
  $("ov2-gt-box").textContent = (d.box_target ? Math.round(d.box_target) + "°" : "—");
  $("ov2-gv-prog").textContent = (d.progress === null || d.progress === undefined) ? "—" : Math.round(d.progress) + "%";
  $("ov2-gl-prog").textContent = d.filename || "";
  var f = d.fans || {};
  $("ov2-gv-fanm").textContent = (f.model === null || f.model === undefined) ? "—" : f.model + "%";
  $("ov2-gv-fana").textContent = (f.aux === null || f.aux === undefined) ? "—" : f.aux + "%";
  $("ov2-gv-fanb").textContent = (f.box === null || f.box === undefined) ? "—" : f.box + "%";
  $("ov2-gv-speed").textContent = (d.print_speed === null || d.print_speed === undefined) ? "—" : d.print_speed + (d.speed_unit || "%");
  // home stat tiles (nozzle / bed / chamber / chamber light)
  var lightOn = (d.light_on === 1 || d.light_on === true);
  $("hm-t-noz-v").textContent = (d.nozzle === null || d.nozzle === undefined) ? "—" : Math.round(d.nozzle) + "°";
  $("hm-t-noz-v").style.color = TEMP_COLORS[heatState(d.nozzle, d.nozzle_target)] || "";
  $("hm-t-noz-t").textContent = (d.nozzle_target === null || d.nozzle_target === undefined) ? "—" : Math.round(d.nozzle_target) + "°";
  $("hm-t-bed-v").textContent = (d.bed === null || d.bed === undefined) ? "—" : Math.round(d.bed) + "°";
  $("hm-t-bed-v").style.color = TEMP_COLORS[heatState(d.bed, d.bed_target)] || "";
  $("hm-t-bed-t").textContent = (d.bed_target === null || d.bed_target === undefined) ? "—" : Math.round(d.bed_target) + "°";
  $("hm-t-box-v").textContent = (d.box === null || d.box === undefined) ? "—" : Math.round(d.box) + "°";
  $("hm-t-box-t").textContent = (d.box_target ? Math.round(d.box_target) + "°" : "—");
  $("hm-t-light-v").textContent = lightOn ? "Light on" : (d.light_on === 0 || d.light_on === false) ? "Light off" : "—";
  $("hm-t-light").classList.toggle("is-off", !lightOn);
  // home print tile
  $("hm-file").textContent = d.filename || "—";
  $("hm-state").textContent = (d.print_state || "") + (d.task_id ? " · " + d.task_id : "");
  $("hm-prog").style.width = Math.min(100, Math.max(0, pct)) + "%";
  $("hm-pct").textContent = Math.round(pct) + "%";
  $("hm-layer").textContent = (d.current_layer !== null && d.current_layer !== undefined && d.total_layer)
    ? ("Layer " + d.current_layer + " / " + d.total_layer) : "";
  $("hm-time").textContent = (d.elapsed_s ? ("Elapsed " + fmtDur(d.elapsed_s)) : "") +
    (d.remaining_s ? (" · Remaining " + fmtDur(d.remaining_s)) : "");
  // home camera (same feed as the printer page; dims when the light is off)
  var hmCam = $("hm-cam"), hmWrap = $("hm-camwrap"), hmNote = $("hm-camnote");
  if (!camOn){ hmWrap.style.display = "none"; hmNote.style.display = "block"; hmNote.textContent = "Camera is off — enable it in Options."; }
  else if (!d.online){ hmWrap.style.display = "none"; hmNote.style.display = "block"; hmNote.textContent = "Camera unavailable while the printer is offline."; }
  else {
    hmWrap.style.display = "block";
    if ((hmCam.getAttribute("src") || "").indexOf("/camera") !== 0) hmCam.setAttribute("src", "/camera");
    if (hmCam.naturalWidth > 0) camRetryClear(hmCam, hmNote);
    else { var hst = camRetry[hmCam.id]; if (!hst || !hst.timer) hmNote.style.display = "none"; }
  }
  hmCam.onerror = function(){ camRetryImg(hmCam, hmNote); };
  hmCam.onload = function(){ camRetryClear(hmCam, hmNote); };
  hmWrap.classList.toggle("lights-off", !lightOn);
  camWrap.classList.toggle("lights-off", !lightOn);
  // controls state (printer page is primary; home mirrors it)
  $("ov2-ctl-lightstate").textContent = (d.light_on === 1 || d.light_on === true) ? "Light is on" :
    (d.light_on === 0 || d.light_on === false) ? "Light is off" : "";
  var spdNames = {50: "Quiet", 75: "Slower", 100: "Normal", 125: "Faster", 150: "Ludicrous"};
  var spdTxt = (d.print_speed === null || d.print_speed === undefined) ? "—" : d.print_speed + (d.speed_unit || "%");
  if (d.speed_unit === "%" && d.print_speed !== null && d.print_speed !== undefined && spdNames[Math.round(d.print_speed)]) spdTxt = spdNames[Math.round(d.print_speed)] + " · " + spdTxt;
  $("ov2-ctl-speedcur").textContent = spdTxt;
  var spBtns = document.querySelectorAll("#ov2-ctl-speeds button");
  spBtns.forEach(function(b){
    b.className = (d.speed_unit === "%" && d.print_speed !== null && d.print_speed !== undefined &&
      parseInt(b.getAttribute("data-speed"), 10) === Math.round(d.print_speed)) ? "on" : "";
  });
  [["fanm", "model"], ["fana", "aux"], ["fanb", "box"]].forEach(function(pair){
    var sl = $("ov2-" + pair[0]), val = f[pair[1]];
    if (document.activeElement !== sl && val !== null && val !== undefined) sl.value = val;
    $("ov2-" + pair[0] + "-val").textContent = (val === null || val === undefined) ? "—" : val + "%";
  });
  renderFilament(d.canvas);
  updateBadges();
  maybeTunes(d);
  pushSample(d);
  if (currentPage === "printer") drawAll();
}
function pollMs(){
  var v = parseInt(lsGet("pd_multi_poll", "1"), 10);
  if (isNaN(v) || v < 1) v = 1;
  if (v > 10) v = 10;
  return v * 1000;
}
function poll(){
  fetch("/api/status").then(function(r){ return r.json(); }).then(function(d){
    render(d);
    var printing = (d.print_state === "Printing");
    setTimeout(poll, printing ? pollMs() : 5000);
  }).catch(function(){
    setTimeout(poll, 5000);
  });
}

// --- history + stats ---
function loadHistory(){
  fetch("/api/history").then(function(r){ return r.json(); }).then(function(j){
    lastHistory = j.history || [];
    if (currentPage === "maintenance") renderMaintenance();
  }).catch(function(){});
}
function esc(s){
  return String(s === null || s === undefined ? "" : s).replace(/&/g, "&amp;")
    .replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// --- filament ---
function renderFilament(canvas){
  var html = "";
  var note = "";
  if (!canvas || !canvas.found || !canvas.slots || !canvas.slots.length){
    note = "No CANVAS trays reported yet — they appear here once the printer reports them.";
  } else {
    canvas.slots.forEach(function(s){
      var col = s.color || "#888888";
      var extra = "";
      if (s.active && _filamentSeed && _filamentSeed.length){
        extra = " · " + _filamentSeed[0].remainingG + " g left · printing now";
      }
      html += "<div class=\\"slot" + (s.active ? " active" : "") + "\\">" +
        "<span class=\\"chip\\" style=\\"background:" + col + "\\"></span>" +
        "<div><b>Slot " + s.slot + (s.active ? " · IN USE" : "") + "</b><br>" +
        "<span class=\\"sub\\">" + esc([s.brand, s.material, s.type].filter(function(x){ return x; }).join(" · ")) + extra + "</span></div>" +
        "<span class=\\"sub\\" style=\\"margin-left:auto\\">" + (s.loaded === true ? "Loaded" : s.loaded === false ? "Empty" : "") + "</span></div>";
    });
  }
  [["ov2-fi-slots", "ov2-fi-note"], ["hm-fi-slots", "hm-fi-note"]].forEach(function(pair){
    var box = $(pair[0]), nt = $(pair[1]);
    if (!box) return;
    box.innerHTML = html;
    if (nt){ nt.style.display = note ? "block" : "none"; nt.textContent = note; }
  });
}

function loadRaw(){
  fetch("/api/raw").then(function(r){ return r.json(); }).then(function(j){
    var raw = j.canvas_raw || {};
    var txt = "";
    if (raw.auto_refill === 1) txt = "Auto-refill: On";
    else if (raw.auto_refill === 0) txt = "Auto-refill: Off";
    var a = $("ov2-fi-refill"), b = $("hm-fi-refill");
    if (a) a.textContent = txt;
    if (b) b.textContent = txt;
  }).catch(function(){});
}

function postControl(body, done){
  fetch("/api/control", {method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body)}).then(function(r){ return r.json(); }).then(function(j){
    $("ov2-ctl-msg").textContent = j.message || (j.ok ? "Sent" : "Failed");
    $("hm-ctl-msg").textContent = $("ov2-ctl-msg").textContent;
    if (done) done(j);
  }).catch(function(){ $("ov2-ctl-msg").textContent = "Could not reach the dashboard."; $("hm-ctl-msg").textContent = $("ov2-ctl-msg").textContent; });
}
// home mirrors of the same controls (shared code path, not a second implementation)
$("ov2-ctl-pause").onclick = function(){ postControl({action: "pause"}); };
$("ov2-ctl-resume").onclick = function(){ postControl({action: "resume"}); };
$("ov2-ctl-stop").onclick = function(){
  if (confirm("Stop the current print?")) postControl({action: "stop", confirm: true});
};
$("ov2-ctl-light").onclick = function(){ postControl({action: "light"}); };
document.querySelectorAll("#ov2-ctl-speeds button").forEach(function(b){
  b.onclick = function(){ postControl({action: "speed", value: parseInt(b.getAttribute("data-speed"), 10)}); };
});
[["fanm", "model"], ["fana", "aux"], ["fanb", "box"]].forEach(function(pair){
  var sl = $("ov2-" + pair[0]);
  sl.addEventListener("input", function(){ $("ov2-" + pair[0] + "-val").textContent = sl.value + "%"; });
  sl.addEventListener("change", function(){
    postControl({action: "fan", fan: pair[1], value: parseInt(sl.value, 10)});
  });
});

// --- ip save (both boxes share one handler) ---
function saveIp(inp, msgEl){
  var ip = inp.value.trim();
  if (!ip) return;
  fetch("/api/set-ip", {method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({ip: ip})}).then(function(r){ return r.json(); }).then(function(j){
    msgEl.textContent = j.message || "";
  }).catch(function(){ msgEl.textContent = "Could not reach the dashboard."; });
}
$("ipsave").onclick = function(){ saveIp($("ipinput"), $("ipmsg")); };
$("opt-ipsave").onclick = function(){ saveIp($("opt-ip"), $("opt-ipmsg")); };

// --- options wiring ---
function initOptions(){
  var pollInp = $("opt-poll");
  pollInp.value = lsGet("pd_multi_poll", "1");
  pollInp.addEventListener("change", function(){
    var v = parseInt(pollInp.value, 10);
    if (isNaN(v) || v < 1) v = 1;
    if (v > 10) v = 10;
    pollInp.value = v;
    lsSet("pd_multi_poll", String(v));
  });
  var tuneStart = $("opt-tune-start");
  tuneStart.checked = lsGet("pd_multi_tune_start", "1") === "1";
  tuneStart.addEventListener("change", function(){ lsSet("pd_multi_tune_start", tuneStart.checked ? "1" : "0"); });
  var tuneEnd = $("opt-tune-end");
  tuneEnd.checked = lsGet("pd_multi_tune_end", "1") === "1";
  tuneEnd.addEventListener("change", function(){ lsSet("pd_multi_tune_end", tuneEnd.checked ? "1" : "0"); });
  var camBox = $("opt-cam");
  camBox.checked = lsGet("pd_multi_cam", "1") === "1";
  camBox.addEventListener("change", function(){
    lsSet("pd_multi_cam", camBox.checked ? "1" : "0");
    if (!camBox.checked){ camRetryClear($("ov-cam"), $("ov-camnote")); camRetryClear($("hm-cam"), $("hm-camnote")); $("ov-cam").removeAttribute("src"); $("hm-cam").removeAttribute("src"); }
  });
  var freeBox = $("opt-freeonly");
  freeBox.checked = lsGet("pd_multi_freeonly", "1") === "1";
  freeBox.addEventListener("change", function(){ lsSet("pd_multi_freeonly", freeBox.checked ? "1" : "0"); });
  $("opt-layoutreset").onclick = function(){
    try {
      var rm = [];
      for (var i = 0; i < localStorage.length; i++){
        var k = localStorage.key(i);
        if (k && k.indexOf(LAYOUT_PREFIX) === 0) rm.push(k);
      }
      rm.forEach(function(k){ localStorage.removeItem(k); });
    } catch(e){}
    location.reload();
  };
  $("opt-reset").onclick = function(){
    ["pd_multi_poll", "pd_multi_tune_start", "pd_multi_tune_end", "pd_multi_cam", "pd_multi_freeonly", "pd_multi_muted"].forEach(function(k){
      try { localStorage.removeItem(k); } catch(e){}
    });
    pollInp.value = "1";
    tuneStart.checked = true;
    tuneEnd.checked = true;
    camBox.checked = true;
    freeBox.checked = true;
    refreshMuteBtn();
    $("opt-resetmsg").textContent = "Saved options cleared — defaults restored.";
  };
}

function setBadge(id, text){
  var el = $(id);
  if (!el) return;
  if (text){ el.textContent = text; el.className = "navbadge show"; }
  else { el.textContent = ""; el.className = "navbadge"; }
}
function updateBadges(){
  var d = lastStatus;
  if (!d) return;
  var empty = 0;
  if (d.canvas && d.canvas.found && d.canvas.slots){
    d.canvas.slots.forEach(function(s){ if (s.loaded === false) empty++; });
  }
  setBadge("nb-tray", empty > 0 ? String(empty) : "");
  setBadge("nb-printer", d.print_state === "Complete" ? "!" : "");
}

// --- keyboard shortcuts + fullscreen ---
var KEYMAP = {h: "home", p: "printer", s: "search",
  f: "filament", m: "maintenance", o: "options"};
document.addEventListener("keydown", function(e){
  if (e.metaKey || e.ctrlKey || e.altKey) return;
  var tag = (document.activeElement && document.activeElement.tagName) || "";
  if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;
  var page = KEYMAP[e.key];
  if (page){ location.hash = "#/" + page; }
});
$("fsbtn").onclick = function(){
  if (document.fullscreenElement) document.exitFullscreen();
  else document.documentElement.requestFullscreen();
};

function toggleCamZoom(){
  var z = $("camzoom");
  if (z.classList.contains("on")){ z.classList.remove("on"); $("camzoom-img").removeAttribute("src"); }
  else { $("camzoom-img").setAttribute("src", "/camera?t=" + Date.now()); z.classList.add("on"); }
}
$("ov-cam").onclick = toggleCamZoom;
$("hm-cam").onclick = toggleCamZoom;
$("camzoom").onclick = toggleCamZoom;

goDefault();
initOptions();
poll();
loadHistory();

var LAYOUT_PREFIX = "pd_multi_layout_";
var suppressTileClick = false;
document.addEventListener("click", function(e){
  if (suppressTileClick){ suppressTileClick = false; e.stopPropagation(); e.preventDefault(); }
}, true);
function pageGrids(pageEl){
  var direct = pageEl.querySelectorAll(":scope > .grid");
  if (direct.length) return Array.prototype.slice.call(direct);
  var all = pageEl.querySelectorAll(".grid"), out = [];
  all.forEach(function(g){ if (!g.closest(".tile")) out.push(g); });
  return out;
}
function eachPageGrid(fn){
  document.querySelectorAll(".page").forEach(function(page){
    pageGrids(page).forEach(function(g){ fn(page.id.replace("page-", ""), g); });
  });
}
function gridTiles(grid){
  return Array.prototype.slice.call(grid.children).filter(function(el){
    return el.classList && el.classList.contains("tile");
  });
}
function loadLayout(pageName){
  try { return JSON.parse(localStorage.getItem(LAYOUT_PREFIX + pageName)) || null; }
  catch(e){ return null; }
}
function saveLayout(pageName, grid){
  var order = [], spans = {}, heights = {};
  gridTiles(grid).forEach(function(el){
    if (!el.dataset.lk) return;
    order.push(el.dataset.lk);
    if (el.dataset.cs) spans[el.dataset.lk] = [parseInt(el.dataset.cs, 10), 1];
    var hh = parseInt((el.style.height || "").replace("px", ""), 10);
    if (hh) heights[el.dataset.lk] = hh;
  });
  try { localStorage.setItem(LAYOUT_PREFIX + pageName, JSON.stringify({order: order, spans: spans, heights: heights})); } catch(e){}
}
var TILE_ROW_UNIT = 32;
function tileRowGap(grid){
  try { return parseFloat(getComputedStyle(grid).rowGap) || 16; } catch(e){ return 16; }
}
function spanFor(height, grid){
  var gap = tileRowGap(grid);
  return Math.max(1, Math.ceil((height + gap) / (TILE_ROW_UNIT + gap)));
}
function packGrid(grid){
  if (!grid) return;
  gridTiles(grid).forEach(function(el){
    if (el.classList.contains('tile-ghost')) return;
    var h = el.offsetHeight || parseInt((el.style.height || '').replace('px', ''), 10) || 110;
    el.style.gridRow = 'span ' + spanFor(h, grid);
  });
}
function applyLayout(pageName, grid){
  var saved = loadLayout(pageName);
  if (!saved) return;
  var byKey = {};
  gridTiles(grid).forEach(function(el){ if (el.dataset.lk) byKey[el.dataset.lk] = el; });
  var placed = {};
  // Pre-v6 Printer layouts saved the four separate control tiles; the
  // merged "Printer Controls" tile takes the first one's saved position
  // so an existing layout does not throw the controls to the end.
  var LEGACY_CTL = {"print-control": 1, "chamber-light": 1, "print-speed": 1, "fans": 1};
  (saved.order || []).forEach(function(k){
    if (LEGACY_CTL[k]){
      if (byKey["printer-controls"] && !placed["printer-controls"]){
        grid.appendChild(byKey["printer-controls"]); placed["printer-controls"] = true;
      }
      return;
    }
    if (byKey[k]){ grid.appendChild(byKey[k]); placed[k] = true; }
  });
  gridTiles(grid).forEach(function(el){
    if (el.dataset.lk && !placed[el.dataset.lk]) grid.appendChild(el);
  });
  Object.keys(saved.spans || {}).forEach(function(k){
    var el = byKey[k]; if (!el) return;
    var sp = saved.spans[k];
    el.style.gridColumn = "span " + sp[0];
    el.dataset.cs = sp[0];
  });
  // v9: heights are explicit pixels, not row spans. Old saved row spans
  // (spans[][1]) are deliberately ignored: on pages dense with full-width
  // tiles a row span could not grow a tile in place — grid auto-placement
  // moves the tile to the next spot with enough free tracks instead — which
  // is why Printer tiles "wouldn't resize" while Media Now tiles did.
  // Tiles return to natural height; re-stretch with the grip if wanted.
  Object.keys(saved.heights || {}).forEach(function(k){
    var el = byKey[k]; if (!el) return;
    el.style.height = saved.heights[k] + "px";
  });
}
function bindTileDrag(tile, grid, pageName){
  if (tile.dataset.dragBound) return;
  tile.dataset.dragBound = '1';
  tile.addEventListener('pointerdown', function(e){
    if (e.button !== undefined && e.button !== 0) return;
    var t = e.target;
    if (t && t.closest && t.closest('button,input,select,textarea,a,.resize-handle,img')) return;
    try { tile.setPointerCapture(e.pointerId); } catch(err){}
    var startX = e.clientX, startY = e.clientY, moved = false, ph = null;
    var rect = tile.getBoundingClientRect();
    var offX = startX - rect.left, offY = startY - rect.top, w = rect.width, ht = rect.height;
    var prevUserSelect = '';
    function onMove(ev){
      var dx = ev.clientX - startX, dy = ev.clientY - startY;
      if (!moved && Math.abs(dx) + Math.abs(dy) < 5) return;
      if (!moved){
        moved = true;
        prevUserSelect = document.body.style.userSelect;
        document.body.style.userSelect = 'none';
        ph = document.createElement('div');
        ph.className = 'tile tile-ghost';
        ph.style.height = ht + 'px';
        if (tile.classList.contains('wide') && !tile.dataset.cs) ph.style.gridColumn = '1 / -1';
        else ph.style.gridColumn = 'span ' + (tile.dataset.cs || '1');
        if (tile.style.gridRow) ph.style.gridRow = tile.style.gridRow;
        grid.insertBefore(ph, tile);
        tile.classList.add('dragging');
        tile.style.position = 'fixed';
        tile.style.left = (ev.clientX - offX) + 'px';
        tile.style.top = (ev.clientY - offY) + 'px';
        tile.style.width = w + 'px';
        tile.style.margin = '0';
        tile.style.pointerEvents = 'none';
        tile.style.zIndex = '1000';
      }
      tile.style.left = (ev.clientX - offX) + 'px';
      tile.style.top = (ev.clientY - offY) + 'px';
      // Geometry-based drop targeting: use the tiles real on-screen
      // rectangles, not a fixed row pitch. v9 assumed 110px rows, but
      // v9 tiles have explicit pixel heights, so the old math mapped the
      // cursor to the wrong row and drops landed in the wrong place.
      var others = gridTiles(grid).filter(function(el){ return el !== tile && el !== ph; }).map(function(el){
        var r = el.getBoundingClientRect();
        return {el: el, top: r.top, bottom: r.bottom, cx: r.left + r.width / 2, cy: r.top + r.height / 2};
      });
      others.sort(function(a, b){ return (a.top - b.top) || (a.cx - b.cx); });
      var target = null;
      for (var i = 0; i < others.length; i++){
        var o = others[i];
        if (ev.clientY < o.cy || (ev.clientY <= o.bottom && ev.clientX < o.cx)){ target = o.el; break; }
      }
      if (target) grid.insertBefore(ph, target);
      else grid.appendChild(ph);
    }
    function onUp(){
      tile.removeEventListener('pointermove', onMove);
      tile.removeEventListener('pointerup', onUp);
      tile.removeEventListener('pointercancel', onUp);
      document.body.style.userSelect = prevUserSelect;
      if (!moved) return;
      grid.insertBefore(tile, ph);
      ph.remove();
      tile.classList.remove('dragging');
      tile.style.position = ''; tile.style.left = ''; tile.style.top = '';
      tile.style.width = ''; tile.style.margin = ''; tile.style.pointerEvents = '';
      tile.style.zIndex = '';
      packGrid(grid);
      suppressTileClick = true;
      saveLayout(pageName, grid);
    }
    tile.addEventListener('pointermove', onMove);
    tile.addEventListener('pointerup', onUp);
    tile.addEventListener('pointercancel', onUp);
  });
}
function bindTileResize(tile, grid, pageName){
  if (tile.querySelector(".resize-handle")) return;
  var rh = document.createElement("div");
  rh.className = "resize-handle";
  rh.title = "Drag to resize tile (width and height)";
  tile.appendChild(rh);
  rh.addEventListener("pointerdown", function(e){
    if (e.button !== undefined && e.button !== 0) return;
    e.preventDefault(); e.stopPropagation();
    try { rh.setPointerCapture(e.pointerId); } catch(err){}
    var startX = e.clientX, startY = e.clientY;
    var cs = parseInt(tile.dataset.cs || "1", 10);
    var startH = tile.offsetHeight || 200;
    // Column pitch measured live (grid may be ultrawide or narrow) so one
    // step always equals one visible column. Height is explicit pixels, not
    // row spans: on pages dense with full-width tiles a row span could not
    // grow a tile in place (grid auto-placement moved it instead), which is
    // why Printer tiles "wouldn't resize" while Media Now tiles did.
    var cs0 = getComputedStyle(grid);
    var cols = (cs0.gridTemplateColumns === "none") ? 1 : cs0.gridTemplateColumns.split(" ").length;
    if (!cols) cols = 1;
    var colGap = parseFloat(cs0.columnGap) || 0;
    var colPitch = Math.max(80, (grid.clientWidth + colGap) / cols);
    // A full-width ("wide") tile starts at the full column count, so the
    // first drag does not snap it down to one column.
    if (tile.classList.contains("wide") && !tile.dataset.cs) cs = cols;
    var badge = document.createElement("div");
    badge.className = "rs-badge";
    tile.appendChild(badge);
    tile.classList.add("resizing");
    var prevUserSelect = document.body.style.userSelect;
    document.body.style.userSelect = "none";
    function show(nc, nh){
      badge.textContent = nc + ' \\u00d7 ' + nh + 'px';
      tile.style.gridColumn = 'span ' + nc;
      tile.style.height = nh + 'px';
      tile.dataset.cs = nc;
      tile.style.gridRow = 'span ' + spanFor(nh, grid);
    }
    show(cs, startH);
    function onMove(ev){
      var nc = Math.max(1, Math.min(6, Math.round(cs + (ev.clientX - startX) / colPitch)));
      var nh = Math.max(110, Math.min(1400, Math.round(startH + (ev.clientY - startY))));
      show(nc, nh);
    }
    function onUp(){
      rh.removeEventListener("pointermove", onMove);
      rh.removeEventListener("pointerup", onUp);
      rh.removeEventListener("pointercancel", onUp);
      if (badge.parentNode) badge.parentNode.removeChild(badge);
      tile.classList.remove("resizing");
      document.body.style.userSelect = prevUserSelect;
      saveLayout(pageName, grid);
    }
    rh.addEventListener("pointermove", onMove);
    rh.addEventListener("pointerup", onUp);
    rh.addEventListener("pointercancel", onUp);
  });
}
function initTileLayout(){
  eachPageGrid(function(pageName, grid){
    var used = {};
    gridTiles(grid).forEach(function(el){ if (el.dataset.lk) used[el.dataset.lk] = true; });
    gridTiles(grid).forEach(function(el){
      if (!el.dataset.lk){
        var h = el.querySelector("h2");
        var base = ((h ? h.textContent : "") || "tile").replace(/[^a-zA-Z0-9]+/g, "-").toLowerCase().replace(/^-+|-+$/g, "") || "tile";
        var key = base, n = 1;
        while (used[key]){ n++; key = base + "-" + n; }
        used[key] = true; el.dataset.lk = key;
      }
      bindTileDrag(el, grid, pageName);
      bindTileResize(el, grid, pageName);
    });
    applyLayout(pageName, grid);
    packGrid(grid);
  });
}
document.querySelectorAll(".tile h2").forEach(function(h){
  h.title = "Click to collapse / expand this section";
  h.addEventListener('click', function(){ h.parentElement.classList.toggle('collapsed'); var g = h.parentElement.parentElement; if (g && g.classList.contains('grid')) packGrid(g); });
});

// ------------------------------------------------------- v18: temp editors
// Tap a nozzle/bed temp tile (Home or Printer) to change its target while
// printing. The tile shows the printer's actual reported target back, so
// Sam can see whether the printer took it.
var TEMP_LIMITS = {nozzle: {min: 0, max: 320, label: "Nozzle"},
                   bed: {min: 0, max: 120, label: "Bed"}};
function closeTempEditors(){
  document.querySelectorAll(".temp-editor").forEach(function(ed){ ed.hidden = true; ed.innerHTML = ""; });
}
function openTempEditor(tile){
  var kind = tile.getAttribute("data-temp");
  var lim = TEMP_LIMITS[kind];
  if (!lim) return;
  closeTempEditors();
  var ed = tile.querySelector(".temp-editor");
  if (!ed) return;
  var cur = lastStatus ? (kind === "nozzle" ? lastStatus.nozzle_target : lastStatus.bed_target) : null;
  ed.innerHTML = '<div class="temp-editor-row"><label class="temp-editor-lab">Target °C<input type="number" min="' + lim.min + '" max="' + lim.max + '" step="1" value="' + (cur === null || cur === undefined ? "" : Math.round(cur)) + '"></label>' +
    '<button class="btn primary" type="button">Set target</button>' +
    '<button class="btn" type="button">Cancel</button></div>' +
    '<p class="temp-hint">Applies right away, while printing. The tile shows the printer\u2019s actual target back.</p>' +
    '<p class="temp-error" hidden></p>';
  ed.hidden = false;
  var input = ed.querySelector("input");
  var err = ed.querySelector(".temp-error");
  var btns = ed.querySelectorAll("button");
  btns[0].onclick = function(ev){
    ev.stopPropagation();
    var v = parseInt(input.value, 10);
    if (isNaN(v) || v < lim.min || v > lim.max){
      err.hidden = false; err.textContent = lim.label + " target must be " + lim.min + "–" + lim.max + " °C.";
      return;
    }
    var body = {action: "temp"};
    body[kind] = v;
    postControl(body, function(j){
      if (j && j.ok){ closeTempEditors(); }
      else { err.hidden = false; err.textContent = (j && j.message) || "The printer did not take it."; }
    });
  };
  btns[1].onclick = function(ev){ ev.stopPropagation(); closeTempEditors(); };
  input.onclick = function(ev){ ev.stopPropagation(); };
  input.focus(); input.select();
}
function bindTempTiles(){
  document.querySelectorAll(".temp-tile[data-temp]").forEach(function(tile){
    tile.addEventListener("click", function(e){
      if (e.target.closest("button,input,select,textarea,a")) return;
      var ed = tile.querySelector(".temp-editor");
      if (ed && !ed.hidden) return;
      openTempEditor(tile);
    });
  });
  document.addEventListener("click", function(e){
    if (!e.target.closest || !e.target.closest(".temp-tile")) closeTempEditors();
  });
}

// ------------------------------------------------------- v18: light tile
function bindLightTile(){
  var t = $("hm-t-light");
  if (!t) return;
  t.addEventListener("click", function(){ postControl({action: "light"}); });
  t.addEventListener("keydown", function(e){
    if (e.key === "Enter" || e.key === " "){ e.preventDefault(); postControl({action: "light"}); }
  });
}

// ------------------------------------------------------- v18: mini temp charts
// Compact 30-minute sparklines (inline SVG) inside the Printer temp tiles.
function drawMiniChart(svgId, key, target, color){
  var svg = $(svgId);
  if (!svg) return;
  var pts = samples.filter(function(s){ return s[key] !== null && s[key] !== undefined; });
  svg.innerHTML = "";
  if (pts.length < 2) return;
  var vals = pts.map(function(s){ return s[key]; });
  var min = Math.min.apply(null, vals), max = Math.max.apply(null, vals);
  if (target !== null && target !== undefined){ min = Math.min(min, target); max = Math.max(max, target); }
  if (max - min < 5){ max += 2.5; min -= 2.5; }
  var W = 100, H = 40, pad = 2;
  function X(i){ return pad + (W - pad * 2) * (pts.length === 1 ? 0 : i / (pts.length - 1)); }
  function Y(v){ return H - pad - (H - pad * 2) * (v - min) / (max - min); }
  var line = pts.map(function(s, i){ return (i ? "L" : "M") + X(i).toFixed(1) + " " + Y(s[key]).toFixed(1); }).join(" ");
  var area = line + " L" + X(pts.length - 1).toFixed(1) + " " + (H - pad) + " L" + X(0).toFixed(1) + " " + (H - pad) + " Z";
  var ns = "http://www.w3.org/2000/svg";
  var a = document.createElementNS(ns, "path");
  a.setAttribute("d", area); a.setAttribute("class", "tarea"); a.setAttribute("fill", color);
  var l = document.createElementNS(ns, "path");
  l.setAttribute("d", line); l.setAttribute("class", "tline"); l.setAttribute("stroke", color);
  svg.appendChild(a); svg.appendChild(l);
  if (target !== null && target !== undefined){
    var t = document.createElementNS(ns, "line");
    t.setAttribute("x1", pad); t.setAttribute("x2", W - pad);
    t.setAttribute("y1", Y(target)); t.setAttribute("y2", Y(target));
    t.setAttribute("class", "ttarget"); svg.appendChild(t);
  }
}
function drawMiniCharts(d){
  drawMiniChart("ov2-mc-noz", "noz", d.nozzle_target, TEMP_COLORS[heatState(d.nozzle, d.nozzle_target)] || "#4da3ff");
  drawMiniChart("ov2-mc-bed", "bed", d.bed_target, TEMP_COLORS[heatState(d.bed, d.bed_target)] || "#4da3ff");
  drawMiniChart("ov2-mc-box", "box", d.box_target || 0, "#57a8ff");
}

// ------------------------------------------------------- v18: 3D model search
function searchSitesEnabled(){
  var on = [];
  document.querySelectorAll("#se3-sites .chip").forEach(function(c){
    if (c.classList.contains("on")) on.push(c.getAttribute("data-site"));
  });
  return on;
}
function runSearch3d(){
  var q = ($("se3-q").value || "").trim();
  var res = $("se3-results"), count = $("se3-count"), note = $("se3-note");
  if (!q){ note.textContent = "Type something to search for."; res.innerHTML = ""; count.textContent = ""; return; }
  var sites = searchSitesEnabled();
  if (!sites.length){ note.textContent = "Turn at least one site chip on."; return; }
  var free = lsGet("pd_multi_freeonly", "1") === "1";
  note.textContent = "Searching…";
  res.innerHTML = ""; count.textContent = "";
  fetch("/api/search3d?q=" + encodeURIComponent(q) + "&sites=" + encodeURIComponent(sites.join(",")) + "&free=" + (free ? "1" : "0"))
    .then(function(r){ return r.json(); }).then(function(j){
      var cards = "";
      var shown = 0;
      (j.sites || []).forEach(function(rep){
        if (rep.site === "Cults3D" && !rep.ok){
          cards += '<div class="search-card"><div class="sc-body"><div class="sc-name">Cults3D</div>' +
            '<div class="sc-site">Cults3D search needs a free API key — until one is added, open it in a new tab:</div>' +
            '<a class="sc-link" target="_blank" rel="noopener" href="https://cults3d.com/en/search?q=' + encodeURIComponent(q) + '">Search Cults3D for \u201c' + esc(q) + '\u201d \u2197</a></div></div>';
          return;
        }
        if (!rep.ok){
          cards += '<div class="search-card"><div class="sc-body"><div class="sc-name">' + esc(rep.site) + '</div>' +
            '<div class="sc-site">' + esc(rep.error || "search failed") + '</div></div></div>';
          return;
        }
        (rep.results || []).forEach(function(r){
          shown++;
          cards += '<div class="search-card">' +
            (r.thumb ? '<img src="' + esc(r.thumb) + '" alt="" loading="lazy" onerror="this.style.display=\\'none\\'">' : "") +
            '<div class="sc-body"><div class="sc-name">' + esc(r.name) + '</div>' +
            '<div class="sc-site"><span class="sc-tag">' + esc(r.site) + '</span>' + (r.free ? "" : ' <span class="sc-paid">Paid ' + esc(r.price || "") + '</span>') + '</div>' +
            '<a class="sc-link" target="_blank" rel="noopener" href="' + esc(r.url) + '">Get the STL \u2197</a></div></div>';
        });
      });
      res.innerHTML = cards || '<p class="note">No results.</p>';
      count.textContent = shown + (shown === 1 ? " result" : " results") + (free ? " · free prints only" : " · including paid prints");
      note.textContent = "Searches the model sites — each result shows a picture, the model name and a link to get the STL.";
    }).catch(function(){
      note.textContent = "Could not reach the dashboard search.";
    });
}
function bindSearch(){
  document.querySelectorAll("#se3-sites .chip").forEach(function(c){
    c.addEventListener("click", function(){ c.classList.toggle("on"); });
  });
  $("se3-go").addEventListener("click", runSearch3d);
  $("se3-q").addEventListener("keydown", function(e){ if (e.key === "Enter") runSearch3d(); });
}

// ------------------------------------------------------- v18: filament tab
var FILAMENT_ADDED_KEY = "pd_multi_filament_added";
var _filamentSeed = null;
function filamentAll(){
  var all = [];
  (_filamentSeed || []).forEach(function(s){
    all.push({brand: s.brand, material: s.material, colorName: s.colorName,
              colorHex: s.colorHex, grams: s.remainingG, settings: s.settings, seed: true});
  });
  var added = [];
  try { added = JSON.parse(lsGet(FILAMENT_ADDED_KEY, "[]")) || []; } catch(e){ added = []; }
  added.forEach(function(s){
    all.push({brand: s.brand, material: s.material, colorName: s.colorName,
              colorHex: s.colorHex, grams: s.grams, settings: null, seed: false, id: s.id});
  });
  return all;
}
function renderFilamentTab(){
  var box = $("fi-groups");
  if (!box) return;
  var groups = {};
  filamentAll().forEach(function(s){
    var k = [s.brand, s.material, s.colorName].join("|");
    if (!groups[k]) groups[k] = {brand: s.brand, material: s.material, colorName: s.colorName,
      colorHex: s.colorHex, grams: 0, count: 0, settings: s.settings, items: []};
    groups[k].grams += (s.grams || 0); groups[k].count += 1;
    if (s.settings) groups[k].settings = s.settings;
    groups[k].items.push(s);
  });
  var keys = Object.keys(groups);
  if (!keys.length){ box.innerHTML = '<p class="note">No spools yet.</p>'; return; }
  var html = "";
  keys.forEach(function(k){
    var g = groups[k];
    html += '<div class="fi-group"><div class="fi-head"><span class="fi-swatch" style="background:' + esc(g.colorHex) + '"></span>' +
      '<div><b>' + esc(g.brand) + " " + esc(g.material) + " " + esc(g.colorName) + '</b><br>' +
      '<span class="sub">' + (g.grams / 1000).toFixed(2) + ' kg · ' + g.count + (g.count === 1 ? " spool" : " spools") + '</span></div></div>';
    if (g.settings){
      var st = g.settings;
      html += '<div class="fi-sets">Nozzle ' + st.nozzleTempC + '°C · Bed ' + st.bedTempC + '°C · Fan ' + st.fanPercent + '% · Flow ' + st.flowRatio + ' · Pressure advance ' + st.pressureAdvance + ' · Max volumetric ' + st.maxVolumetricSpeed + ' mm³/s</div>';
    }
    g.items.forEach(function(it){
      if (!it.seed){
        html += '<div class="row" style="margin-top:8px"><span class="sub">' + esc(it.brand) + ' ' + esc(it.colorName) + ' — ' + it.grams + ' g</span>' +
          '<button data-del="' + it.id + '" style="margin-left:auto">Remove</button></div>';
      }
    });
    html += '</div>';
  });
  box.innerHTML = html;
  box.querySelectorAll("[data-del]").forEach(function(b){
    b.addEventListener("click", function(){
      var id = b.getAttribute("data-del");
      var added = [];
      try { added = JSON.parse(lsGet(FILAMENT_ADDED_KEY, "[]")) || []; } catch(e){}
      added = added.filter(function(s){ return String(s.id) !== String(id); });
      lsSet(FILAMENT_ADDED_KEY, JSON.stringify(added));
      renderFilamentTab();
    });
  });
}
function loadFilament(){
  fetch("/api/filament").then(function(r){ return r.json(); }).then(function(j){
    _filamentSeed = j.seed || [];
    renderFilamentTab();
  }).catch(function(){
    _filamentSeed = [];
    renderFilamentTab();
  });
}
var COLOR_NAMES = [["Black","#1a1a1a"],["White","#f2f2f2"],["Grey","#808080"],["Silver","#c0c0c0"],["Red","#d63a2f"],["Orange","#e85d1f"],["Yellow","#e8c41c"],["Gold","#c9a227"],["Green","#1d9e57"],["Blue","#2470c9"],["Purple","#7a3fd1"],["Pink","#e87aa1"],["Brown","#7a4a21"],["Teal","#1f8a8a"],["Transparent","#d8e2e2"]];
function nearestColorName(hex){
  function rgb(h){ h = h.replace("#", ""); return [parseInt(h.substr(0,2),16), parseInt(h.substr(2,2),16), parseInt(h.substr(4,2),16)]; }
  var c = rgb(hex), best = null, bd = 1e9;
  COLOR_NAMES.forEach(function(n){
    var r = rgb(n[1]);
    var d = (c[0]-r[0])*(c[0]-r[0]) + (c[1]-r[1])*(c[1]-r[1]) + (c[2]-r[2])*(c[2]-r[2]);
    if (d < bd){ bd = d; best = n[0]; }
  });
  return best || "";
}
function bindFilament(){
  var photo = $("fi-photo"), canvas = $("fi-photocanvas"), swatch = $("fi-swatch"),
      colorInp = $("fi-color"), pick = $("fi-colorpick");
  function setColor(hex){
    swatch.style.background = hex;
    pick.value = hex;
    if (!colorInp.value) colorInp.value = nearestColorName(hex);
  }
  pick.addEventListener("input", function(){ setColor(pick.value); colorInp.value = nearestColorName(pick.value); });
  photo.addEventListener("change", function(){
    var f = photo.files && photo.files[0];
    if (!f) return;
    var img = new Image();
    img.onload = function(){
      var w = 120, h = Math.max(1, Math.round(120 * img.height / img.width));
      canvas.width = w; canvas.height = h;
      var ctx = canvas.getContext("2d");
      ctx.drawImage(img, 0, 0, w, h);
      var d = ctx.getImageData(0, 0, w, h).data;
      var r = 0, g = 0, b = 0, n = 0;
      for (var i = 0; i < d.length; i += 4){ r += d[i]; g += d[i+1]; b += d[i+2]; n++; }
      var hex = "#" + [r/n, g/n, b/n].map(function(v){
        var s = Math.round(v).toString(16); return s.length === 1 ? "0" + s : s;
      }).join("");
      colorInp.value = "";
      setColor(hex);
      URL.revokeObjectURL(img.src);
    };
    img.src = URL.createObjectURL(f);
  });
  $("fi-add").addEventListener("click", function(){
    var brand = ($("fi-brand").value || "").trim() || "Unknown";
    var material = $("fi-material").value || "PLA";
    var color = ($("fi-color").value || "").trim() || nearestColorName(pick.value);
    var grams = parseInt($("fi-grams").value, 10);
    if (isNaN(grams) || grams < 0) grams = 0;
    var added = [];
    try { added = JSON.parse(lsGet(FILAMENT_ADDED_KEY, "[]")) || []; } catch(e){}
    added.push({id: Date.now(), brand: brand, material: material, colorName: color, colorHex: pick.value, grams: grams});
    lsSet(FILAMENT_ADDED_KEY, JSON.stringify(added));
    $("fi-addmsg").textContent = "Added " + brand + " " + material + " " + color + ".";
    $("fi-brand").value = ""; $("fi-color").value = ""; $("fi-grams").value = "1000";
    renderFilamentTab();
  });
}

// ------------------------------------------------------- v18: maintenance tab
var MAINT_WIKI = "https://wiki.elegoo.com/Centauri-carbon";
var MAINT_ITEMS = [
  {id: "fans", title: "Clean the fans", days: 30},
  {id: "rails", title: "Lube the rails", days: 90},
  {id: "filter", title: "Charcoal filter", days: 180},
  {id: "bed", title: "Clean the bed", days: 7},
  {id: "belts", title: "Check the belts", days: 180},
  {id: "nozzle", title: "Check the nozzle", days: 90}
];
var MAINT_KEY = "pd_multi_maint";
function maintState(){
  try { return JSON.parse(lsGet(MAINT_KEY, "{}")) || {}; } catch(e){ return {}; }
}
function maintSave(st){ lsSet(MAINT_KEY, JSON.stringify(st)); }
function maintStatus(it, lastDone){
  if (!lastDone) return {cls: "todo", label: "Not recorded yet", pct: 0, due: null};
  var done = new Date(lastDone + "T12:00:00");
  var due = new Date(done.getTime() + it.days * 86400000);
  var now = new Date();
  var total = due - done, left = due - now;
  var pct = Math.max(0, Math.min(100, Math.round(100 * (1 - left / total))));
  var dueStr = due.toISOString().slice(0, 10);
  if (left <= 0) return {cls: "overdue", label: "Overdue", pct: 100, due: dueStr};
  if (left <= 3 * 86400000) return {cls: "due", label: "Due soon", pct: pct, due: dueStr};
  return {cls: "ok", label: "Up to date", pct: pct, due: dueStr};
}
function renderMaintenance(){
  var box = $("mt-cards");
  if (!box) return;
  var st = maintState();
  var over = 0, soon = 0, ok = 0, todo = 0;
  var html = "";
  MAINT_ITEMS.forEach(function(it){
    var s = maintStatus(it, st[it.id]);
    if (s.cls === "overdue") over++;
    else if (s.cls === "due") soon++;
    else if (s.cls === "ok") ok++;
    else todo++;
    html += '<div class="maint-card"><h3><a href="' + MAINT_WIKI + '" target="_blank" rel="noopener">' + esc(it.title) + ' \u2197</a></h3>' +
      '<div><span class="maint-badge ' + s.cls + '">' + s.label + '</span></div>' +
      '<p class="note" style="margin:8px 0 0">Last done: ' + (st[it.id] ? esc(st[it.id]) : "—") + '<br>Due next: ' + (s.due || "—") + ' · every ' + it.days + ' days</p>' +
      '<div class="maint-bar"><i style="width:' + s.pct + '%"></i></div>' +
      '<button data-maint="' + it.id + '">Mark done</button></div>';
  });
  box.innerHTML = html;
  $("mt-summary").textContent = over + " overdue · " + soon + " due soon · " + ok + " up to date · " + todo + " not recorded yet";
  var hours = 0, jobs = 0;
  (lastHistory || []).forEach(function(h){ hours += (h.duration_s || 0) / 3600; jobs++; });
  $("mt-hours").textContent = jobs ? (hours.toFixed(1) + " hrs · " + jobs + " jobs in history") : "—";
  box.querySelectorAll("[data-maint]").forEach(function(b){
    b.addEventListener("click", function(){
      var s2 = maintState();
      s2[b.getAttribute("data-maint")] = new Date().toISOString().slice(0, 10);
      maintSave(s2);
      renderMaintenance();
    });
  });
}
function bindMaintenance(){
  // Cards render on demand when the tab opens (showPage) and after history loads.
}

// ------------------------------------------------------- v18: layout lock
function setLayoutLocked(locked){
  document.body.classList.toggle("layout-locked", locked);
  lsSet("pd_multi_layout_locked", locked ? "1" : "0");
  var b = $("layout-lock");
  if (b) b.textContent = locked ? "🔒 Layout locked" : "🔓 Layout unlocked";
}
function bindLayoutLock(){
  var b = $("layout-lock");
  if (!b) return;
  b.addEventListener("click", function(){
    setLayoutLocked(!document.body.classList.contains("layout-locked"));
  });
  // While locked, swallow the pointerdown that starts a tile drag/resize.
  // Clicks on buttons/inputs/links inside tiles still work, and so does
  // the heading click that collapses a tile.
  document.addEventListener("pointerdown", function(e){
    if (!document.body.classList.contains("layout-locked")) return;
    var t = e.target;
    if (t && t.closest && t.closest(".tile") && !t.closest("button,input,select,textarea,a,img")) e.stopPropagation();
  }, true);
}

initTileLayout();
setLayoutLocked(lsGet("pd_multi_layout_locked", "1") === "1");
bindLayoutLock();
bindTempTiles();
bindLightTile();
bindSearch();
bindFilament();
bindMaintenance();
loadFilament();

</script>
</body>
</html>

"""

if __name__ == "__main__":
    main()
