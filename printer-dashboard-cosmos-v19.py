#!/usr/bin/env python3
"""
Printer Dashboard (multi-page) — standalone monitor for an Elegoo
Centauri Carbon (+ CANVAS 4-color unit) running OpenCentauri COSMOS.

Talks to the printer over the LAN using Moonraker's HTTP API on port 80
(Klipper underneath). Object names are discovered at runtime, so the
dashboard adapts to the printer instead of assuming them.
Pure Python standard library — no pip installs.

Run:  python printer-dashboard-cosmos-v19.py   (serves http://<this-box>:8092/)
Config (OPTIONAL, next to this file): printer-dashboard-config.json
    { "printer_ip": "192.168.1.166", "port": 8092 }
Out of the box the printer is assumed at its reserved address
192.168.1.166 (Pi-hole DHCP reservation). If it moves, the dashboard
page itself asks for the IP and saves it to the config (merging, never
clobbering).

Build tag: printerdash-cosmos-v19
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
# v19 ships with NO config file (a blank config once wiped Sam's keys).
# The printer IP/port are read at runtime from the shared configs folder
# or printer-dashboard-config.json next to this file, if one exists —
# and the built-in default is the printer's reserved 192.168.1.166, so
# it works with no config at all. The IP Sam types into the page's
# "Printer not found yet" box is saved to whichever config file the IP
# was loaded from (or next to this file if none existed), so it persists
# across restarts. Nothing secret is ever baked into this file.
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
# it onto the media PC, so v19 also looks here — no JSON editing needed.
THINGIVERSE_TOKEN_PATH = os.path.join(HERE, "thingiverse-token.json")
BUILD_TAG = "printerdash-cosmos-v19"

# ------------------------------------------------------------- Moonraker ----
# The printer now runs OpenCentauri COSMOS (Klipper + Moonraker). The
# dashboard talks plain HTTP to Moonraker on port 80 — no WebSocket, no
# discovery broadcast, no SDCP. Object names are discovered at runtime
# from /printer/objects/list, so the dashboard adapts to Sam's actual
# printer instead of assuming names. Verified against the Moonraker API
# docs (web_api + printer_objects) and real COSMOS+CANVAS object data
# (AFC 1.2.1): lanes are `AFC_lane CANVAS_1..4` with map/load/prep/
# tool_loaded/material/color/filament_name/weight/status; the `AFC`
# object itself lists `lanes` and `current_load`.

DEFAULT_PRINTER_IP = "192.168.1.166"   # Pi-hole DHCP reservation for the CC1

DEFAULTS = {"printer_ip": DEFAULT_PRINTER_IP, "port": 8092,
            "moonraker_port": 80}


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
    """Shared configs folder first, then the file next to this script,
    filling in anything still empty. Returns (cfg, source) where source
    is the file the printer IP came from — that is where save_printer_ip
    writes, so a typed-in IP persists across restarts. If no file
    supplied an IP, source is the local file next to this script and the
    built-in default (the printer's reserved .166) is used."""
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
MOONRAKER_PORT = int(CONFIG.get("moonraker_port") or 80)

# ---------------------------------------------------------------- state -----
_lock = threading.Lock()
_state = {
    "online": False,
    "last_seen": 0.0,
    "printer_ip": CONFIG.get("printer_ip") or DEFAULT_PRINTER_IP,
    "machine_name": "",
    "firmware": "",
    "klippy_state": "",
    "klippy_message": "",
    "objects": [],        # names from /printer/objects/list
    "discovered": {},     # chamber sensor, LEDs, fans, AFC lanes, macros
    "status": {},         # last /printer/objects/query payload
    "temp_store": {},     # last /server/temperature_store payload
    "history": [],        # /server/history/list jobs, newest first
    "history_totals": {},  # /server/history/totals
    "files": [],          # /server/files/list (gcodes root)
    "file_meta": {},      # path -> /server/files/metadata
    "gcode_store": [],    # recent console lines from /server/gcode_store
    "fail_count": 0,
    "error": "",
}
_wake = threading.Event()  # set when settings change -> poller retries now


def save_printer_ip(ip):
    """Persist a user-entered printer IP into the app config, MERGING with
    whatever is already there. Writes to the file the IP was loaded from
    (CONFIG_SOURCE) — the shared folder when it exists, otherwise the
    config next to this file. Only ever called from the page's IP form or
    when the printer is found at a new address — the app never creates
    or rewrites the config on its own."""
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


# -------------------------------------------------------- Moonraker client --
def _mk_url(path):
    return "http://%s:%d%s" % (get_state()["printer_ip"], MOONRAKER_PORT, path)


def _moonraker_error(e):
    """Pull Moonraker's own error message out of an HTTPError, if any."""
    try:
        body = e.read().decode("utf-8", "replace")
        d = json.loads(body)
        err = d.get("error") or {}
        return err.get("message") or str(e)
    except Exception:
        return str(e)


def moonraker_get(path, timeout=8):
    req = urllib.request.Request(_mk_url(path),
                                 headers={"User-Agent": "PrinterDash/1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            d = json.load(resp)
    except urllib.error.HTTPError as e:
        raise RuntimeError(_moonraker_error(e))
    return d.get("result", d)


def moonraker_post(path, payload, timeout=10):
    data = json.dumps(payload or {}).encode()
    req = urllib.request.Request(
        _mk_url(path), data=data,
        headers={"Content-Type": "application/json",
                 "User-Agent": "PrinterDash/1"},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            d = json.load(resp)
    except urllib.error.HTTPError as e:
        raise RuntimeError(_moonraker_error(e))
    return d.get("result", d)


def gcode(script):
    """Send G-code to Klipper via Moonraker. `script` may hold several
    commands separated by newlines."""
    return moonraker_post("/printer/gcode/script", {"script": script})


def _led_name(disc, which):
    """Klipper object name (without the `led ` prefix) for the chamber
    (`case`) or toolhead (`hotend`) LED, or None if not discovered."""
    o = (disc or {}).get("case_led" if which == "case" else "hotend_led")
    if not o:
        return None
    return o.split(" ", 1)[1] if " " in o else o


def discover_objects(objs):
    """Pick the objects this dashboard needs out of
    /printer/objects/list. Names vary by printer, so match by shape:
    the chamber sensor is the temperature_sensor with 'chamber' in its
    name; fans are `fan` (part) + `fan_generic *` (aux/case by name);
    LEDs are `led *`; AFC lanes are `AFC_lane *` (fallback
    `AFC_stepper *`). Macros are every `gcode_macro *`."""
    disc = {"chamber": None, "leds": [], "case_led": None,
            "hotend_led": None, "fans": {}, "afc": False,
            "afc_lanes": [], "macros": []}
    for o in objs or []:
        if o.startswith("temperature_sensor "):
            name = o.split(" ", 1)[1]
            if "chamber" in name.lower() and not disc["chamber"]:
                disc["chamber"] = o
        elif o.startswith("led "):
            name = o.split(" ", 1)[1]
            disc["leds"].append(name)
            if name.lower() == "case":
                disc["case_led"] = o
            if name.lower() == "hotend":
                disc["hotend_led"] = o
        elif o == "fan":
            disc["fans"]["part"] = o
        elif o.startswith("fan_generic "):
            name = o.split(" ", 1)[1]
            low = name.lower()
            if "aux" in low:
                disc["fans"]["aux"] = o
            elif "case" in low or "box" in low or "chamber" in low:
                disc["fans"]["box"] = o
            else:
                disc["fans"][name] = o
        elif o.startswith("temperature_fan "):
            disc["fans"]["temp_" + o.split(" ", 1)[1]] = o
        elif o == "AFC":
            disc["afc"] = True
        elif o.startswith("AFC_lane "):
            disc["afc_lanes"].append(o)
        elif o.startswith("gcode_macro "):
            disc["macros"].append(o.split(" ", 1)[1])
    if not disc["chamber"]:
        # Fall back to the first temperature sensor of any kind.
        for o in objs or []:
            if o.startswith("temperature_sensor "):
                disc["chamber"] = o
                break
    if not disc["afc_lanes"]:
        for o in objs or []:
            if o.startswith("AFC_stepper "):
                disc["afc_lanes"].append(o)
    if not disc["case_led"] and disc["leds"]:
        disc["case_led"] = "led " + disc["leds"][0]
    disc["macros"] = sorted(set(disc["macros"]))
    return disc


def moonraker_poller():
    """Owns the printer connection forever: poll Moonraker, discover
    objects, refresh temperature history, console log, print history
    and files; back off and retry on drop."""
    backoff = 2
    last_objects = 0.0
    last_temp = 0.0
    last_slow = 0.0
    last_gcode = 0.0
    while True:
        try:
            ip = get_state()["printer_ip"]
            if not ip:
                set_state(online=False,
                          error="No printer IP configured yet.")
                _wake.wait(timeout=5)
                _wake.clear()
                continue
            info = moonraker_get("/printer/info", timeout=6)
            kstate = info.get("state", "")
            ready = (kstate == "ready")
            set_state(machine_name="Centauri Carbon",
                      firmware=info.get("software_version", ""),
                      klippy_state=kstate,
                      klippy_message=info.get("state_message", ""),
                      online=True,
                      error="" if ready else (info.get("state_message")
                                              or f"Klipper state: {kstate}"),
                      fail_count=0, last_seen=time.time())
            backoff = 2
            now = time.time()
            if ready and (now - last_objects > 60
                          or not get_state()["objects"]):
                try:
                    objs = moonraker_get("/printer/objects/list",
                                         timeout=6).get("objects", [])
                    set_state(objects=objs,
                              discovered=discover_objects(objs))
                    last_objects = now
                except Exception:
                    pass
            disc = get_state()["discovered"] or {}
            if ready:
                want = ["extruder", "heater_bed", "print_stats",
                        "virtual_sdcard", "display_status", "gcode_move",
                        "toolhead", "idle_timeout", "bed_mesh", "fan"]
                if disc.get("chamber"):
                    want.append(disc["chamber"])
                for f in (disc.get("fans") or {}).values():
                    want.append(f)
                for l in disc.get("leds") or []:
                    want.append("led " + l)
                if disc.get("afc"):
                    want.append("AFC")
                want += disc.get("afc_lanes") or []
                qs = "&".join(quote(o, safe="") for o in want)
                try:
                    res = moonraker_get("/printer/objects/query?" + qs,
                                        timeout=6)
                    set_state(status=res.get("status", res),
                              last_seen=time.time())
                except Exception:
                    pass
            if now - last_temp > 30:
                try:
                    set_state(temp_store=moonraker_get(
                        "/server/temperature_store?include_monitors=false",
                        timeout=8))
                    last_temp = now
                except Exception:
                    pass
            if now - last_gcode > 30:
                try:
                    gs = moonraker_get("/server/gcode_store?count=50",
                                       timeout=8)
                    set_state(gcode_store=gs.get("gcode_store", []))
                    last_gcode = now
                except Exception:
                    pass
            if now - last_slow > 60:
                try:
                    h = moonraker_get(
                        "/server/history/list?limit=20&order=desc",
                        timeout=8)
                    set_state(history=h.get("jobs", []))
                except Exception:
                    pass
                try:
                    t = moonraker_get("/server/history/totals", timeout=8)
                    set_state(history_totals=t)
                except Exception:
                    pass
                try:
                    fl = moonraker_get("/server/files/list?root=gcodes",
                                       timeout=8)
                    files = fl.get("files", []) if isinstance(fl, dict) else []
                    set_state(files=files)
                    meta = {}
                    newest = sorted(
                        files, key=lambda f: f.get("modified") or 0,
                        reverse=True)[:10]
                    for f in newest:
                        p = f.get("path")
                        if not p:
                            continue
                        try:
                            meta[p] = moonraker_get(
                                "/server/files/metadata?filename="
                                + quote(p, safe=""), timeout=8)
                        except Exception:
                            pass
                    set_state(file_meta=meta)
                except Exception:
                    pass
                last_slow = now
            _wake.wait(timeout=2)
            _wake.clear()
        except Exception as e:
            set_state(online=False, error=f"Printer connection: {e}",
                      fail_count=get_state()["fail_count"] + 1)
            _wake.wait(timeout=backoff)
            _wake.clear()
            backoff = min(backoff * 2, 30)


# ------------------------------------------------------------ normalize -----
PRINT_STATE_LABELS = {
    "standby": "Idle", "printing": "Printing", "paused": "Paused",
    "complete": "Complete", "error": "Error", "cancelled": "Stopped",
}


def _num(v, default=None):
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _pct01(v):
    """A 0.0–1.0 fraction -> 0–100 int, or None."""
    try:
        if v is None:
            return None
        return int(round(float(v) * 100))
    except Exception:
        return None


def _parse_afc(s, disc):
    """Normalize the AFC (CANVAS) lanes into the slot list the page has
    always used, plus the full lane detail for the Moonraker tab.
    Real COSMOS data (AFC 1.2.1): each lane is `AFC_lane CANVAS_<n>`
    with map ("T0".."T3" — NOT in lane order, always use map), load,
    prep, tool_loaded, material, color, filament_name, weight, status;
    the `AFC` object itself lists `lanes` and `current_load`."""
    if not disc.get("afc"):
        return {"found": False, "slots": [], "lanes": [],
                "current_load": None}
    afc = s.get("AFC") or {}
    current = afc.get("current_load")
    lanes = []
    for lane_obj in disc.get("afc_lanes") or []:
        lane = s.get(lane_obj) or {}
        name = lane_obj.split(" ", 1)[1] if " " in lane_obj else lane_obj
        color = str(lane.get("color") or "")
        if color and not color.startswith("#"):
            color = "#" + color
        weight = lane.get("weight")
        try:
            weight = round(float(weight)) if weight is not None else None
        except Exception:
            weight = None
        lanes.append({
            "name": name,
            "map": lane.get("map") or "",
            "loaded": bool(lane.get("tool_loaded")),
            "prep": bool(lane.get("prep")),
            "load": bool(lane.get("load")),
            "material": lane.get("material") or "",
            "color": color,
            "filament_name": lane.get("filament_name") or "",
            "weight": weight,
            "status": lane.get("status") or "",
        })
    slots = []
    for i, ln in enumerate(lanes):
        slots.append({
            "slot": i + 1,
            "type": ln["filament_name"] or ln["material"],
            "material": ln["material"],
            "brand": "",
            "color": ln["color"],
            "loaded": ln["loaded"],
            "active": bool(ln["loaded"])
                      or (current is not None and ln["name"] == current),
            "raw": ln,
        })
    return {"found": True, "slots": slots, "lanes": lanes,
            "current_load": current,
            "extra_keys": sorted(afc.keys()) if isinstance(afc, dict) else []}


def normalize_status():
    """Shape Moonraker's objects into the same dict the page has always
    used, so the Home/Printer tabs work unchanged."""
    st = get_state()
    s = st.get("status") or {}
    disc = st.get("discovered") or {}
    ex = s.get("extruder") or {}
    hb = s.get("heater_bed") or {}
    ps = s.get("print_stats") or {}
    vs = s.get("virtual_sdcard") or {}
    ds = s.get("display_status") or {}
    gm = s.get("gcode_move") or {}
    th = s.get("toolhead") or {}
    it = s.get("idle_timeout") or {}
    chamber = s.get(disc.get("chamber") or "") or {}
    pstate = PRINT_STATE_LABELS.get(ps.get("state"), ps.get("state") or "")
    progress = ds.get("progress")
    if progress is None:
        progress = vs.get("progress")
    progress_pct = None
    if isinstance(progress, (int, float)):
        progress_pct = round(progress * 100, 1)
    elapsed = _num(ps.get("print_duration")) or 0
    remaining = None
    if pstate == "Printing":
        meta = (st.get("file_meta") or {}).get(ps.get("filename") or "") or {}
        est = meta.get("estimated_time")
        if isinstance(est, (int, float)) and est > 0:
            remaining = max(est - elapsed, 0)
        elif progress and progress > 0.02:
            remaining = elapsed * (1 - progress) / progress
    sf = gm.get("speed_factor")
    speed_pct = int(round(sf * 100)) if isinstance(sf, (int, float)) else None
    info = ps.get("info") or {}
    fans = {}
    fans["model"] = _pct01((s.get("fan") or {}).get("speed"))
    fans["aux"] = _pct01((s.get((disc.get("fans") or {}).get("aux") or "")
                          or {}).get("speed"))
    fans["box"] = _pct01((s.get((disc.get("fans") or {}).get("box") or "")
                          or {}).get("speed"))
    light_on = None
    light_level = None
    case_led = s.get(disc.get("case_led") or "") or {}
    cd = case_led.get("color_data")
    if isinstance(cd, list) and cd and isinstance(cd[0], (list, tuple)):
        w = cd[0][3] if len(cd[0]) > 3 else 0
        light_level = int(round(float(w or 0) * 100))
        light_on = 1 if any((c or 0) > 0.02 for c in cd[0]) else 0
    pos = th.get("position") or []
    coords = ""
    if len(pos) >= 3:
        coords = "X%.1f Y%.1f Z%.2f" % (pos[0], pos[1], pos[2])
    video = ""
    if st.get("printer_ip"):
        video = "http://%s/webcam/?action=stream" % st["printer_ip"]
    return {
        "online": st["online"],
        "needs_ip": not bool(st["printer_ip"]),
        "error": st["error"],
        "printer_ip": st["printer_ip"],
        "machine_name": st["machine_name"] or "Centauri Carbon",
        "firmware": st["firmware"],
        "machine_state": it.get("state") or st.get("klippy_state") or "",
        "print_state": pstate,
        "filename": ps.get("filename") or "",
        "task_id": ps.get("filename") or "",
        "progress": progress_pct,
        "current_layer": info.get("current_layer"),
        "total_layer": info.get("total_layer"),
        "print_speed": speed_pct,
        "speed_unit": "%",
        "elapsed_s": int(elapsed),
        "remaining_s": int(remaining) if remaining is not None else 0,
        "total_s": int(elapsed + (remaining or 0)),
        "nozzle": _num(ex.get("temperature")),
        "nozzle_target": _num(ex.get("target")),
        "bed": _num(hb.get("temperature")),
        "bed_target": _num(hb.get("target")),
        "box": _num(chamber.get("temperature")),
        "box_target": None,
        "coords": coords,
        "fans": fans,
        "light_on": light_on,
        "light_level": light_level,
        "video_url": video,
        "canvas": _parse_afc(s, disc),
        "klippy_state": st.get("klippy_state") or "",
        "build": BUILD_TAG,
    }


def normalize_history():
    out = []
    for j in get_state().get("history") or []:
        if not isinstance(j, dict):
            continue
        out.append({
            "task_id": str(j.get("job_id", "")),
            "filename": j.get("filename") or "?",
            "status": str(j.get("status") or "").capitalize(),
            "begin": j.get("start_time") or 0,
            "duration_s": int(j.get("print_duration")
                              or j.get("total_duration") or 0),
            "filament_used_mm": j.get("filament_used"),
            "thumbnail": "",
        })
    return out


def temp_history_payload():
    """Moonraker's /server/temperature_store, trimmed to the three
    sensors the charts draw: arrays are oldest -> newest, one sample
    per second, up to 1200 samples (20 minutes). Leading Nones align
    the newest samples when sensors have different lengths."""
    st = get_state()
    ts = st.get("temp_store") or {}
    disc = st.get("discovered") or {}

    def series(key):
        d = ts.get(key) or {}
        temps = d.get("temperatures") or []
        return [round(float(v), 1) for v in temps
                if isinstance(v, (int, float))]

    noz = series("extruder")
    bed = series("heater_bed")
    box = series(disc.get("chamber") or "") if disc.get("chamber") else []
    n = max(len(noz), len(bed), len(box))
    if n == 0:
        return {"ok": False, "t0": 0, "noz": [], "bed": [], "box": []}
    t0 = int(time.time()) - n

    def pad(a):
        return ([None] * (n - len(a))) + a if len(a) < n else a

    return {"ok": True, "t0": t0, "noz": pad(noz), "bed": pad(bed),
            "box": pad(box)}


def moonraker_tab():
    """Everything the Moonraker tab needs in one call."""
    st = get_state()
    s = st.get("status") or {}
    disc = st.get("discovered") or {}
    base = normalize_status()
    ps = s.get("print_stats") or {}
    gm = s.get("gcode_move") or {}
    th = s.get("toolhead") or {}
    filament_mm = _num(ps.get("filament_used")) or 0.0
    # Grams, assuming 1.75 mm filament at 1.24 g/cm^3 (PLA).
    filament_g = filament_mm * 3.14159265 * (1.75 / 2.0) ** 2 / 1000.0 * 1.24
    ho = gm.get("homing_origin") or []
    zoff = ho[2] if len(ho) > 2 else None
    pos = th.get("position") or []
    zpos = pos[2] if len(pos) > 2 else None
    fan_list = []
    labels = {"part": "Part fan", "aux": "Aux fan", "box": "Box fan"}
    for key, obj in (disc.get("fans") or {}).items():
        f = s.get(obj) or {}
        fan_list.append({
            "key": key,
            "name": labels.get(key, key.replace("_", " ").title()),
            "object": obj,
            "speed": _pct01(f.get("speed")),
            "rpm": f.get("rpm"),
        })
    bm = s.get("bed_mesh") or {}
    probed = bm.get("probed_matrix") or bm.get("mesh_matrix") or []
    flat = [v for row in probed if isinstance(row, list)
            for v in row if isinstance(v, (int, float))]
    mesh = {
        "profile": bm.get("profile_name") or "",
        "mesh_min": bm.get("mesh_min"),
        "mesh_max": bm.get("mesh_max"),
        "matrix": probed,
        "min": min(flat) if flat else None,
        "max": max(flat) if flat else None,
    }
    files = []
    for f in st.get("files") or []:
        if not isinstance(f, dict):
            continue
        p = f.get("path") or f.get("filename") or ""
        meta = (st.get("file_meta") or {}).get(p) or {}
        files.append({
            "filename": p,
            "size": f.get("size"),
            "modified": f.get("modified"),
            "estimated_time": meta.get("estimated_time"),
            "filament_total": meta.get("filament_total"),
        })
    files.sort(key=lambda x: x["modified"] or 0, reverse=True)
    totals = st.get("history_totals") or {}
    totals_txt = ""
    if totals:
        bits = []
        if totals.get("total_jobs") is not None:
            bits.append("%s jobs" % totals["total_jobs"])
        if totals.get("total_print_time"):
            bits.append("%.1f hrs printing"
                        % (totals["total_print_time"] / 3600.0))
        if totals.get("total_filament_used"):
            bits.append("%.1f m filament"
                        % (totals["total_filament_used"] / 1000.0))
        totals_txt = " · ".join(bits)
    found = []
    if disc.get("chamber"):
        found.append("chamber sensor: " + disc["chamber"])
    if disc.get("leds"):
        found.append("LEDs: " + ", ".join(disc["leds"]))
    if disc.get("fans"):
        found.append("fans: " + ", ".join((disc["fans"] or {}).values()))
    if disc.get("afc"):
        found.append("AFC lanes: %d" % len(disc.get("afc_lanes") or []))
    found.append("%d macros" % len([m for m in (disc.get("macros") or [])
                                     if not m.startswith("_")]))
    return {
        "online": st["online"],
        "error": st["error"],
        "klippy_state": st.get("klippy_state") or "",
        "klippy_message": st.get("klippy_message") or "",
        "firmware": st["firmware"],
        "filename": base["filename"],
        "print_state": base["print_state"],
        "progress": base["progress"],
        "current_layer": base["current_layer"],
        "total_layer": base["total_layer"],
        "elapsed_s": base["elapsed_s"],
        "remaining_s": base["remaining_s"],
        "filament_used_mm": round(filament_mm, 1),
        "filament_used_g": round(filament_g, 1),
        "speed_factor": base["print_speed"],
        "z_offset": zoff,
        "z_position": zpos,
        "message": ps.get("message") or "",
        "bed_mesh": mesh,
        "fans": fan_list,
        "macros": [m for m in (disc.get("macros") or [])
                   if not m.startswith("_")],
        "afc": _parse_afc(s, disc),
        "files": files,
        "history": normalize_history(),
        "history_totals": totals_txt,
        "discovered_summary": "; ".join(found),
        "gcode_log": (st.get("gcode_store") or [])[-50:],
    }
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

    def _read_json_body(self):
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except Exception:
            return {}
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            return {}

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
        elif path == "/api/moonraker":
            self._json(moonraker_tab())
        elif path == "/api/temp_history":
            self._json(temp_history_payload())
        elif path == "/api/history":
            self._json({"history": normalize_history()})
        elif path == "/api/raw":
            st = get_state()
            self._json({"status": st["status"], "objects": st["objects"],
                        "discovered": st["discovered"],
                        "afc": _parse_afc(st["status"], st["discovered"]),
                        "history": st["history"],
                        "files": st["files"],
                        "temp_store_keys": sorted((st["temp_store"] or {}).keys()),
                        "klippy": {"state": st["klippy_state"],
                                   "message": st["klippy_message"]}})
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
                req = self._read_json_body()
                ip = str(req.get("ip") or "").strip()
                ipaddress.IPv4Address(ip)  # raises on anything that isn't an IPv4
            except Exception:
                self._json({"ok": False,
                            "message": "That doesn't look like an IP address "
                                       "(use dotted-quad form)"}, 400)
                return
            set_state(printer_ip=ip, objects=[], discovered={}, status={},
                      history=[], history_totals={}, files=[], file_meta={},
                      temp_store={}, gcode_store=[], online=False, error="",
                      fail_count=0)
            try:
                save_printer_ip(ip)
            except Exception as e:
                self._json({"ok": False, "message": f"could not save config: {e}"}, 500)
                return
            _wake.set()
            self._json({"ok": True,
                        "message": f"Saved. Connecting to the printer at {ip}…"})
            return
        if path == "/api/gcode":
            req = self._read_json_body()
            script = str(req.get("script") or "").strip()
            if not script:
                self._json({"ok": False, "message": "empty command"}, 400)
                return
            if len(script) > 2000:
                self._json({"ok": False, "message": "command too long"}, 400)
                return
            if not get_state()["online"]:
                self._json({"ok": False, "message": "printer is offline"}, 503)
                return
            try:
                gcode(script)
                try:
                    gs = moonraker_get("/server/gcode_store?count=50",
                                       timeout=8)
                    set_state(gcode_store=gs.get("gcode_store", []))
                except Exception:
                    pass
            except Exception as e:
                self._json({"ok": False, "message": str(e)}, 502)
                return
            self._json({"ok": True, "message": f"Sent: {script}"})
            return
        if path != "/api/control":
            self._json({"error": "not found"}, 404)
            return
        req = self._read_json_body()
        if not get_state()["online"]:
            self._json({"ok": False, "message": "printer is offline"}, 503)
            return
        action = req.get("action")
        disc = get_state()["discovered"] or {}
        try:
            msg = self._do_control(action, req, disc)
        except ValueError as e:
            self._json({"ok": False, "message": str(e)}, 400)
            return
        except Exception as e:
            self._json({"ok": False, "message": str(e)}, 502)
            return
        self._json({"ok": True, "message": msg})

    def _do_control(self, action, req, disc):
        """Run one control action against Moonraker. Returns the message
        for the page. Raises ValueError for bad input, anything else for
        a Moonraker failure (its own message is passed through)."""
        if action == "pause":
            gcode("PAUSE")
            return "Pause sent"
        if action == "resume":
            gcode("RESUME")
            return "Resume sent"
        if action == "stop":
            if not req.get("confirm"):
                raise ValueError("stop needs confirm")
            gcode("CANCEL_PRINT")
            return "Stop sent"
        if action == "light":
            name = _led_name(disc, "case")
            if not name:
                raise ValueError("No chamber LED found on this printer")
            on = normalize_status()["light_on"]
            gcode(f"SET_LED LED={name} WHITE={0 if on else 1}")
            return f"Light {'off' if on else 'on'} sent"
        if action == "light_dim":
            name = _led_name(disc, "case")
            if not name:
                raise ValueError("No chamber LED found on this printer")
            try:
                value = int(req.get("value"))
            except Exception:
                raise ValueError("light needs a % value")
            if not 0 <= value <= 100:
                raise ValueError("light must be 0–100%")
            gcode(f"SET_LED LED={name} WHITE={value / 100:.2f}")
            return f"Chamber light {value}% sent"
        if action == "hotend_light":
            name = _led_name(disc, "hotend")
            if not name:
                raise ValueError("No toolhead LED found on this printer")
            st = get_state()["status"] or {}
            led = st.get("led " + name) or {}
            cd = led.get("color_data") or []
            on = bool(cd and cd[0] and any((c or 0) > 0.02 for c in cd[0]))
            gcode(f"SET_LED LED={name} WHITE={0 if on else 1}")
            return f"Toolhead LED {'off' if on else 'on'} sent"
        if action == "speed":
            try:
                value = int(req.get("value"))
            except Exception:
                raise ValueError("speed needs a % value")
            if not 10 <= value <= 300:
                raise ValueError("speed must be 10–300%")
            # M220: Klipper accepts any %, so 75/125/150 work now.
            gcode(f"M220 S{value}")
            return (f"Speed {value}% sent — watch the Speed readout to "
                    "confirm the printer took it")
        if action == "fan":
            which = req.get("fan")
            if which not in ("model", "aux", "box"):
                raise ValueError("unknown fan")
            try:
                value = int(req.get("value"))
            except Exception:
                raise ValueError("fan needs a % value")
            if not 0 <= value <= 100:
                raise ValueError("fan must be 0–100%")
            if which == "model":
                gcode(f"M106 S{int(round(value * 255 / 100))}")
            else:
                obj = (disc.get("fans") or {}).get(which)
                if not obj:
                    raise ValueError(f"No {which} fan found on this printer")
                fname = obj.split(" ", 1)[1] if " " in obj else obj
                gcode(f"SET_FAN_SPEED FAN={fname} SPEED={value / 100:.2f}")
            return f"{which} fan {value}% sent"
        if action == "temp":
            # COSMOS has no chamber heater: only nozzle + bed take targets.
            # M104/M140 set the target without waiting; the tile shows the
            # printer's actual reported target back, so you can see
            # whether it took.
            sent = []
            try:
                if req.get("nozzle") is not None:
                    v = int(req.get("nozzle"))
                    if not 0 <= v <= 320:
                        raise ValueError("nozzle must be 0-320")
                    gcode(f"M104 S{v}")
                    sent.append(f"nozzle {v}°C")
                if req.get("bed") is not None:
                    v = int(req.get("bed"))
                    if not 0 <= v <= 120:
                        raise ValueError("bed must be 0-120")
                    gcode(f"M140 S{v}")
                    sent.append(f"bed {v}°C")
            except (TypeError, ValueError) as e:
                raise ValueError(f"bad temp: {e}")
            if req.get("box") is not None:
                raise ValueError("COSMOS has no chamber heater — "
                                 "chamber temperature is display only")
            if not sent:
                raise ValueError("temp needs a value")
            return ("Target temp sent (" + ", ".join(sent) + ") — watch the "
                    "tile's target value to confirm the printer took it")
        if action == "zoffset":
            try:
                delta = float(req.get("delta"))
            except Exception:
                raise ValueError("z-offset needs a mm value")
            if delta == 0 or abs(delta) > 0.1:
                raise ValueError("z-offset nudge must be within ±0.1 mm")
            gcode(f"SET_GCODE_OFFSET Z_ADJUST={delta} MOVE=1")
            return f"Z-offset {'+' if delta > 0 else ''}{delta} mm sent"
        if action == "zoffset_reset":
            gcode("SET_GCODE_OFFSET Z=0 MOVE=1")
            return "Z-offset reset to 0"
        if action == "start_print":
            fn = str(req.get("filename") or "").strip()
            if not fn:
                raise ValueError("start_print needs a filename")
            moonraker_post("/printer/print/start", {"filename": fn})
            return f"Starting {fn}"
        raise ValueError(f"unknown action {action!r}")


def main():
    port = int(CONFIG.get("port") or 8092)
    threading.Thread(target=moonraker_poller, daemon=True).start()
    srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Printer Dashboard ({BUILD_TAG}) on http://0.0.0.0:{port}/  "
          f"— printer: {CONFIG.get('printer_ip') or DEFAULT_PRINTER_IP} "
          f"(Moonraker :{MOONRAKER_PORT})")
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
  /* ---------- v19: Moonraker tab ---------- */
  #page-moonraker .tile{--svc:#57a8ff}
  .glog{background:#0a0e14;border:1px solid var(--line);border-radius:8px;padding:10px;font-size:13px;line-height:1.5;white-space:pre-wrap;word-break:break-word;max-height:320px;overflow:auto;margin:10px 0 0}
  .glog .cmd{color:#8fb0ff}
  .glog .resp{color:#9fe8bb}
  .lane-card{background:var(--tile2);border:1px solid var(--line);border-radius:12px;padding:12px}
  .lane-card.loaded{border-color:var(--green);box-shadow:0 0 0 1px var(--green) inset}
  .mesh-wrap canvas{width:100%;height:auto;display:block;border-radius:12px;border:1px solid var(--line)}
  .mk-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
  .mk-stat{background:var(--tile2);border:1px solid var(--line);border-radius:10px;padding:10px}
  .mk-stat .v{font-size:20px;font-weight:750}
  .mk-stat .l{font-size:12px;color:var(--dim);margin-top:2px}
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
  <a href="#/moonraker" data-page="moonraker">Moonraker</a>
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
      <div style="margin:10px 0"><span class="sub">Chamber light brightness: <b id="ov2-light-val">—</b></span>
        <input type="range" id="ov2-light" min="0" max="100" step="5" value="100"></div>
      <div class="row"><button id="ov2-ctl-hotend-light">Toggle toolhead LED</button>
        <span id="ov2-ctl-hotend-state" class="sub"></span></div>
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

<section class="page" id="page-moonraker">
  <div class="grid">
    <div class="tile wide"><h2>Print details</h2>
      <div class="row"><span id="mk-file" class="big">—</span>
        <span id="mk-state" class="sub"></span></div>
      <div class="pbar" style="margin:12px 0 6px"><div id="mk-prog"></div></div>
      <div class="mk-grid" style="margin-top:12px">
        <div class="mk-stat"><div class="v" id="mk-pct">—</div><div class="l">Progress</div></div>
        <div class="mk-stat"><div class="v" id="mk-layer">—</div><div class="l">Layer</div></div>
        <div class="mk-stat"><div class="v" id="mk-eta">—</div><div class="l">Time left (ETA)</div></div>
        <div class="mk-stat"><div class="v" id="mk-elapsed">—</div><div class="l">Elapsed</div></div>
        <div class="mk-stat"><div class="v" id="mk-filament">—</div><div class="l">Filament used</div></div>
        <div class="mk-stat"><div class="v" id="mk-speed">—</div><div class="l">Speed factor</div></div>
        <div class="mk-stat"><div class="v" id="mk-zpos">—</div><div class="l">Nozzle Z</div></div>
        <div class="mk-stat"><div class="v" id="mk-zoff">—</div><div class="l">Z offset</div></div>
      </div>
      <p class="note" id="mk-msg" style="margin-top:10px"></p></div>
    <div class="tile wide"><h2>Bed mesh</h2>
      <div class="mesh-wrap"><canvas id="mk-mesh" width="520" height="360"></canvas></div>
      <p class="note" id="mk-meshstats" style="margin-top:8px">—</p>
      <div class="row" style="margin-top:8px">
        <button id="mk-mesh-cal">Calibrate bed mesh</button>
        <button id="mk-mesh-load">Load default mesh</button>
        <span class="note" id="mk-mesh-msg"></span></div></div>
    <div class="tile wide"><h2>G-code console</h2>
      <div class="row"><input type="text" id="mk-gcode-in" placeholder="Type a G-code command, e.g. M117 Hello" style="flex:1;min-width:200px">
        <button id="mk-gcode-send">Send</button></div>
      <div class="glog" id="mk-gcode-log">Console output appears here.</div></div>
    <div class="tile"><h2>Fans — speed &amp; RPM</h2>
      <div id="mk-fans"></div>
      <p class="note">RPM shows — when a fan has no tachometer wire.</p></div>
    <div class="tile"><h2>Z-offset nudge</h2>
      <p class="note">Nudges the nozzle while printing. Current offset: <b id="mk-zoff2">—</b></p>
      <div class="row" id="mk-zbtns">
        <button data-z="-0.05">−0.05</button>
        <button data-z="-0.02">−0.02</button>
        <button data-z="-0.01">−0.01</button>
        <button data-z="0.01">+0.01</button>
        <button data-z="0.02">+0.02</button>
        <button data-z="0.05">+0.05</button>
        <button id="mk-zreset">Reset to 0</button>
      </div>
      <p class="note" id="mk-zmsg" style="margin-top:10px"></p></div>
    <div class="tile wide"><h2>Macros</h2>
      <div class="row" id="mk-macros" style="gap:8px"></div>
      <p class="note" id="mk-macros-note" style="margin-top:10px"></p></div>
    <div class="tile wide"><h2>CANVAS lanes (AFC)</h2>
      <div id="mk-lanes" class="maint-grid"></div>
      <p class="note" id="mk-lanes-note" style="margin-top:10px"></p></div>
    <div class="tile wide"><h2>Files on the printer</h2>
      <table><thead><tr><th>File</th><th>Size</th><th>Modified</th><th>Est. time</th><th>Filament</th><th></th></tr></thead>
      <tbody id="mk-files"></tbody></table>
      <p class="note" id="mk-files-msg" style="margin-top:10px"></p></div>
    <div class="tile wide"><h2>Print history</h2>
      <table><thead><tr><th>File</th><th>Status</th><th>Started</th><th>Print time</th><th>Filament</th></tr></thead>
      <tbody id="mk-history"></tbody></table>
      <p class="note">Totals: <span id="mk-totals">—</span></p></div>
    <div class="tile"><h2>What the dashboard found</h2>
      <p class="note" id="mk-found">—</p>
      <p class="note" id="mk-klippy">—</p></div>
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
      <p class="note">The light is a Klipper LED now (SET_LED LED=case). In the slicer, put SET_LED LED=case WHITE=1 at the end of start G-code and SET_LED LED=case WHITE=0 at the start of end G-code — M8213/M8212 e-stop COSMOS, do not use them. Use the light tile to toggle it by hand.</p>
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
      <p class="note">Printer-only dashboard, COSMOS / Moonraker, pure Python standard library. Build: <span id="opt-build"></span></p>
      <p class="note">Keyboard shortcuts: H home · P printer · K moonraker · S search · F filament · M maintenance · O options. Shortcuts do not fire while typing in a field.</p></div>
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
<footer><span id="build"></span><span>Moonraker · standalone · port 8092</span></footer>
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
var PAGES = ["home", "printer", "moonraker", "search", "filament", "maintenance", "options"];
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
  if (name === "moonraker"){ loadMoonraker(); }
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
var tempSamples = [];   // server temperature_store: 1-second samples, 20-minute window
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
function drawChart(cv, series, fixedMax, noteId, src, agoLabel){
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
  src = src || samples;
  var t0 = src[0].t, t1 = src[src.length - 1].t;
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
  ctx.fillText(agoLabel || "30m ago", padL + 20, h - 6);
  ctx.fillText("now", w - padR - 14, h - 6);
  series.forEach(function(s){
    ctx.strokeStyle = s.color; ctx.lineWidth = 2; ctx.beginPath();
    var started = false;
    s.data.forEach(function(v, i){
      if (v === null || v === undefined) return;
      var x = padL + plotW * (src[i].t - t0) / (t1 - t0);
      var y = padT + plotH * (1 - (v - min) / (max - min));
      if (!started){ ctx.moveTo(x, y); started = true; } else ctx.lineTo(x, y);
    });
    ctx.stroke();
  });
}
function seriesFor(key, src){
  return (src || samples).map(function(s){ return s[key]; });
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
    var tsrc = (tempSamples.length > 10) ? tempSamples : samples;
    var tAgo = (tempSamples.length > 10) ? "20m ago" : "30m ago";
    drawChart($(cp + "temp"), [
      {data: seriesFor("noz", tsrc), color: "#ff5347"},
      {data: seriesFor("bed", tsrc), color: "#57a8ff"},
      {data: seriesFor("box", tsrc), color: "#e8a33d"}], null, cp + "tempnote", tsrc, tAgo);
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
  if (d.light_level !== null && d.light_level !== undefined && document.activeElement !== $("ov2-light")){
    $("ov2-light").value = d.light_level;
    $("ov2-light-val").textContent = d.light_level + "%";
  }
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
    var txt = "";
    var afc = j.afc || {};
    if (afc.found && afc.current_load) txt = "Loaded lane: " + afc.current_load;
    var a = $("ov2-fi-refill"), b = $("hm-fi-refill");
    if (a) a.textContent = txt;
    if (b) b.textContent = txt;
  }).catch(function(){});
}

function postControl(body, done){
  fetch("/api/control", {method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body)}).then(function(r){ return r.json(); }).then(function(j){
    var _m = j.message || (j.ok ? "Sent" : "Failed");
    $("ov2-ctl-msg").textContent = _m;
    var _hm = $("hm-ctl-msg"); if (_hm) _hm.textContent = _m;
    if (done) done(j);
  }).catch(function(){ var _m2 = "Could not reach the dashboard."; $("ov2-ctl-msg").textContent = _m2; var _hm2 = $("hm-ctl-msg"); if (_hm2) _hm2.textContent = _m2; });
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

// --- v19: chamber light dimmer + toolhead LED ---
$("ov2-ctl-hotend-light").onclick = function(){ postControl({action: "hotend_light"}); };
var lightSlider = $("ov2-light");
lightSlider.addEventListener("input", function(){ $("ov2-light-val").textContent = lightSlider.value + "%"; });
lightSlider.addEventListener("change", function(){ postControl({action: "light_dim", value: parseInt(lightSlider.value, 10)}); });

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
var KEYMAP = {h: "home", p: "printer", k: "moonraker", s: "search",
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
loadTempHistory();
setInterval(loadTempHistory, 30000);
pollMoonraker();

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
function drawMiniChart(svgId, key, target, color, src){
  var svg = $(svgId);
  if (!svg) return;
  var pts = (src || samples).filter(function(s){ return s[key] !== null && s[key] !== undefined; });
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
  var tsrc = (tempSamples.length > 10) ? tempSamples : null;
  drawMiniChart("ov2-mc-noz", "noz", d.nozzle_target, TEMP_COLORS[heatState(d.nozzle, d.nozzle_target)] || "#4da3ff", tsrc);
  drawMiniChart("ov2-mc-bed", "bed", d.bed_target, TEMP_COLORS[heatState(d.bed, d.bed_target)] || "#4da3ff", tsrc);
  drawMiniChart("ov2-mc-box", "box", d.box_target || 0, "#57a8ff", tsrc);
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

// ------------------------------------------------------- v19: Moonraker tab
// Live data for the Moonraker tab comes from /api/moonraker (polled while
// the tab is open). Temperature history comes from Moonraker's own
// /server/temperature_store (1-second samples, 20-minute window) via
// /api/temp_history — it feeds the Printer tab's temperature chart and the
// mini sparklines; fan/speed charts keep the local rolling samples.
var lastMoonraker = null;
var lastMacroKey = "";
function loadTempHistory(){
  fetch("/api/temp_history").then(function(r){ return r.json(); }).then(function(j){
    if (!j.ok || !j.noz || j.noz.length < 10) return;
    var arr = [];
    for (var i = 0; i < j.noz.length; i++){
      arr.push({t: (j.t0 + i) * 1000, noz: j.noz[i], bed: j.bed ? j.bed[i] : null,
        box: j.box ? j.box[i] : null, fanm: null, fana: null, fanb: null,
        speed: null, prog: null});
    }
    tempSamples = arr;
    if (currentPage === "printer") drawAll();
  }).catch(function(){});
}
function postGcode(script, done){
  fetch("/api/gcode", {method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify({script: script})}).then(function(r){ return r.json(); }).then(function(j){
    if (done) done(j);
  }).catch(function(){ if (done) done({ok: false, message: "Could not reach the dashboard."}); });
}
function runMacro(name){
  postGcode(name, function(j){
    var m = $("mk-macros-note");
    if (m) m.textContent = j.message || (j.ok ? "Sent " + name : "Failed");
    loadMoonraker();
  });
}
function drawBedMesh(cv, mesh){
  var ctx = fitCanvas(cv, 360);
  var w = cv.clientWidth || 520, h = 360;
  ctx.clearRect(0, 0, w, h);
  var m = mesh.matrix || [];
  if (!m.length || mesh.min === null || mesh.min === undefined){
    ctx.fillStyle = "#8fa3ba"; ctx.font = "14px sans-serif"; ctx.textAlign = "center";
    ctx.fillText("No bed mesh reported yet — run a calibration.", w / 2, h / 2);
    return;
  }
  var rows = m.length, cols = m[0].length;
  var padL = 46, padR = 14, padT = 14, padB = 26;
  var cw = (w - padL - padR) / cols, ch = (h - padT - padB) / rows;
  function colorFor(v){
    var f = (mesh.max === mesh.min) ? 0.5 : (v - mesh.min) / (mesh.max - mesh.min);
    // blue (low) -> green (mid) -> red (high)
    var r, g, b;
    if (f < 0.5){ var t = f / 0.5; r = 40 + t * 40; g = 90 + t * 120; b = 255 - t * 120; }
    else { var t2 = (f - 0.5) / 0.5; r = 80 + t2 * 175; g = 210 - t2 * 150; b = 135 - t2 * 110; }
    return "rgb(" + Math.round(r) + "," + Math.round(g) + "," + Math.round(b) + ")";
  }
  for (var y = 0; y < rows; y++){
    for (var x = 0; x < cols; x++){
      ctx.fillStyle = colorFor(m[y][x]);
      ctx.fillRect(padL + x * cw + 1, padT + y * ch + 1, cw - 2, ch - 2);
      ctx.fillStyle = "rgba(255,255,255,.92)";
      ctx.font = "11px sans-serif"; ctx.textAlign = "center";
      ctx.fillText((m[y][x] >= 0 ? "+" : "") + m[y][x].toFixed(3), padL + x * cw + cw / 2, padT + y * ch + ch / 2 + 4);
    }
  }
  ctx.fillStyle = "#8fa3ba"; ctx.font = "11px sans-serif"; ctx.textAlign = "left";
  ctx.fillText("min " + mesh.min.toFixed(3) + " mm", padL, h - 8);
  ctx.fillText("max " + mesh.max.toFixed(3) + " mm", padL + 130, h - 8);
  ctx.fillText("range " + (mesh.max - mesh.min).toFixed(3) + " mm", padL + 260, h - 8);
}
function renderMoonraker(j){
  lastMoonraker = j;
  $("mk-file").textContent = j.filename || "—";
  $("mk-state").textContent = j.print_state || "";
  var pct = (j.progress === null || j.progress === undefined) ? 0 : j.progress;
  $("mk-prog").style.width = Math.min(100, Math.max(0, pct)) + "%";
  $("mk-pct").textContent = Math.round(pct) + "%";
  $("mk-layer").textContent = (j.current_layer !== null && j.current_layer !== undefined && j.total_layer)
    ? (j.current_layer + " / " + j.total_layer) : "—";
  $("mk-eta").textContent = j.remaining_s ? fmtDur(j.remaining_s) : "—";
  $("mk-elapsed").textContent = j.elapsed_s ? fmtDur(j.elapsed_s) : "—";
  $("mk-filament").textContent = (j.filament_used_mm ? (j.filament_used_mm / 1000).toFixed(2) + " m" : "—") +
    (j.filament_used_g ? (" · " + j.filament_used_g.toFixed(1) + " g") : "");
  $("mk-speed").textContent = (j.speed_factor === null || j.speed_factor === undefined) ? "—" : j.speed_factor + "%";
  $("mk-zpos").textContent = (j.z_position === null || j.z_position === undefined) ? "—" : j.z_position.toFixed(2) + " mm";
  var zo = (j.z_offset === null || j.z_offset === undefined) ? "—" : (j.z_offset >= 0 ? "+" : "") + j.z_offset.toFixed(3) + " mm";
  $("mk-zoff").textContent = zo;
  $("mk-zoff2").textContent = zo;
  $("mk-msg").textContent = j.message || "";
  // bed mesh
  drawBedMesh($("mk-mesh"), j.bed_mesh || {});
  var bm = j.bed_mesh || {};
  $("mk-meshstats").textContent = (bm.profile ? ("Profile: " + bm.profile + " · ") : "") +
    ((bm.min === null || bm.min === undefined) ? "No mesh data yet." :
      ("Min " + bm.min.toFixed(3) + " mm · Max " + bm.max.toFixed(3) + " mm · Range " + (bm.max - bm.min).toFixed(3) + " mm"));
  // fans
  var fh = "";
  (j.fans || []).forEach(function(f){
    fh += '<div class="row" style="justify-content:space-between;margin:6px 0"><span>' + esc(f.name) + '</span><b>' +
      ((f.speed === null || f.speed === undefined) ? "—" : f.speed + "%") + (f.rpm !== null && f.rpm !== undefined ? " · " + f.rpm + " RPM" : "") + '</b></div>';
  });
  $("mk-fans").innerHTML = fh || '<p class="note">No fans reported.</p>';
  // macros
  var mkKey = (j.macros || []).join("|");
  if (mkKey !== lastMacroKey){
    lastMacroKey = mkKey;
    var mh = "";
    (j.macros || []).forEach(function(m){
      mh += '<button data-macro="' + esc(m) + '">' + esc(m) + '</button>';
    });
    $("mk-macros").innerHTML = mh || '<span class="note">No macros reported.</span>';
    $("mk-macros").querySelectorAll("[data-macro]").forEach(function(b){
      b.addEventListener("click", function(){ runMacro(b.getAttribute("data-macro")); });
    });
  }
  $("mk-macros-note").textContent = (j.macros && j.macros.length) ? j.macros.length + " macros on the printer." : "";
  // AFC lanes
  var lh = "";
  if (j.afc && j.afc.found && j.afc.lanes){
    j.afc.lanes.forEach(function(ln){
      var col = ln.color || "#888888";
      lh += '<div class="lane-card' + (ln.loaded ? " loaded" : "") + '"><h3 style="margin:0 0 6px">' + esc(ln.name) +
        (ln.map ? ' · ' + esc(ln.map) : "") + (ln.loaded ? ' · LOADED' : "") + '</h3>' +
        '<div class="row"><span class="chip" style="background:' + col + '"></span><div><b>' +
        esc(ln.filament_name || ln.material || "Empty") + '</b><br><span class="sub">' +
        esc([ln.material, ln.weight !== null && ln.weight !== undefined ? (ln.weight + " g") : ""].filter(function(x){ return x; }).join(" · ")) + '</span></div></div></div>';
    });
  }
  $("mk-lanes").innerHTML = lh || '<p class="note">No AFC lanes reported — they appear here once the printer reports them.</p>';
  $("mk-lanes-note").textContent = (j.afc && j.afc.found && j.afc.current_load) ? ("Lane in the toolhead: " + j.afc.current_load) : "";
  // files
  var frows = "";
  (j.files || []).forEach(function(f){
    frows += "<tr><td>" + esc(f.filename) + "</td><td>" + (f.size ? (f.size / 1048576).toFixed(1) + " MB" : "—") + "</td><td>" +
      fmtDate(f.modified) + "</td><td>" + (f.estimated_time ? fmtDur(f.estimated_time) : "—") + "</td><td>" +
      (f.filament_total ? (f.filament_total / 1000).toFixed(1) + " m" : "—") + '</td><td><button data-print="' + esc(f.filename) + '">Print</button></td></tr>';
  });
  $("mk-files").innerHTML = frows || '<tr><td colspan="6">No files on the printer yet.</td></tr>';
  $("mk-files").querySelectorAll("[data-print]").forEach(function(b){
    b.addEventListener("click", function(){
      var fn = b.getAttribute("data-print");
      if (!confirm("Start printing " + fn + "?")) return;
      postControl({action: "start_print", filename: fn}, function(r){
        $("mk-files-msg").textContent = (r && r.message) || "";
      });
    });
  });
  // history
  var hrows = "";
  (j.history || []).forEach(function(h){
    hrows += "<tr><td>" + esc(h.filename) + "</td><td>" + esc(h.status) + "</td><td>" + fmtDate(h.begin) + "</td><td>" +
      fmtDur(h.duration_s) + "</td><td>" + (h.filament_used_mm ? (h.filament_used_mm / 1000).toFixed(2) + " m" : "—") + "</td></tr>";
  });
  $("mk-history").innerHTML = hrows || '<tr><td colspan="5">No print history yet.</td></tr>';
  $("mk-totals").textContent = j.history_totals || "—";
  $("mk-found").textContent = j.discovered_summary || "—";
  $("mk-klippy").textContent = (j.klippy_state ? ("Klipper: " + j.klippy_state + (j.klippy_message ? " — " + j.klippy_message : "")) : "") +
    (j.firmware ? (" · " + j.firmware) : "");
  // console log
  var log = $("mk-gcode-log");
  if (j.gcode_log && j.gcode_log.length){
    var lh2 = "";
    j.gcode_log.forEach(function(e){
      lh2 += '<div><span class="' + (e.type === "command" ? "cmd" : "resp") + '">' + esc(e.message || "") + '</span></div>';
    });
    log.innerHTML = lh2;
    log.scrollTop = log.scrollHeight;
  }
}
function loadMoonraker(){
  fetch("/api/moonraker").then(function(r){ return r.json(); }).then(function(j){
    renderMoonraker(j);
  }).catch(function(){});
}
function pollMoonraker(){
  if (currentPage === "moonraker") loadMoonraker();
  setTimeout(pollMoonraker, 5000);
}
$("mk-gcode-send").onclick = function(){
  var inp = $("mk-gcode-in");
  var script = (inp.value || "").trim();
  if (!script) return;
  postGcode(script, function(j){
    var m = $("mk-macros-note");
    if (m) m.textContent = j.message || "";
    inp.value = "";
    loadMoonraker();
  });
};
$("mk-gcode-in").addEventListener("keydown", function(e){
  if (e.key === "Enter") $("mk-gcode-send").click();
});
document.querySelectorAll("#mk-zbtns button[data-z]").forEach(function(b){
  b.onclick = function(){
    postControl({action: "zoffset", delta: parseFloat(b.getAttribute("data-z"))}, function(j){
      $("mk-zmsg").textContent = (j && j.message) || "";
      loadMoonraker();
    });
  };
});
$("mk-zreset").onclick = function(){
  postControl({action: "zoffset_reset"}, function(j){
    $("mk-zmsg").textContent = (j && j.message) || "";
    loadMoonraker();
  });
};
$("mk-mesh-cal").onclick = function(){
  if (!confirm("Run BED_MESH_CALIBRATE? The printer will probe the bed — it takes a few minutes.")) return;
  postGcode("BED_MESH_CALIBRATE", function(j){
    $("mk-mesh-msg").textContent = j.message || "";
  });
};
$("mk-mesh-load").onclick = function(){
  postGcode("BED_MESH_PROFILE LOAD=default", function(j){
    $("mk-mesh-msg").textContent = j.message || "";
    loadMoonraker();
  });
};

</script>
</body>
</html>

"""

if __name__ == "__main__":
    main()
