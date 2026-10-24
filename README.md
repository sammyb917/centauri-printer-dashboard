# Printer Dashboard — Elegoo Centauri Carbon + CANVAS (SDCP V3)

A single-file, zero-dependency web dashboard for the Elegoo Centauri Carbon
FDM printer and its CANVAS 4-color filament unit. It talks to the printer
over your LAN using Elegoo's SDCP protocol (V3 dialect) — no cloud account,
no slicer, no Home Assistant required.

## What it does

- **Now printing** — file name, progress bar, current/total layer, elapsed
  and remaining time, print speed, and head position
- **Temperatures** — nozzle, bed, and chamber, with targets, plus fan speeds
  and chamber light state. Nozzle and bed values are color-coded against
  their targets: red (pulsing while the printer is heating up, steady
  during a print) below target, green at target, blue above target or
  cooling — and they flash when a heater first reaches its target
- **Print-finished chime** — a short three-note sound in the browser when
  a print completes (header speaker button mutes it; choice is remembered)
- **Camera** — the printer's built-in MJPEG stream, proxied through the
  dashboard so it just works in any browser
- **CANVAS filament** — every tray with brand, material, name, and color
  chip; the tray currently in use is highlighted
- **Print history** — collapsible list of past prints with status and
  duration
- **Controls** (collapsible) — Pause, Resume, Stop (with confirmation),
  chamber light toggle, print-speed presets (50–150%), and
  Model / Aux / Box fan sliders

The dashboard is one Python file using only the standard library: the web
server, the WebSocket client, and the SDCP protocol handling are all
hand-rolled. There is nothing to `pip install`.

## Requirements

- Python 3.8 or newer
- A Centauri Carbon (or compatible SDCP V3 printer) on the same LAN
- That's it

## Quick start

**Windows**

1. Copy this folder anywhere.
2. Double-click `start-printer-dashboard.bat`.
3. Open <http://localhost:8091/> in a browser.

**Linux / macOS**

```sh
python3 printer-dashboard.py
```

Then open <http://localhost:8091/> in a browser.

To stop it, close the console window or press Ctrl+C in its terminal.

## Finding the printer

You don't need to know the printer's IP address:

1. The dashboard first tries any saved/configured IP.
2. If nothing answers there after a few attempts, it broadcasts an SDCP
   discovery probe on the LAN and adopts the address that replies.
3. If the scan finds nothing (some routers block the broadcast), the page
   itself shows an IP-entry box. Type the printer's address — it's on the
   printer's touchscreen under Network — and the dashboard connects
   immediately and remembers it.

### Optional config file

Settings live in `printer-dashboard-config.json` next to
`printer-dashboard.py`. You normally never touch it — the dashboard writes
it for you when you enter an IP on the page — but you can pre-seed it:

```json
{ "printer_ip": "192.168.1.50", "port": 8091 }
```

`printer_ip` pins the printer's address; `port` changes the dashboard's
own web port (default 8091).

## Protocol notes (SDCP V3)

Hard-won details about the Centauri Carbon's SDCP V3.0.0 dialect
(firmware V1.4.x), verified against a real printer. Several of these
differ from the older/resin SDCP documentation floating around:

- **Requests must carry the discovery IDs.** Every command envelope's
  top-level `Id` must be the *connection Id* handed out by the UDP
  discovery reply, and `Data.MainboardID` the printer's mainboard ID.
  Requests with missing or wrong IDs are silently ignored — the printer
  won't even error.
- **Status and attributes are top-level keys, not wrapped in `Data`.**
  Status pushes arrive on topic `sdcp/status/<id>` shaped as
  `{"Status": {...}, "MainboardID": ..., "Topic": ...}`, and attributes
  on `sdcp/attributes/<id>` as `{"Attributes": {...}, ...}`. There is no
  `Data.Data` payload like older SDCP dialects use.
- **Command replies are bare acks.** A status request (Cmd 0) is answered
  with just `{"Ack": 0}` on `sdcp/response/<id>`; the real data arrives
  as the unsolicited status pushes. Don't wait for data in the reply.
- **`PrintInfo` ticks are seconds, not milliseconds.** A 14h44m print
  reports `TotalTicks: 53012`. (Resin-era firmware counts milliseconds;
  this code treats values above ~200000 total as milliseconds.)
- **CANVAS is Cmd 324.** The response carries `canvas_list` → `tray_list`
  (snake_case keys) plus `active_canvas_id` / `active_tray_id`. Tray
  `status`: 1 = loaded, 2 = loaded **and in use**.
- **Settings go through Cmd 403**, as bundles mirroring the status
  objects: `{"LightStatus": {"SecondLight": 0|1}}` for the chamber light,
  `{"CurrentFanSpeed": {"ModelFan": n, "AuxiliaryFan": n, "BoxFan": n}}`
  (send the full bundle with one fan changed) for fans, and
  `{"PrintSpeedPct": n}` for print speed.
- Print control: pause = Cmd 129, **stop = Cmd 130, continue/resume =
  Cmd 131**. Camera enable = Cmd 386 with `{"Enable": 1}`; the MJPEG
  stream itself is plain HTTP on port 3031. Print history is two-step:
  Cmd 320 returns task IDs, Cmd 321 returns each task's detail.
- Wire layout: WebSocket on port 3030 (`/websocket`), UDP discovery
  broadcast `M99999` on port 3000, camera on port 3031.

If you're debugging a dialect quirk, open
`http://localhost:8091/api/raw` — it dumps the parsed status, attributes,
CANVAS payload, and the last raw messages received from the printer.

## Development

`tests/fake_printer.py` is a simulated SDCP printer (WebSocket on 3030,
UDP discovery on 3000) so you can develop and test without hardware:

```sh
FAKE_V3=1 python3 tests/fake_printer.py
```

Modes (environment variables):

- `FAKE_V3=1` — speak the V3 dialect described above (top-level
  `Status`/`Attributes` keys, bare-ack replies, seconds ticks)
- `FAKE_STRICT=1` — ignore any request whose connection Id / MainboardID
  don't match what discovery issued, like the real printer
- `FAKE_NOPUSH=1` — never push status; only answer polls
- `FAKE_FLAT=1` — send status in the flat variant shape (no nesting)

Every command the fake receives is recorded to `/tmp/fake-received.json`.

## License

MIT — see [LICENSE](LICENSE).
