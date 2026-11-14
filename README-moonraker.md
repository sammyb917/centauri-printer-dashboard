# Printer Dashboard — COSMOS / Moonraker (v19)

Standalone monitor for the Elegoo Centauri Carbon (+ CANVAS 4-color unit)
now running **OpenCentauri COSMOS 26.10.1** (Klipper + Moonraker).

This is the v18 dashboard converted from Elegoo's SDCP protocol to
Moonraker's HTTP API. Same 6 tabs (Home, Printer, Search, Filament,
Maintenance, Options), same dark mode, same movable/stretchable tiles
with Layout lock — plus a **new Moonraker tab**. Pure Python standard
library, no pip installs, no API keys.

**No config file is shipped with this build.** The printer is assumed
at its reserved address **192.168.1.166** (Pi-hole DHCP reservation).
If it moves, type the new IP in the page's "Printer not found yet" box
or in Options → Printer connection and it is saved next to the script.

## What changed from v18

- Backend: SDCP WebSocket (port 3030) + UDP discovery → Moonraker HTTP
  on port 80. Object names (chamber sensor, LEDs, fans, AFC lanes,
  macros) are **discovered at runtime** from `/printer/objects/list`,
  so the dashboard adapts to the printer instead of assuming names.
- Printer tab, same as before, now via Moonraker:
  - Nozzle / bed / chamber temps (`extruder`, `heater_bed`,
    `temperature_sensor chamber`).
  - 3 fan sliders: part fan via `M106`, aux/case via `SET_FAN_SPEED`.
  - Speed buttons 50/75/100/125/150% via `M220` — Klipper accepts any
    %, so 75/125/150 work now (they did not stick on SDCP).
  - Chamber light tap-toggle + brightness slider via
    `SET_LED LED=case WHITE=…`; toolhead LED toggle via
    `SET_LED LED=hotend WHITE=…`.
  - Camera: COSMOS serves the webcam at
    `http://<printer>/webcam/?action=stream` (proxied through the
    dashboard as `/camera`, same as before).
  - Print status / progress from `print_stats` + `virtual_sdcard` +
    `display_status`; layer numbers from `print_stats.info`.
  - Temperature charts now come from Moonraker's own
    `/server/temperature_store` (1-second samples). Note: Moonraker
    keeps 1200 samples, so the chart window is **20 minutes**, not 30.
    Fan/speed charts still use the dashboard's own rolling samples.
- **New Moonraker tab:** real print progress (layer, ETA, filament
  used in m + g), bed mesh heatmap (+ Calibrate / Load default),
  G-code console (any command via `/printer/gcode/script`, with the
  recent console log), fan RPM readouts, Z-offset nudge while
  printing (±0.01/0.02/0.05, reset), every Klipper macro as a button,
  AFC/CANVAS lane status (CANVAS_1..4, tool map, loaded lane, spool
  material/colour/weight), file list (with Print, behind a confirm)
  and print history.
- **COSMOS has no chamber heater** — chamber temperature is display
  only now; setting a chamber target is refused with a message.
- **Remove M8213/M8212 from the slicer G-code** — they e-stop COSMOS.
  Use `SET_LED LED=case WHITE=1` at the end of start G-code and
  `SET_LED LED=case WHITE=0` at the start of end G-code instead.
- Fixed a v18 bug: control actions threw a JavaScript error because
  `postControl` wrote to `$("hm-ctl-msg")`, which does not exist in
  the page — the temp editor's close callback never ran after a
  successful set. Guarded in v19.

## Deploy (media PC)

1. Copy **only** `printer-dashboard-cosmos-v19.py` to the media PC
   (same folder as the old dashboard, or a new folder — either works).
2. Kill the old dashboard: end **both** `python.exe` and `pythonw.exe`
   for it in Task Manager.
3. Run the new file: `python printer-dashboard-cosmos-v19.py`
   (serves `http://<media-pc>:8092/`).
4. Hard-refresh the page: **Ctrl+Shift+R**. The footer must read
   `printerdash-cosmos-v19`.
5. Do **not** run v18 and v19 at the same time — they both use port
   8092. v18/v17 files are untouched.

## Things to check on the real printer

- Home + Printer tabs show live temps, fans, light, camera, progress.
- Speed buttons 75/125/150 actually stick now (watch the Speed readout).
- Light tile toggles; brightness slider dims the chamber light.
- Moonraker tab: bed mesh draws; Z nudge moves the nozzle a touch
  while printing; macros run; AFC lanes show the right spools and the
  loaded lane; files list + history fill in.
- Layer numbers show only if the COSMOS Orca profile's
  `SET_PRINT_STATS_INFO` lines are in the slicer start / layer-change
  G-code (total layer in start G-code, current layer on layer change).
  Without them the layer field stays blank — everything else works.
- If the printer's object names differ from the expected ones
  (`temperature_sensor chamber`, `led case`, `led hotend`,
  `fan_generic aux_fan` / `case_fan`, `AFC_lane CANVAS_1..4`), the
  Moonraker tab's "What the dashboard found" tile shows exactly what
  was discovered — that is the first thing to look at.

## Known limits

- Temperature chart window is 20 minutes (Moonraker's store size).
- Filament-used grams assume 1.75 mm filament at 1.24 g/cm³ (PLA).
- AFC lanes are display-only in this build (no load/unload buttons).
- Macros starting with `_` are hidden from the Macros tile.
- The dashboard talks to Moonraker on port 80 by default; if COSMOS
  ever moves it, set `moonraker_port` in
  `printer-dashboard-config.json` next to the script.
- Print history totals come from Moonraker's history database on the
  printer; a fresh COSMOS install starts empty.

## Endpoints (for reference)

- `GET /api/status` — normalized status (same shape as v18).
- `GET /api/moonraker` — everything the Moonraker tab needs.
- `GET /api/temp_history` — temperature_store series (nozzle/bed/box).
- `GET /api/history` — print history (Moonraker).
- `GET /api/raw` — raw Moonraker objects + discovered names (diagnostics).
- `POST /api/control` — pause / resume / stop / light / light_dim /
  hotend_light / speed / fan / temp / zoffset / zoffset_reset /
  start_print.
- `POST /api/gcode` — `{"script": "..."}` to Klipper.
- `POST /api/set-ip` — `{"ip": "192.168.1.166"}`.
- `GET /camera` — proxied MJPEG webcam. `GET /thumb?u=…` — proxied
  printer file (e.g. file thumbnails).
- `GET /api/search3d`, `GET /api/filament` — unchanged from v18.

## Verification done (without the live printer)

- `python3 -m py_compile` clean.
- Served page loads; `node --check` on the served JavaScript clean.
- jsdom runtime test (canned Moonraker payloads): 24/24 checks passed
  — all 7 tabs, Home/Printer rendering, speed-button POST, Moonraker
  tab render (mesh, fans/RPM, macros, AFC lanes, files, history,
  console log), G-code console POST.
- End-to-end test against a fake Moonraker (real HTTP): discovery,
  status normalization, `/api/moonraker`, `/api/temp_history`, camera
  proxy, and every control action verified by the exact G-code the
  fake received (`PAUSE`, `M220 S125`, `M106 S128`,
  `SET_FAN_SPEED FAN=aux_fan SPEED=0.75`, `M104 S220`,
  `SET_LED LED=case WHITE=0/0.40`, `SET_LED LED=hotend WHITE=1`,
  `SET_GCODE_OFFSET Z_ADJUST=0.02 MOVE=1`, `SET_GCODE_OFFSET Z=0 MOVE=1`,
  `/printer/print/start`).
- Not verified without the live printer: actual object names on Sam's
  COSMOS install (discovery handles differences, but the "What the
  dashboard found" tile is the check), real camera frames, real bed
  mesh values, real AFC lane data, layer numbers (need the Orca
  profile's `SET_PRINT_STATS_INFO` lines).
