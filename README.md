# Printer Dashboard — Elegoo Centauri Carbon (+ CANVAS)

A single-file Python (standard library only, no pip installs) dashboard for
the Elegoo Centauri Carbon 1 with the CANVAS 4-color unit. It talks to the
printer over your LAN using Elegoo's SDCP protocol, and serves a dark-mode
web UI on port 8092.

**This package ships with NO config file.** Out of the box the printer is
found by LAN auto-discovery. Never commit a config file — one may hold your
printer's IP or API tokens.

## Quick start

1. Make sure Python 3.8+ is installed.
2. Run it: `python printer-dashboard.py` (Windows: double-click
   `start-printer-dashboard.bat`).
3. Open http://localhost:8092/ in a browser.
4. If the page shows **"Printer not found yet"**, type your printer's IP
   address and click Connect. The dashboard saves it next to the `.py`
   file, so it sticks across restarts. If the printer moves to a new
   address later, the dashboard re-scans the network on its own after a
   few failed tries and updates the saved IP itself.
5. The footer tag should read `printerdash-multi-v18`. If it shows an
   older tag after an upgrade, hard-refresh the browser (Ctrl+Shift+R) —
   a plain refresh does not reload the app.

## Config (optional, next to the .py file)

- `printer-dashboard-config.json` — `{"printer_ip": "", "port": 8092}`.
  Leave `printer_ip` empty for auto-discovery.
- `printer-dashboard-multi-config.json` — `{"thingiverse_token": "..."}`
  (optional, see Search below). Or put the token in the single-purpose
  file `thingiverse-token.json` next to the `.py` instead.
- Tokens are never printed, logged, or baked into this file.

## What's in it

- **6 tabs:** Home, Printer, Search, Filament, Maintenance, Options.
  Dark mode throughout.
- **Home:** camera window + CANVAS tile showing which tray is loaded and
  in use. Four small stat tiles in one row: Nozzle temp, Bed temp,
  Chamber, Chamber light. **Click the camera picture to enlarge it**;
  click again to close.
- **Printer:** temp tiles with compact 30-minute charts, CANVAS card,
  Controls tile (pause / resume / stop), print speed as five buttons —
  **Quiet 50%, Slower 75%, Normal 100% (default), Faster 125%,
  Ludicrous 150%** — and three fan sliders: part cooling (ModelFan),
  side/aux (AuxiliaryFan), chamber (BoxFan).
- **Progress ring:** the print-progress % sits on top of a ring arc, so
  it is never covered up.
- **Temp colours:** red while heating / green at target / blue while
  cooling, on numbers, chips and bars, with a legend.
- **Tap-to-change temps:** tap the nozzle or bed tile on Home or the
  Printer tab to type a new target (nozzle 0–320 °C, bed 0–120 °C). It
  applies while printing, updates the matching tile on the other tab, and
  recomputes the red/green/blue state. The tile shows the printer's
  actual reported target back, so you can see whether the printer took it.
- **Chamber light:** tap the Home "Chamber light" tile to toggle it, in
  sync with the Printer tab switch and camera dimming.
- **Search tab:** one search box; server-side search of MakerWorld (no
  key), Printables (no key), Thingiverse (free app token from your config
  — get one at https://www.thingiverse.com/developers, no key means
  Thingiverse is skipped) and Cults3D (free API key at
  https://cults3d.com/en/api/keys — until one is added to your config
  file, Cults3D shows a link out instead of results). Each result shows a
  small picture, model name, the site it came from, and a "Get the STL"
  link to the model page. Site chips are clickable toggles. Options has a
  **"Free prints only"** switch, ON by default — when on, paid designs are
  removed from the results entirely and the header count equals the cards
  shown.
- **Filament tab:** one home for your spools, grouped by
  brand+material+color into one tile each with combined kg + spool count.
  It starts empty — add spools with the form, which includes a photo
  colour picker: snap a photo of the spool and the page samples the
  colour, fills the swatch and guesses the name (correct it if the light
  fooled it). Spools live in the browser (localStorage), not on the
  server, and each tile shows the OrcaSlicer settings to use for that
  spool.
- **Maintenance tab:** six upkeep cards — clean the fans (30d), lube the
  rails (90d), charcoal filter (180d), clean the bed (7d), check the
  belts (180d), check the nozzle (90d) — each with last done, due next,
  interval, progress bar, status badge, and a Mark done button (stamps
  today and recalculates the next due date). The summary at the top shows
  total print hours plus overdue / due soon / up to date counts. Each card
  title links (new tab) to the Elegoo Centauri Carbon wiki:
  https://wiki.elegoo.com/Centauri-carbon. Maintenance state lives in the
  browser (localStorage).
- **Auto light:** when a print starts the light turns on; when it ends the
  light turns off (paused prints keep it on). Options switch. It only
  works while the dashboard page is open — the watcher lives in the page
  JavaScript.
- **Tunes:** start of print plays an upbeat "giddy up and go" jingle; end
  of print plays a short tune instead of a plain chime (never for stopped
  prints). WebAudio melodies in the page JavaScript, with Options
  switches to mute or change them.
- **Tiles:** all tiles are movable + stretchable, with a Layout lock
  button (locked by default; layout saves in the browser).
- No estimated print-head mm/s speed — the printer only sends
  PrintSpeedPct, so no speed is guessed from coordinates.

## Protocol notes (SDCP V3)

Connection needs the printer's MainboardID plus a connection Id from the
UDP discovery reply. Status pushes arrive about once a second; the top
level `Status` / `Attributes` keys carry the machine state. A bare
`{"Ack": 0}` is a valid reply to several commands. Print ticks are in
seconds, not milliseconds. CANVAS tray status: 1 = loaded, 2 = in use
(Cmd 324). Device control rides Cmd 403: light (`LightStatus`), fans
(`TargetFanSpeed` bundle), temps (`TempTargetNozzle` / `TempTargetHotbed` /
`TempTargetBox`), speed (`PrintSpeedPct`). Pause is Cmd 129, stop is
Cmd 130, resume is Cmd 131. Camera is MJPEG on port 3031.

Note: unknown Cmd codes (or wrong payloads) sent in quick succession can
crash the printer's `app` daemon and kill an active print — this dashboard
sends only the confirmed Cmd 403 variants above.

## Development / tests

`tests/fake_printer.py` is a fake SDCP printer for offline testing: UDP
discovery on port 3000, a WebSocket server on 3030, status pushes, history
(320→321), CANVAS (324), video (386), and pause/stop/resume plus light
and speed via Cmd 403. Run it with `FAKE_V3=1` for the strict V3 dialect.
It records every command it receives to `/tmp/fake-received.json`.

Limitation: the fake predates the v18 fan/temp commands — Cmd 403
payloads with `TargetFanSpeed` or `TempTargetNozzle`/`TempTargetHotbed`
are accepted silently but not reflected in the fake's status. Discovery,
status, history, CANVAS, video, pause/stop and speed still verify
end to end.

## License

MIT — see `LICENSE`.
