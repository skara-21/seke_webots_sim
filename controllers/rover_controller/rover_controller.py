"""
ROVER — Webots Python Controller (SQLite Cloud edition)
────────────────────────────────────────────────────────
Communicates entirely through SQLite Cloud REST API (HTTPS port 443).
No direct connection to the backend server needed — only internet access.

Flow:
  1. Polls dispatch_queue for a job with this robot's ID
  2. Navigates hub → restaurant → destination → hub
  3. Writes telemetry to robot_telemetry (upsert)
  4. Writes phase events to robot_phase_reports
"""

import json
import math
import sys
import threading
import time
import urllib.request
import urllib.parse
import uuid
from datetime import datetime, timezone

from controller import Robot

# ── SQLite Cloud config ───────────────────────────────────────────────────────

CLOUD_HOST   = 'nnxfwoaadz.finer-aphid.eks.use2.1kviht.sqlite.cloud'
CLOUD_APIKEY = '7YxzpXoOAj0kLzs18aLrNDYlrN5iUs4lhrnyW4R874w'
CLOUD_AUTH   = f'sqlitecloud://{CLOUD_HOST}:8860?apikey={CLOUD_APIKEY}'
DB_NAME      = 'seke-db'

# ── Sim config ────────────────────────────────────────────────────────────────

EARTH_R      = 6_371_000
WORLD_SCALE  = 1.0
TIMESTEP     = 64       # ms — must match WorldInfo.basicTimeStep
ARRIVE_M     = 3.0      # waypoint arrival radius (metres)
LOG_EVERY    = 5        # report position every N sim steps

# Hub GPS table (mirrors hubData.js)
HUB_GPS = {
    "ALK-1": (40.5419, -3.6313),
    "ALK-2": (40.5505, -3.6535),
    "ALK-3": (40.5391, -3.6262),
    "ALK-4": (40.5148, -3.6520),
    "SSR-1": (40.5553, -3.6145),
    "SSR-2": (40.5484, -3.6262),
    "SSR-3": (40.5638, -3.6118),
}

# GPS reference — overridden by dispatch_queue row
GPS_REF_LAT = 40.5495
GPS_REF_LON = -3.6240

# ── SQLite Cloud REST ─────────────────────────────────────────────────────────

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def cloud_query(sql, timeout=8):
    """POST a raw SQL string to SQLite Cloud REST API. Returns list of rows or []."""
    payload = json.dumps({"sql": sql, "database": DB_NAME}).encode()
    req = urllib.request.Request(
        f'https://{CLOUD_HOST}/v2/weblite/sql',
        data=payload,
        headers={
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {CLOUD_AUTH}',
        },
        method='POST'
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            data = json.loads(r.read()).get('data', [])
            return data if isinstance(data, list) else []
    except Exception as e:
        print(f'[cloud] query failed: {e}')
        return []

def sql_str(v):
    """Escape a value for safe interpolation into SQL."""
    if v is None:
        return 'NULL'
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"

# ── GPS helpers ───────────────────────────────────────────────────────────────

def gps_to_webots(lat, lon):
    dlat = math.radians(lat - GPS_REF_LAT)
    dlon = math.radians(lon - GPS_REF_LON)
    x = dlon * math.cos(math.radians(GPS_REF_LAT)) * EARTH_R * WORLD_SCALE
    z = -dlat * EARTH_R * WORLD_SCALE
    return x, z

def webots_to_gps(x, z):
    dlat = -z / (EARTH_R * WORLD_SCALE)
    dlon = x / (EARTH_R * WORLD_SCALE * math.cos(math.radians(GPS_REF_LAT)))
    return GPS_REF_LAT + math.degrees(dlat), GPS_REF_LON + math.degrees(dlon)

def bearing_to(x1, z1, x2, z2):
    dx, dz = x2 - x1, z2 - z1
    return (math.degrees(math.atan2(dx, -dz)) + 360) % 360

def dist(x1, z1, x2, z2):
    return math.sqrt((x2 - x1) ** 2 + (z2 - z1) ** 2)

# ── Dispatch poller (background thread, polls dispatch_queue) ─────────────────

class DispatchPoller:
    def __init__(self, robot_id):
        self._robot_id = robot_id
        self._order    = None
        self._lock     = threading.Lock()
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def _loop(self):
        while True:
            rid = self._robot_id.replace("'", "''")
            rows = cloud_query(
                f"SELECT * FROM dispatch_queue "
                f"WHERE robot_id = '{rid}' AND consumed_at IS NULL LIMIT 1"
            )
            if rows:
                row = rows[0]
                # Mark consumed so no other instance picks it up
                cloud_query(
                    f"UPDATE dispatch_queue SET consumed_at = {sql_str(utcnow())} "
                    f"WHERE queue_id = {sql_str(row['queue_id'])}"
                )
                with self._lock:
                    if self._order is None:
                        self._order = row
            time.sleep(1)

    def pop(self):
        with self._lock:
            o, self._order = self._order, None
            return o

# ── Telemetry / phase reporting ───────────────────────────────────────────────

def report_position(robot_id, lat, lng, speed_kmh, heading_deg, battery_pct=100.0):
    ts = utcnow()
    cloud_query(
        f"INSERT INTO robot_telemetry "
        f"(robot_id, lat, lng, speed_kmh, heading_deg, battery_pct, updated_at) "
        f"VALUES ({sql_str(robot_id)}, {lat:.6f}, {lng:.6f}, "
        f"{speed_kmh:.2f}, {heading_deg:.1f}, {battery_pct:.1f}, {sql_str(ts)}) "
        f"ON CONFLICT(robot_id) DO UPDATE SET "
        f"lat=excluded.lat, lng=excluded.lng, speed_kmh=excluded.speed_kmh, "
        f"heading_deg=excluded.heading_deg, battery_pct=excluded.battery_pct, "
        f"updated_at=excluded.updated_at"
    )

def report_phase(robot_id, order_id, phase):
    report_id = str(uuid.uuid4())
    cloud_query(
        f"INSERT INTO robot_phase_reports "
        f"(report_id, robot_id, order_id, phase, reported_at) "
        f"VALUES ({sql_str(report_id)}, {sql_str(robot_id)}, "
        f"{sql_str(order_id)}, {sql_str(phase)}, {sql_str(utcnow())})"
    )
    print(f'[{robot_id}] Phase → {phase}')

# ── Waypoint builder ──────────────────────────────────────────────────────────

def geo_to_waypoints(geo_json_str):
    """Convert stored [[lng,lat],...] geometry JSON to Webots (x,z) waypoints."""
    if not geo_json_str:
        return None
    try:
        coords = json.loads(geo_json_str)
        return [gps_to_webots(c[1], c[0]) for c in coords]
    except Exception:
        return None

def straight_line(from_lat, from_lng, to_lat, to_lng):
    return [gps_to_webots(from_lat, from_lng), gps_to_webots(to_lat, to_lng)]

# ── Main controller ───────────────────────────────────────────────────────────

class RoverController:
    def __init__(self, robot_id: str):
        self.robot_id = robot_id
        self.hub_id   = "-".join(robot_id.split("-")[:2])
        self.hub_lat, self.hub_lng = HUB_GPS.get(self.hub_id, (GPS_REF_LAT, GPS_REF_LON))

        self._rb = Robot()
        ts = int(self._rb.getBasicTimeStep())
        self._ts = ts

        self._lm = self._rb.getDevice("left_motor")
        self._rm = self._rb.getDevice("right_motor")
        self._gps     = self._rb.getDevice("gps")
        self._compass = self._rb.getDevice("compass")
        cam = self._rb.getDevice("front_camera")

        self._lm.setPosition(float("inf"))
        self._rm.setPosition(float("inf"))
        self._lm.setVelocity(0)
        self._rm.setVelocity(0)

        self._gps.enable(ts)
        self._compass.enable(ts)
        if cam:
            cam.enable(ts)

        self._max_spd = self._lm.getMaxVelocity()
        print(f"[{robot_id}] Max motor velocity: {self._max_spd:.1f} rad/s")

        self._poller = DispatchPoller(robot_id)
        self._tick   = 0
        print(f"[{robot_id}] Ready — hub {self.hub_id} ({self.hub_lat}, {self.hub_lng})")
        print(f"[{robot_id}] Polling SQLite Cloud for dispatch jobs...")

    def _pos(self):
        v = self._gps.getValues()
        lat, lon = v[0], v[1]
        wx, wz = gps_to_webots(lat, lon)
        return lat, lon, wx, wz

    def _heading(self):
        n = self._compass.getValues()
        return (math.degrees(math.atan2(n[0], -n[2])) + 360) % 360

    def _drive_toward(self, tx, tz, cx, cz):
        target_brg = bearing_to(cx, cz, tx, tz)
        err = (target_brg - self._heading() + 180) % 360 - 180
        k_turn = 0.05
        turn   = -max(-self._max_spd, min(self._max_spd, k_turn * err))
        if abs(err) > 60:
            lv = -turn * 0.8
            rv =  turn * 0.8
        else:
            fwd = self._max_spd * max(0.3, 1.0 - abs(err) / 90)
            lv  = fwd - turn
            rv  = fwd + turn
        lv = max(-self._max_spd, min(self._max_spd, lv))
        rv = max(-self._max_spd, min(self._max_spd, rv))
        self._lm.setVelocity(lv)
        self._rm.setVelocity(rv)
        return lv, rv

    def _stop(self):
        self._lm.setVelocity(0)
        self._rm.setVelocity(0)

    def _navigate(self, waypoints):
        wp_idx = 0
        while wp_idx < len(waypoints):
            if self._rb.step(self._ts) == -1:
                return False
            self._tick += 1
            lat, lon, cx, cz = self._pos()
            tx, tz = waypoints[wp_idx]
            if dist(cx, cz, tx, tz) < ARRIVE_M:
                wp_idx += 1
                print(f"[{self.robot_id}] WP {wp_idx}/{len(waypoints)}")
                continue
            lv, rv = self._drive_toward(tx, tz, cx, cz)
            if self._tick % LOG_EVERY == 0:
                spd_kmh = abs(lv + rv) / 2 * 0.10 * 3.6
                report_position(self.robot_id, round(lat, 6), round(lon, 6),
                                 round(spd_kmh, 2), round(self._heading(), 1))
        self._stop()
        return True

    def _wait_steps(self, n_steps):
        for _ in range(n_steps):
            if self._rb.step(self._ts) == -1:
                return False
            self._tick += 1
        return True

    def run(self):
        print(f"[{self.robot_id}] Entering idle loop")
        while True:
            if self._rb.step(self._ts) == -1:
                break
            self._tick += 1

            if self._tick % 30 == 0:
                lat, lon, _, _ = self._pos()
                report_position(self.robot_id, round(lat, 6), round(lon, 6),
                                 0.0, round(self._heading(), 1))

            order = self._poller.pop()
            if not order:
                continue

            # ── Update GPS reference from dispatch row ────────────────────────
            global GPS_REF_LAT, GPS_REF_LON
            if order.get('gps_ref_lat'):
                GPS_REF_LAT = float(order['gps_ref_lat'])
                GPS_REF_LON = float(order['gps_ref_lng'])
                print(f"[{self.robot_id}] GPS ref updated: {GPS_REF_LAT:.5f}, {GPS_REF_LON:.5f}")

            order_id = order['order_id']
            rest_lat = float(order['restaurant_lat'])
            rest_lng = float(order['restaurant_lng'])
            dest_lat = float(order['dest_lat'])
            dest_lng = float(order['dest_lng'])
            print(f"[{self.robot_id}] Dispatched → order {order_id}")

            # Leg 1: hub → restaurant
            leg1 = geo_to_waypoints(order.get('leg1_geometry')) or \
                   straight_line(self.hub_lat, self.hub_lng, rest_lat, rest_lng)
            print(f"[{self.robot_id}] Leg 1: {len(leg1)} waypoints to restaurant")
            if not self._navigate(leg1):
                break

            # At restaurant
            report_phase(self.robot_id, order_id, 'at_restaurant')
            if not self._wait_steps(int(3000 / self._ts)):
                break
            report_phase(self.robot_id, order_id, 'loading')

            # Leg 2: restaurant → destination
            leg2 = geo_to_waypoints(order.get('leg2_geometry')) or \
                   straight_line(rest_lat, rest_lng, dest_lat, dest_lng)
            print(f"[{self.robot_id}] Leg 2: {len(leg2)} waypoints to destination")
            if not self._navigate(leg2):
                break

            report_phase(self.robot_id, order_id, 'navigating_delivery')
            report_phase(self.robot_id, order_id, 'delivered')

            # Leg 3: destination → hub
            leg3 = straight_line(dest_lat, dest_lng, self.hub_lat, self.hub_lng)
            print(f"[{self.robot_id}] Leg 3: returning to hub")
            if not self._navigate(leg3):
                break

            print(f"[{self.robot_id}] Back at hub — going idle")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    robot_id = sys.argv[1] if len(sys.argv) > 1 else "ALK-1-R1"
    RoverController(robot_id).run()
