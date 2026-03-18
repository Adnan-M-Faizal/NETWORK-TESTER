"""
ISP Receipt Generator v6.0
Adaptive | Smart Targets | Stability-First | Dual-Stack

Architecture:
  Startup   -> calibrate line speed -> elect fastest primary target
  Runtime   -> 1/sec primary ping + 30s full sweep + 60s adaptive speed test
  Scoring   -> consistency(40) loss(25) jitter(20) latency(10) speed(5)
  Smart     -> tier auto-upgrades, primary re-elects on 3x consecutive fail
"""

import subprocess, platform, sys, time, csv, re, math, os, socket
import threading, urllib.request, urllib.error
from datetime import datetime
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION             = "6.0"
LATENCY_THRESHOLD   = 100.0
JITTER_THRESHOLD    = 20.0
PING_INTERVAL       = 1
SPEED_TEST_INTERVAL = 60
SWEEP_INTERVAL      = 30
PRIMARY_FAIL_LIMIT  = 3
CALIBRATION_BYTES   = 2_000_000
CALIBRATION_TIMEOUT = 12
PROBE_BYTES         = 1_000_000
PROBE_MIN_BYTES     = 900_000
UPLOAD_MAX_PLAUSIBLE_MBPS = 500
JITTER_WINDOW       = 20
BLOAT_GOOD, BLOAT_OK, BLOAT_BAD = 5, 30, 100
LOG_FILE    = "isp_receipts_v6.csv"
REPORT_FILE = "session_report_v6.txt"

# name -> (min_mbps, max_mbps, streams, dl_bytes, ul_bytes)
TIERS = {
    "basic":    (0,   50,    2, 5_000_000,  2_000_000),
    "standard": (50,  200,   4, 15_000_000, 5_000_000),
    "fast":     (200, 500,   6, 30_000_000, 10_000_000),
    "gigabit":  (500, 99999, 8, 50_000_000, 20_000_000),
}
DEFAULT_TIER = "standard"

# (display_name, domain, category)
TARGET_POOL = [
    ("Cloudflare DNS",  "one.one.one.one",           "dns"),
    ("Google DNS",      "dns.google",                "dns"),
    ("Quad9 DNS",       "dns.quad9.net",             "dns"),
    ("Instagram",       "www.instagram.com",         "social"),
    ("Facebook",        "www.facebook.com",          "social"),
    ("Twitter/X",       "x.com",                     "social"),
    ("TikTok",          "www.tiktok.com",            "social"),
    ("Snapchat",        "www.snapchat.com",          "social"),
    ("Reddit",          "www.reddit.com",            "social"),
    ("WhatsApp",        "web.whatsapp.com",          "messaging"),
    ("Telegram",        "web.telegram.org",          "messaging"),
    ("Discord",         "discord.com",               "messaging"),
    ("Botim",           "botim.me",                  "messaging"),
    ("Zoom",            "zoom.us",                   "messaging"),
    ("Teams",           "teams.microsoft.com",       "messaging"),
    ("YouTube",         "www.youtube.com",           "streaming"),
    ("Netflix",         "www.netflix.com",           "streaming"),
    ("Twitch",          "www.twitch.tv",             "streaming"),
    ("Spotify",         "open.spotify.com",          "streaming"),
    ("Prime Video",     "www.primevideo.com",        "streaming"),
    ("Google",          "www.google.com",            "productivity"),
    ("Cloudflare",      "www.cloudflare.com",        "productivity"),
    ("Microsoft",       "www.microsoft.com",         "productivity"),
    ("Apple",           "www.apple.com",             "productivity"),
    ("Steam",           "store.steampowered.com",    "gaming"),
    ("EA",              "www.ea.com",                "gaming"),
    ("Epic Games",      "www.epicgames.com",         "gaming"),
]

CATEGORY_COLOUR = {
    "dns":          "\033[96m",
    "social":       "\033[95m",
    "messaging":    "\033[93m",
    "streaming":    "\033[91m",
    "productivity": "\033[92m",
    "gaming":       "\033[94m",
}

CDN_ENDPOINTS_V4 = [
    ("https://speed.cloudflare.com/__down?bytes={}",      True),
    ("https://proof.ovh.net/files/10Mb.dat",              False),
    ("https://ash-speed.hetzner.com/10MB.bin",            False),
    ("https://fsn-speed.hetzner.com/10MB.bin",            False),
    ("https://hel-speed.hetzner.com/10MB.bin",            False),
    ("https://speedtest.tele2.net/10MB.zip",              False),
    ("https://speedtest.ftp.otenet.gr/files/test10Mb.db", False),
    ("https://download.thinkbroadband.com/10MB.zip",      False),
]
CDN_ENDPOINTS_V6 = [
    ("https://ipv6.speed.cloudflare.com/__down?bytes={}", True),
    ("https://proof.ovh.net/files/10Mb.dat",              False),
]
UPLOAD_URL_V4 = "https://speed.cloudflare.com/__up"
UPLOAD_URL_V6 = "https://ipv6.speed.cloudflare.com/__up"

G = "\033[92m"; Y = "\033[93m"; R = "\033[91m"
C = "\033[96m"; M = "\033[95m"; BLU = "\033[94m"
B = "\033[1m";  D = "\033[2m";  X = "\033[0m"


# ── Maths ────────────────────────────────────────────────────────────

def _mean(v):
    return sum(v) / len(v) if v else 0.0

def _stddev(v):
    if len(v) < 2: return 0.0
    m = _mean(v)
    return math.sqrt(sum((x - m) ** 2 for x in v) / len(v))

def _consistency(samples):
    if len(samples) < 5: return 100.0
    m = _mean(samples)
    if m == 0: return 100.0
    cv = _stddev(samples) / m
    if cv <= 0.05: return 100.0
    if cv >= 0.50: return 0.0
    return 100 - ((cv - 0.05) / 0.45) * 100


# ── Scoring ──────────────────────────────────────────────────────────

def _s_consistency(s): return _consistency(s)
def _s_loss(p):        return max(0.0, 100 - p * 5)
def _s_jitter(ms):
    if ms <= 2:  return 100.0
    if ms >= 50: return 0.0
    return 100 - ((ms - 2) / 48) * 100
def _s_latency(ms):
    if ms <= 10:  return 100.0
    if ms >= 300: return 0.0
    return 100 - ((ms - 10) / 290) * 100
def _s_throughput(mbps):
    if mbps >= 100: return 100.0
    if mbps >= 25:  return 80 + ((mbps - 25) / 75) * 20
    if mbps >= 10:  return 60 + ((mbps - 10) / 15) * 20
    if mbps >= 5:   return 40 + ((mbps - 5)  / 5)  * 20
    return max(0.0, (mbps / 5) * 40)
def _s_bloat(ms):
    if ms is None:        return 100.0
    if ms <= BLOAT_GOOD:  return 100.0
    if ms >= 150:         return 0.0
    return 100 - ((ms - BLOAT_GOOD) / (150 - BLOAT_GOOD)) * 100

def compute_score(lat_samples, loss_pct, jitter_ms, best_dl_mbps, bloat_ms):
    w = {"consistency": 40, "loss": 25, "jitter": 20, "latency": 10, "throughput": 5}
    tot = sum(w.values())
    raw = (
        _s_consistency(lat_samples)      * w["consistency"] +
        _s_loss(loss_pct)                * w["loss"]        +
        _s_jitter(jitter_ms)             * w["jitter"]      +
        _s_latency(_mean(lat_samples))   * w["latency"]     +
        _s_throughput(best_dl_mbps)      * w["throughput"]
    ) / tot
    return round(raw, 1)

def grade(score, ansi=True):
    if   score >= 92: t = "A+  Exceptional"
    elif score >= 85: t = "A   Excellent"
    elif score >= 75: t = "B   Good"
    elif score >= 60: t = "C   Acceptable"
    elif score >= 40: t = "D   Degraded"
    else:             t = "F   Poor -- show this report to your ISP"
    if not ansi: return t
    c = G if score >= 75 else (Y if score >= 50 else R)
    return f"{c}{t}{X}"

def bloat_label(ms, ansi=True):
    if ms is None:          s, c = "n/a",                  D
    elif ms <= BLOAT_GOOD:  s, c = f"{ms:.0f}ms negligible", G
    elif ms <= BLOAT_OK:    s, c = f"{ms:.0f}ms moderate",   Y
    elif ms <= BLOAT_BAD:   s, c = f"{ms:.0f}ms bad",        R
    else:                   s, c = f"{ms:.0f}ms severe",      R
    return f"{c}{s}{X}" if ansi else s

def lat_col(ms):
    if ms <= 0: return f"{R}FAIL{X}"
    c = G if ms < 50 else (Y if ms < 100 else R)
    return f"{c}{ms:.1f}{X}"


# ── Session state ────────────────────────────────────────────────────

class TargetRecord:
    def __init__(self, name, domain, category):
        self.name        = name
        self.domain      = domain
        self.category    = category
        self.v4_samples  = []; self.v6_samples  = []
        self.v4_drops    = 0;  self.v6_drops    = 0
        self.v4_total    = 0;  self.v6_total    = 0
        self.resolved_v4 = None; self.resolved_v6 = None
    def avg_v4(self):  return _mean(self.v4_samples)
    def avg_v6(self):  return _mean(self.v6_samples)
    def loss_v4(self): return (self.v4_drops / self.v4_total * 100) if self.v4_total else 0.0
    def loss_v6(self): return (self.v6_drops / self.v6_total * 100) if self.v6_total else 0.0


class SessionStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.tier_name           = DEFAULT_TIER
        self.calib_mbps          = 0.0
        self.primary_name        = None
        self.primary_domain      = None
        self.primary_consec_fails = 0
        self.total_pings         = 0
        self.drops               = 0
        self.lat_samples         = []
        self.recent_lat          = deque(maxlen=JITTER_WINDOW)
        self.dl_v4_samples       = []; self.dl_v6_samples = []
        self.ul_v4_samples       = []; self.ul_v6_samples = []
        self.last_dl_v4          = None; self.last_dl_v6  = None
        self.last_ul_v4          = None; self.last_ul_v6  = None
        self.loaded_lat          = []
        self.idle_lat_avg        = None
        self.targets             = {n: TargetRecord(n, d, c) for n, d, c in TARGET_POOL}
        self.ipv6_available      = False
        self.speed_test_running  = False
        self.active_endpoint_v4  = None
        self.active_endpoint_v6  = None

    @property
    def packet_loss_pct(self):
        return (self.drops / self.total_pings * 100) if self.total_pings else 0.0
    @property
    def avg_latency(self): return _mean(self.lat_samples)
    @property
    def jitter(self):      return _stddev(list(self.recent_lat))
    @property
    def best_dl(self):
        c = self.dl_v4_samples + self.dl_v6_samples
        return max(c) if c else 0.0
    @property
    def bufferbloat(self):
        if not self.loaded_lat or self.idle_lat_avg is None: return None
        return max(0.0, _mean(self.loaded_lat) - self.idle_lat_avg)
    @property
    def score(self):
        return compute_score(self.lat_samples, self.packet_loss_pct,
                             self.jitter, self.best_dl, self.bufferbloat)
    def tier(self): return TIERS[self.tier_name]


stats = SessionStats()


# ── CSV ──────────────────────────────────────────────────────────────

def setup_csv():
    try:
        with open(LOG_FILE, mode="x", newline="") as f:
            csv.writer(f).writerow(["Timestamp","Event","Target","Protocol",
                "Latency (ms)","Jitter (ms)","DL (Mbps)","UL (Mbps)",
                "Loaded Lat (ms)","Notes"])
    except FileExistsError:
        pass

def log_event(ts, event, target="--", proto="--", lat="--", jitter="--",
              dl="--", ul="--", loaded="--", note=""):
    with open(LOG_FILE, mode="a", newline="") as f:
        csv.writer(f).writerow([ts,event,target,proto,lat,jitter,dl,ul,loaded,note])


# ── DNS resolution ───────────────────────────────────────────────────

def resolve_target(domain, timeout=3):
    """Returns (ipv4_str|None, ipv6_str|None). Always fresh, never cached."""
    v4 = v6 = None
    try:
        socket.setdefaulttimeout(timeout)
        for fam, *_, sockaddr in socket.getaddrinfo(domain, None):
            if fam == socket.AF_INET  and v4 is None:
                v4 = sockaddr[0]
            if fam == socket.AF_INET6 and v6 is None:
                addr = sockaddr[0]
                v6   = addr.split("%")[0] if "%" in addr else addr
    except Exception:
        pass
    finally:
        socket.setdefaulttimeout(None)
    return v4, v6

def check_ipv6():
    try:
        res = socket.getaddrinfo("one.one.one.one", 53,
                                  family=socket.AF_INET6,
                                  type=socket.SOCK_DGRAM)
        if not res: return False
        s = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
        s.settimeout(3)
        s.connect((res[0][4][0], 53))
        s.close()
        return True
    except Exception:
        return False


# ── Ping ─────────────────────────────────────────────────────────────

def ping_host(host, ipv6=False):
    os_name = platform.system().lower()
    if os_name == "windows":
        cmd = ["ping", "-6" if ipv6 else "-4", "-n", "1", "-w", "2000", host]
    else:
        cmd = (["ping6", "-c", "1", "-W", "2", host] if ipv6
               else ["ping",  "-c", "1", "-W", "2", host])
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT,
                                       universal_newlines=True)
        m = re.search(r"time[=<]\s*([\d\.]+)", out)
        return (True, float(m.group(1))) if m else (False, 0.0)
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False, 0.0

def ping_primary():
    """Resolves primary domain fresh, then pings its IPv4."""
    with stats.lock:
        domain = stats.primary_domain
    if not domain: return False, 0.0
    v4, _ = resolve_target(domain)
    if not v4:     return False, 0.0
    return ping_host(v4)


# ── Line calibration ─────────────────────────────────────────────────

def _endpoint_url(template, dynamic, byte_count):
    return template.format(byte_count) if dynamic else template

def _quick_download(url, max_bytes=CALIBRATION_BYTES, timeout=CALIBRATION_TIMEOUT):
    try:
        req = urllib.request.Request(url,
              headers={"User-Agent": f"ISP-Receipt/{VERSION}"})
        t0, received = time.time(), 0
        with urllib.request.urlopen(req, timeout=timeout) as r:
            while received < max_bytes:
                chunk = r.read(65536)
                if not chunk: break
                received += len(chunk)
        return received, time.time() - t0
    except Exception:
        return 0, 0.0

def calibrate_line():
    """Download 2 MB from first reachable CDN, measure speed, pick tier."""
    print(f"\n  [{C}~{X}] Calibrating line speed (2 MB probe)...")
    for template, dynamic in CDN_ENDPOINTS_V4:
        url = _endpoint_url(template, dynamic, CALIBRATION_BYTES)
        received, elapsed = _quick_download(url)
        if received < 500_000 or elapsed <= 0:
            continue
        mbps = (received * 8) / (elapsed * 1_000_000)
        with stats.lock:
            stats.calib_mbps = mbps
        tier = DEFAULT_TIER
        for tname, (mn, mx, *_) in TIERS.items():
            if mn <= mbps < mx:
                tier = tname
                break
        _, _, streams, dl_bytes, ul_bytes = TIERS[tier]
        print(f"  [{G}OK{X}] {G}{mbps:.1f} Mbps{X} -> tier: {B}{tier}{X}  "
              f"({streams} streams x {dl_bytes//1_000_000}MB per test)")
        with stats.lock:
            stats.tier_name = tier
        return tier
    print(f"  [{Y}!{X}] Calibration failed -- using {DEFAULT_TIER}")
    return DEFAULT_TIER


# ── Target election ──────────────────────────────────────────────────

def _probe_one_target(name, domain):
    v4, _ = resolve_target(domain)
    if not v4: return name, domain, float("inf"), None
    ok, lat = ping_host(v4)
    return name, domain, (lat if ok else float("inf")), v4

def elect_primary(exclude_domain=None):
    candidates = [(n, d) for n, d, _ in TARGET_POOL
                  if d != exclude_domain]
    if not candidates:
        candidates = [(n, d) for n, d, _ in TARGET_POOL]

    print(f"\n  [{C}~{X}] Electing primary from {len(candidates)} candidates...")
    results = []
    with ThreadPoolExecutor(max_workers=min(len(candidates), 20)) as ex:
        futs = {ex.submit(_probe_one_target, n, d): (n, d)
                for n, d in candidates}
        for f in as_completed(futs):
            results.append(f.result())

    results.sort(key=lambda r: r[2])
    for name, domain, lat, resolved in results:
        if lat < float("inf"):
            print(f"  [{G}OK{X}] Primary: {B}{name}{X} ({domain}) "
                  f"-> {resolved}  {G}{lat:.1f}ms{X}")
            with stats.lock:
                stats.primary_name   = name
                stats.primary_domain = domain
                stats.targets[name].resolved_v4 = resolved
            return name, domain

    fallback = TARGET_POOL[0]
    print(f"  [{R}!{X}] All targets unreachable -- using {fallback[1]}")
    with stats.lock:
        stats.primary_name   = fallback[0]
        stats.primary_domain = fallback[1]
    return fallback[0], fallback[1]

def re_elect_primary():
    with stats.lock:
        old = stats.primary_domain
    print(f"\n  [{R}!{X}] Primary {old} failed {PRIMARY_FAIL_LIMIT}x -- re-electing...")
    elect_primary(exclude_domain=old)
    with stats.lock:
        stats.primary_consec_fails = 0


# ── CDN endpoint selection ───────────────────────────────────────────

def pick_endpoint(candidates):
    for template, dynamic in candidates:
        url = _endpoint_url(template, dynamic, PROBE_BYTES)
        received, _ = _quick_download(url, max_bytes=PROBE_BYTES, timeout=15)
        if received >= PROBE_MIN_BYTES:
            print(f"  [{G}OK{X}] CDN: {url[:60]}... ({received//1000} KB)")
            return template, dynamic
        print(f"  [{Y}-{X}]  Skip: {url[:55]}... ({received} bytes)")
    print(f"  [{R}!{X}] No CDN passed probe -- will retry next cycle")
    return candidates[0]


# ── Download workers ─────────────────────────────────────────────────

def _single_stream(endpoint_entry, byte_count):
    template, dynamic = endpoint_entry
    url = _endpoint_url(template, dynamic, byte_count)
    try:
        req = urllib.request.Request(url,
              headers={"User-Agent": f"ISP-Receipt/{VERSION}"})
        t0, received = time.time(), 0
        with urllib.request.urlopen(req, timeout=90) as r:
            while True:
                chunk = r.read(65536)
                if not chunk: break
                received += len(chunk)
        return received, time.time() - t0
    except Exception:
        return 0, 0.0

def parallel_download(endpoint_entry, num_streams, bytes_each):
    wall_start = time.time()
    total_bytes = 0
    with ThreadPoolExecutor(max_workers=num_streams) as ex:
        futs = [ex.submit(_single_stream, endpoint_entry, bytes_each)
                for _ in range(num_streams)]
        for f in as_completed(futs):
            b, _ = f.result()
            total_bytes += b
    elapsed = time.time() - wall_start
    if elapsed <= 0 or total_bytes == 0:
        return 0.0, total_bytes, elapsed
    return (total_bytes * 8) / (elapsed * 1_000_000), total_bytes, elapsed


# ── Upload test ──────────────────────────────────────────────────────

def upload_test(upload_url, byte_count):
    block   = os.urandom(min(byte_count, 65536))
    payload = block * (byte_count // len(block))
    payload += os.urandom(byte_count - len(payload))
    req = urllib.request.Request(upload_url, data=payload, method="POST",
        headers={"User-Agent": f"ISP-Receipt/{VERSION}",
                 "Content-Type": "application/octet-stream",
                 "Content-Length": str(len(payload))})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            r.read()
        elapsed = max(time.time() - t0, 0.001)
        mbps    = (len(payload) * 8) / (elapsed * 1_000_000)
        return (-1.0, elapsed) if mbps > UPLOAD_MAX_PLAUSIBLE_MBPS else (mbps, elapsed)
    except Exception:
        return 0.0, 0.0


# ── Loaded latency sampler ───────────────────────────────────────────

def sample_loaded_latency(duration, out_list):
    end = time.time() + duration
    while time.time() < end:
        ok, lat = ping_primary()
        if ok: out_list.append(lat)
        time.sleep(1)


# ── Tier auto-upgrade ────────────────────────────────────────────────

def maybe_upgrade_tier(measured_mbps):
    with stats.lock:
        current = stats.tier_name
    _, max_mbps, *_ = TIERS[current]
    if measured_mbps <= max_mbps * 1.2:
        return
    for tname, (mn, mx, streams, dl, ul) in TIERS.items():
        if mn <= measured_mbps < mx:
            with stats.lock:
                stats.tier_name = tname
            print(f"\n  [{Y}^{X}] Tier upgraded: {current} -> {B}{tname}{X}  "
                  f"({streams} streams x {dl//1_000_000}MB)")
            return


# ── Speed test runners ───────────────────────────────────────────────

def _run_dl(label, colour, endpoint, proto_tag, sample_list, last_attr,
            num_streams, dl_bytes, loaded_ref=None):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mb   = (dl_bytes * num_streams) / 1_000_000
    print(f"\n[{colour}v{X}] {ts} | {label} ({num_streams}x{dl_bytes//1_000_000}MB = {mb:.0f}MB)")

    lat_thread = None
    if loaded_ref is not None:
        est = max(10, int(mb * 8))
        lat_thread = threading.Thread(
            target=sample_loaded_latency, args=(est, loaded_ref), daemon=True)
        lat_thread.start()

    try:
        mbps, total_bytes, elapsed = parallel_download(endpoint, num_streams, dl_bytes)
        if lat_thread: lat_thread.join(timeout=2)

        if mbps > 0:
            with stats.lock:
                sample_list.append(mbps)
                setattr(stats, last_attr, mbps)
                if loaded_ref is not None:
                    stats.loaded_lat.extend(loaded_ref)
                    if stats.idle_lat_avg is None and stats.lat_samples:
                        stats.idle_lat_avg = _mean(stats.lat_samples[-60:])
            maybe_upgrade_tier(mbps)
            col    = G if mbps >= 25 else (Y if mbps >= 5 else R)
            avg_l  = _mean(loaded_ref) if loaded_ref else 0
            bloat  = max(0, avg_l - (stats.idle_lat_avg or avg_l))
            extra  = (f"  loaded:{avg_l:.0f}ms  bloat:{bloat_label(bloat)}"
                      if loaded_ref else "")
            print(f"[{col}v{X}] {label}: {col}{mbps:.1f} Mbps{X}  "
                  f"({total_bytes/1e6:.1f}MB in {elapsed:.1f}s){extra}")
            log_event(ts, f"DL_{proto_tag}", proto=proto_tag, dl=f"{mbps:.2f}",
                      loaded=f"{avg_l:.0f}" if loaded_ref else "--",
                      note=f"{num_streams} streams {elapsed:.1f}s")
        else:
            if lat_thread: lat_thread.join(timeout=1)
            print(f"[{R}x{X}] {label} download failed -- will re-probe next cycle")
            with stats.lock:
                if proto_tag == "v4": stats.active_endpoint_v4 = None
                else:                 stats.active_endpoint_v6 = None
            log_event(ts, f"DL_FAIL_{proto_tag}", proto=proto_tag)
    except Exception as exc:
        if lat_thread: lat_thread.join(timeout=1)
        print(f"[{R}x{X}] {label} error: {exc}")
        log_event(ts, f"DL_ERR_{proto_tag}", proto=proto_tag, note=str(exc))


def _run_ul(label, colour, upload_url, proto_tag, sample_list, last_attr, ul_bytes):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{colour}^{X}] {ts} | {label} upload ({ul_bytes//1_000_000}MB)...",
          end=" ", flush=True)
    try:
        mbps, elapsed = upload_test(upload_url, ul_bytes)
        if mbps == -1.0:
            print(f"\r[{Y}^{X}] {label} upload bogus ({elapsed:.1f}s) -- discarded")
            log_event(ts, f"UL_BOGUS_{proto_tag}", proto=proto_tag,
                      note=f"server ack in {elapsed:.2f}s")
        elif mbps > 0:
            with stats.lock:
                sample_list.append(mbps)
                setattr(stats, last_attr, mbps)
            col = G if mbps >= 10 else (Y if mbps >= 2 else R)
            print(f"\r[{col}^{X}] {label}: {col}{mbps:.1f} Mbps{X}  "
                  f"({ul_bytes/1e6:.0f}MB in {elapsed:.1f}s)")
            log_event(ts, f"UL_{proto_tag}", proto=proto_tag,
                      ul=f"{mbps:.2f}", note=f"{elapsed:.1f}s")
        else:
            print(f"\r[{Y}^{X}] {label} upload returned no data")
    except Exception as exc:
        print(f"\r[{R}x{X}] {label} upload error: {exc}")
        log_event(ts, f"UL_ERR_{proto_tag}", proto=proto_tag, note=str(exc))


def run_speed_test():
    with stats.lock:
        if stats.speed_test_running: return
        stats.speed_test_running = True
        ep4     = stats.active_endpoint_v4
        ep6     = stats.active_endpoint_v6
        ipv6_on = stats.ipv6_available
        _, _, num_streams, dl_bytes, ul_bytes = stats.tier()

    if ep4 is None:
        print(f"\n  [{C}~{X}] Probing IPv4 CDN endpoints...")
        ep4 = pick_endpoint(CDN_ENDPOINTS_V4)
        with stats.lock: stats.active_endpoint_v4 = ep4

    if ipv6_on and ep6 is None:
        print(f"  [{C}~{X}] Probing IPv6 CDN endpoints...")
        ep6 = pick_endpoint(CDN_ENDPOINTS_V6)
        with stats.lock: stats.active_endpoint_v6 = ep6

    loaded_v4 = []
    _run_dl("IPv4 DL", C, ep4, "v4",
            stats.dl_v4_samples, "last_dl_v4",
            num_streams, dl_bytes, loaded_ref=loaded_v4)

    if ipv6_on and ep6:
        _run_dl("IPv6 DL", BLU, ep6, "v6",
                stats.dl_v6_samples, "last_dl_v6",
                num_streams, dl_bytes)

    _run_ul("IPv4 UL", M, UPLOAD_URL_V4, "v4",
            stats.ul_v4_samples, "last_ul_v4", ul_bytes)

    if ipv6_on:
        _run_ul("IPv6 UL", BLU, UPLOAD_URL_V6, "v6",
                stats.ul_v6_samples, "last_ul_v6", ul_bytes)

    with stats.lock:
        stats.speed_test_running = False


def speed_scheduler():
    time.sleep(8)
    while True:
        t = threading.Thread(target=run_speed_test, daemon=True)
        t.start(); t.join()
        time.sleep(SPEED_TEST_INTERVAL)


# ── Multi-target sweep ───────────────────────────────────────────────

def _ping_both(name, domain, do_v6):
    v4, v6        = resolve_target(domain)
    ok4, lat4     = ping_host(v4) if v4 else (False, 0.0)
    ok6, lat6     = (False, 0.0)
    if do_v6 and v6:
        ok6, lat6 = ping_host(v6, ipv6=True)
    return dict(name=name, domain=domain,
                ok4=ok4, lat4=lat4, v4=v4,
                ok6=ok6, lat6=lat6, v6=v6)

def run_sweep():
    ts     = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    do_v6  = stats.ipv6_available
    tasks  = [(n, d) for n, d, _ in TARGET_POOL]
    results = {}

    with ThreadPoolExecutor(max_workers=min(len(tasks), 30)) as ex:
        futs = {ex.submit(_ping_both, n, d, do_v6): n for n, d in tasks}
        for f in as_completed(futs):
            r = f.result()
            results[r["name"]] = r

    with stats.lock:
        for name, domain, cat in TARGET_POOL:
            r   = results.get(name, {})
            rec = stats.targets[name]
            rec.resolved_v4 = r.get("v4")
            rec.resolved_v6 = r.get("v6")
            rec.v4_total   += 1
            if r.get("ok4"): rec.v4_samples.append(r["lat4"])
            else:            rec.v4_drops  += 1
            if do_v6:
                rec.v6_total += 1
                if r.get("ok6"): rec.v6_samples.append(r["lat6"])
                else:            rec.v6_drops  += 1

    # Print table by category
    print(f"\n\n[{BLU}o{X}] {ts} | Multi-target sweep  ({len(results)} targets)\n")
    prev_cat = None
    for name, domain, cat in TARGET_POOL:
        r = results.get(name, {})
        if cat != prev_cat:
            cc = CATEGORY_COLOUR.get(cat, "")
            print(f"  {D}{cc}-- {cat} {'-'*40}{X}")
            prev_cat = cat
        ok4  = r.get("ok4"); lat4 = r.get("lat4", 0)
        v4s  = lat_col(lat4 if ok4 else 0)
        v4ip = f" {D}({r.get('v4','?')}){X}" if r.get("v4") else f" {R}(DNS fail){X}"
        if do_v6:
            ok6  = r.get("ok6"); lat6 = r.get("lat6", 0)
            v6s  = lat_col(lat6 if ok6 else 0)
            v6ip = f" {D}({r.get('v6','?')}){X}" if r.get("v6") else f" {D}(no AAAA){X}"
            if ok4 and ok6:
                d = lat6 - lat4
                dc = G if abs(d) < 5 else (Y if abs(d) < 20 else R)
                delta = f"{dc}{d:+.1f}ms{X}"
            else:
                delta = f"{D}--{X}"
            print(f"    {name:<18}  v4:{v4s}{v4ip}   v6:{v6s}{v6ip}   d:{delta}")
        else:
            print(f"    {name:<18}  v4:{v4s}{v4ip}")

    print()
    for name, r in results.items():
        log_event(ts, "SWEEP", target=name, proto="v4",
                  lat=f"{r['lat4']:.1f}" if r.get("ok4") else "DROP",
                  note=f"resolved={r.get('v4','DNS_FAIL')}")
        if do_v6:
            log_event(ts, "SWEEP", target=name, proto="v6",
                      lat=f"{r['lat6']:.1f}" if r.get("ok6") else "DROP",
                      note=f"resolved={r.get('v6','no_AAAA')}")

def sweep_scheduler():
    time.sleep(12)
    while True:
        t = threading.Thread(target=run_sweep, daemon=True)
        t.start(); t.join()
        time.sleep(SWEEP_INTERVAL)


# ── Live status bar ──────────────────────────────────────────────────

def render_status(ts, latency, jitter, stable):
    v4dl      = f"{stats.last_dl_v4:.1f}" if stats.last_dl_v4 else "--"
    v6dl      = f"{stats.last_dl_v6:.1f}" if stats.last_dl_v6 else "--"
    v4ul      = f"{stats.last_ul_v4:.1f}" if stats.last_ul_v4 else "--"
    bl        = stats.bufferbloat
    bl_str    = f"{bl:.0f}ms" if bl is not None else "--"
    sc        = stats.score
    sc_c      = G if sc >= 75 else (Y if sc >= 50 else R)
    l_c       = (G if latency < LATENCY_THRESHOLD else Y) if stable else R
    tag       = f"[{G}+{X}]" if stable else f"[{R}!{X}]"
    pri       = (stats.primary_name or "?")[:12]
    line      = (
        f"{tag} {D}{ts}{X} "
        f"| {D}{pri:<12}{X} "
        f"| Ping:{l_c}{latency:6.1f}ms{X} "
        f"| Jit:{jitter:5.1f}ms "
        f"| {C}{v4dl}v{v6dl}v {v4ul}^{X}Mbps "
        f"| Bloat:{bl_str} "
        f"| Loss:{stats.packet_loss_pct:4.1f}% "
        f"| Tier:{D}{stats.tier_name}{X} "
        f"| {B}{sc_c}{sc:5.1f}{X} {grade(sc)}"
    )
    print(line, end="\r" if stable else "\n", flush=True)


# ── Session report ───────────────────────────────────────────────────

def write_report(start, end):
    dur    = end - start
    h, rm  = divmod(int(dur), 3600); m, s = divmod(rm, 60)
    sc     = stats.score
    bl     = stats.bufferbloat
    sep    = "=" * 70
    sep2   = "-" * 70
    _, _, streams, dl_bytes, ul_bytes = stats.tier()

    lines = [
        sep,
        f"  ISP RECEIPT GENERATOR v{VERSION}  --  SESSION REPORT",
        sep,
        f"  Period    : {start.strftime('%Y-%m-%d %H:%M:%S')} -> {end.strftime('%H:%M:%S')}",
        f"  Duration  : {h:02d}h {m:02d}m {s:02d}s",
        f"  Tier      : {stats.tier_name}  (calibrated: {stats.calib_mbps:.1f} Mbps)",
        f"  Config    : {streams} streams x {dl_bytes//1_000_000}MB DL / {ul_bytes//1_000_000}MB UL",
        f"  Primary   : {stats.primary_name} ({stats.primary_domain})",
        f"  IPv6      : {'active' if stats.ipv6_available else 'not available'}",
        "",
        sep2, "  STABILITY  (primary target)", sep2,
        f"  Total pings       : {stats.total_pings}",
        f"  Packet drops      : {stats.drops}",
        f"  Packet loss       : {stats.packet_loss_pct:.2f}%",
        f"  Avg latency       : {stats.avg_latency:.1f} ms",
        f"  Avg jitter        : {stats.jitter:.1f} ms",
        f"  Consistency score : {_s_consistency(stats.lat_samples):.1f}/100",
        f"  Min / Max latency : {min(stats.lat_samples, default=0):.1f} / "
                               f"{max(stats.lat_samples, default=0):.1f} ms",
        "",
        sep2, "  THROUGHPUT", sep2,
        f"  IPv4 DL  : {len(stats.dl_v4_samples)} tests  avg {_mean(stats.dl_v4_samples):.1f} Mbps  "
               f"best {max(stats.dl_v4_samples, default=0):.1f} Mbps",
        f"  IPv6 DL  : {len(stats.dl_v6_samples)} tests  avg {_mean(stats.dl_v6_samples):.1f} Mbps  "
               f"best {max(stats.dl_v6_samples, default=0):.1f} Mbps",
        f"  IPv4 UL  : {len(stats.ul_v4_samples)} tests  avg {_mean(stats.ul_v4_samples):.1f} Mbps",
        f"  IPv6 UL  : {len(stats.ul_v6_samples)} tests  avg {_mean(stats.ul_v6_samples):.1f} Mbps",
        "",
        sep2, "  BUFFERBLOAT", sep2,
        (f"  Idle baseline  : {stats.idle_lat_avg:.1f} ms"
         if stats.idle_lat_avg else "  Idle baseline  : n/a"),
        (f"  Loaded latency : {_mean(stats.loaded_lat):.1f} ms"
         if stats.loaded_lat else "  Loaded latency : n/a"),
        (f"  Bloat delta    : {bloat_label(bl, ansi=False)}"
         if bl is not None else "  Bloat delta    : n/a"),
        "",
        sep2, "  TARGET SWEEP  (sorted by IPv4 avg latency)", sep2,
        f"  {'Target':<20}  {'Cat':<12}  {'v4 avg':>8}  {'v4 loss':>8}  "
               f"{'v6 avg':>8}  {'v6 loss':>8}  {'Dv6-v4':>9}",
        f"  {'-'*20}  {'-'*12}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*8}  {'-'*9}",
    ]

    sorted_recs = sorted(
        [r for r in stats.targets.values() if r.v4_total > 0],
        key=lambda r: r.avg_v4() if r.v4_samples else float("inf"))

    for rec in sorted_recs:
        v4a = f"{rec.avg_v4():.1f} ms" if rec.v4_samples else "no data"
        v4l = f"{rec.loss_v4():.1f}%"
        if stats.ipv6_available and rec.v6_total > 0:
            v6a   = f"{rec.avg_v6():.1f} ms" if rec.v6_samples else "no data"
            v6l   = f"{rec.loss_v6():.1f}%"
            delta = (f"{rec.avg_v6()-rec.avg_v4():+.1f} ms"
                     if rec.v4_samples and rec.v6_samples else "--")
        else:
            v6a, v6l, delta = "no IPv6", "--", "--"
        lines.append(
            f"  {rec.name:<20}  {rec.category:<12}  {v4a:>8}  "
            f"{v4l:>8}  {v6a:>8}  {v6l:>8}  {delta:>9}")

    lines += [
        "",
        sep2, "  COMPOSITE SCORE  (stability-first)", sep2,
        f"  Consistency  (40%): {_s_consistency(stats.lat_samples):.1f}/100",
        f"  Packet loss  (25%): {_s_loss(stats.packet_loss_pct):.1f}/100",
        f"  Jitter       (20%): {_s_jitter(stats.jitter):.1f}/100",
        f"  Latency      (10%): {_s_latency(stats.avg_latency):.1f}/100",
        f"  Throughput    (5%): {_s_throughput(stats.best_dl):.1f}/100",
        "",
        f"  FINAL SCORE : {sc:.1f} / 100",
        f"  GRADE       : {grade(sc, ansi=False)}",
        "",
        sep,
        f"  Raw events : {LOG_FILE}",
        f"  This report: {REPORT_FILE}",
        sep,
    ]

    with open(REPORT_FILE, "w") as f:
        f.write("\n".join(lines) + "\n")

    print("\n")
    for ln in lines:
        if "FINAL SCORE" in ln:
            c = G if sc >= 75 else (Y if sc >= 50 else R)
            print(f"  {B}{c}{ln.strip()}{X}")
        elif "GRADE" in ln:
            print(f"  {grade(sc)}")
        else:
            print(ln)


# ── Thread count selector (arrow-key UI) ────────────────────────────

THREAD_OPTIONS = [
    (2, "2 streams  -- basic broadband    (<50 Mbps)"),
    (4, "4 streams  -- standard broadband (50-200 Mbps)"),
    (6, "6 streams  -- fast fibre         (200-500 Mbps)"),
    (8, "8 streams  -- gigabit            (500 Mbps+)"),
]
THREAD_DEFAULT_IDX = 2

def _read_key():
    if platform.system().lower() == "windows":
        import msvcrt
        ch = msvcrt.getch()
        if ch in (b"\r", b"\n"): return "ENTER"
        if ch == b"\xe0":
            c2 = msvcrt.getch()
            if c2 == b"H": return "UP"
            if c2 == b"P": return "DOWN"
        return ch.decode("utf-8", errors="replace")
    else:
        import tty, termios
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
            if ch == "\x1b":
                c2 = sys.stdin.read(1)
                if c2 == "[":
                    c3 = sys.stdin.read(1)
                    if c3 == "A": return "UP"
                    if c3 == "B": return "DOWN"
            return "ENTER" if ch in ("\r", "\n") else ch
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

def _draw_selector(idx):
    n = len(THREAD_OPTIONS) + 2
    print(f"\033[{n}A", end="")
    print(f"  {D}Up/down arrows, Enter to confirm  (auto-calibration will still run):{X}")
    print()
    for i, (_, label) in enumerate(THREAD_OPTIONS):
        pfx = f"{B}{G} >  {X}" if i == idx else f"{D}    {X}"
        print(f"  {pfx}{B if i==idx else D}{label}{X}")

def select_thread_count():
    print(f"\n  {B}Override max stream count? (auto-calibration picks the right tier){X}\n")
    print(f"  {D}Up/down arrows, Enter to confirm:{X}")
    print()
    idx = THREAD_DEFAULT_IDX
    for i, (_, label) in enumerate(THREAD_OPTIONS):
        pfx = f"{B}{G} >  {X}" if i == idx else f"{D}    {X}"
        print(f"  {pfx}{B if i==idx else D}{label}{X}")
    while True:
        key = _read_key()
        if   key == "UP"    and idx > 0:                        idx -= 1
        elif key == "DOWN"  and idx < len(THREAD_OPTIONS) - 1: idx += 1
        elif key == "ENTER":
            chosen = THREAD_OPTIONS[idx][0]
            n = len(THREAD_OPTIONS) + 2
            print(f"\033[{n}A", end="")
            for _ in range(n): print("\033[2K")
            print(f"\033[{n}A", end="")
            print(f"  {G}OK{X}  {B}{THREAD_OPTIONS[idx][1]}{X}  selected\n")
            return chosen
        _draw_selector(idx)


# ── Main ─────────────────────────────────────────────────────────────

def main():
    print(f"\n{B}{'='*70}{X}")
    print(f"{B}    ISP RECEIPT GENERATOR  v{VERSION}  --  Adaptive + Stability-First    {X}")
    print(f"{B}{'='*70}{X}")
    print(f"\n  {D}{len(TARGET_POOL)} targets across "
          f"{len(set(c for _,_,c in TARGET_POOL))} categories "
          f"| all IPs resolved dynamically{X}")

    # Stream count override
    stream_override = None
    try:
        stream_override = select_thread_count()
    except Exception:
        print(f"  {D}(non-interactive -- auto-calibration only){X}\n")

    # IPv6
    print(f"  {D}Checking IPv6...{X}", end="", flush=True)
    ipv6_ok = check_ipv6()
    with stats.lock:
        stats.ipv6_available = ipv6_ok
    print(f"\r  IPv6             : {G+'active'+X if ipv6_ok else Y+'not available'+X}          ")

    # Calibrate
    tier = calibrate_line()

    # Apply stream override cap
    if stream_override is not None:
        _, _, calib_streams, dl, ul = TIERS[tier]
        if stream_override < calib_streams:
            # Find the tier whose stream count matches the override
            best = tier
            for tname, (mn, mx, streams, tdl, tul) in TIERS.items():
                if streams <= stream_override:
                    best = tname
            with stats.lock:
                stats.tier_name = best
            _, _, s, dl2, ul2 = TIERS[best]
            print(f"  {Y}!{X}  Stream cap applied: {calib_streams} -> {s}  (tier: {best})")

    # Elect primary
    elect_primary()

    with stats.lock:
        _, _, streams, dl_bytes, ul_bytes = stats.tier()

    print(f"\n  Tier             : {B}{stats.tier_name}{X}  "
          f"({streams} streams x {dl_bytes//1_000_000}MB per test)")
    print(f"  Speed test every : {SPEED_TEST_INTERVAL}s  "
          f"|  Sweep every: {SWEEP_INTERVAL}s")
    print(f"  Score weights    : consistency 40%  loss 25%  "
          f"jitter 20%  latency 10%  speed 5%")
    print(f"  Log              : {LOG_FILE}")
    print(f"\n  Press {B}Ctrl+C{X} to stop and generate your report.\n")
    print("-" * 75)

    setup_csv()
    start = datetime.now()

    for fn in (speed_scheduler, sweep_scheduler):
        threading.Thread(target=fn, daemon=True).start()

    try:
        while True:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ok, latency = ping_primary()
            trigger_reelect = False

            with stats.lock:
                stats.total_pings += 1
                if not ok:
                    stats.drops += 1
                    stats.primary_consec_fails += 1
                    render_status(ts, 0.0, stats.jitter, stable=False)
                    print(f"  {R}^ DROP #{stats.drops}  "
                          f"(consec:{stats.primary_consec_fails}){X}")
                    log_event(ts, "DROP", target=stats.primary_name, proto="v4",
                              lat="N/A", jitter=f"{stats.jitter:.1f}")
                    if stats.primary_consec_fails >= PRIMARY_FAIL_LIMIT:
                        trigger_reelect = True
                else:
                    stats.primary_consec_fails = 0
                    stats.lat_samples.append(latency)
                    stats.recent_lat.append(latency)
                    jitter = stats.jitter
                    if latency > LATENCY_THRESHOLD or jitter > JITTER_THRESHOLD:
                        evt  = "SPIKE" if latency > LATENCY_THRESHOLD else "JITTER"
                        note = (f"lat {latency:.0f}>{LATENCY_THRESHOLD:.0f}ms"
                                if evt == "SPIKE"
                                else f"jit {jitter:.1f}>{JITTER_THRESHOLD:.0f}ms")
                        render_status(ts, latency, jitter, stable=False)
                        print(f"  {Y}^ {evt}: {note}{X}")
                        log_event(ts, evt, target=stats.primary_name, proto="v4",
                                  lat=f"{latency:.1f}", jitter=f"{jitter:.1f}", note=note)
                    else:
                        render_status(ts, latency, jitter, stable=True)

            if trigger_reelect:
                threading.Thread(target=re_elect_primary, daemon=True).start()

            time.sleep(PING_INTERVAL)

    except KeyboardInterrupt:
        end = datetime.now()
        print(f"\n{'-' * 75}")
        write_report(start, end)


if __name__ == "__main__":
    main()
