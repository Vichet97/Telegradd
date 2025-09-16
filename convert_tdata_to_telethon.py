#!/usr/bin/env python3
"""
Convert all Telegram Desktop tdata accounts to Telethon .session files using opentele (CreateNewSession).

- Scans each subfolder in /Users/vichet/Desktop/Telegradd/sessions/TData expected to have a "tdata" directory.
- If a 2Fa.txt file exists in the account folder, its content is used as the password for 2FA.
- Saves resulting .session files to /Users/vichet/Desktop/Telegradd/sessions/telethon_sessions/<account>.session
"""
import asyncio
from pathlib import Path
from typing import Optional, Tuple
import json
import time

from opentele.td import TDesktop
from opentele.api import CreateNewSession, UseCurrentSession
from opentele.tl import TelegramClient as OTClient

# Paths resolved relative to the project (script) directory
BASE_DIR = Path(__file__).resolve().parent
TDATA_ROOT = BASE_DIR / "sessions" / "TData"
OUTPUT_DIR = BASE_DIR / "sessions" / "telethon_sessions"
SESSIONS_JSON_DIR = BASE_DIR / "sessions" / "sessions_json"


def _find_case_insensitive_file(dir_path: Path, target_name: str) -> Optional[Path]:
    tn = target_name.casefold()
    try:
        for child in dir_path.iterdir():
            if child.is_file() and child.name.casefold() == tn:
                return child
    except Exception:
        pass
    return None


def _ensure_sessions_json_dir() -> None:
    try:
        SESSIONS_JSON_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _parse_port_range(raw: str) -> Optional[tuple[int, int]]:
    try:
        parts = raw.strip().split('-', 1)
        if len(parts) != 2:
            return None
        a, b = int(parts[0].strip()), int(parts[1].strip())
        if a <= 0 or b <= 0:
            return None
        if a > b:
            a, b = b, a
        return a, b
    except Exception:
        return None


def _build_proxy_url(country_code: str, port: int) -> str:
    # socks5://<user>:<pass>@gw.dataimpulse.com:<port>
    # User contains the country code token as provided in the template
    return f"socks5://111e06c27a01837df139__cr.{country_code}:9a0479e9a639e5ba@gw.dataimpulse.com:{port}"


def _find_2fa_code_recursively(root: Path) -> Optional[str]:
    """Search recursively under root for any .txt file containing a 6-character string.
    Returns the first match found (preferring first line if multi-line)."""
    try:
        for txt in root.rglob("*.txt"):
            try:
                content = txt.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            # Prefer first non-empty line
            for line in content.splitlines():
                s = line.strip()
                if len(s) == 6:
                    return s
            # Fallback to whole-file trimmed
            s = content.strip()
            if len(s) == 6:
                return s
    except Exception:
        pass
    return None


async def convert_account(account_dir: Path) -> Tuple[str, bool, str]:
    """Convert a single account folder containing a tdata/ directory to a Telethon session.

    Returns tuple: (account_name, success, message)
    """
    tdata_dir = account_dir / "tdata"
    if not tdata_dir.is_dir():
        return account_dir.name, False, "No tdata directory"

    # Ensure output directory exists and determine target session path early
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session_path = OUTPUT_DIR / f"{account_dir.name}.session"

    # If a session already exists, validate it; if valid, skip; if invalid, remove and re-convert
    if session_path.exists():
        existing_client = OTClient(session=str(session_path))
        try:
            await existing_client.connect()
            me = await existing_client.get_me()
            await existing_client.PrintSessions()
            print(me)
            if me:
                return account_dir.name, True, "Existing session valid; skipped"
        except Exception:
            # treat as invalid
            pass
        finally:
            try:
                await existing_client.disconnect()
            except Exception:
                pass
        # remove invalid session file
        try:
            session_path.unlink(missing_ok=True)
        except Exception:
            return account_dir.name, False, "Failed to remove invalid existing session"

    try:
        tdesk = TDesktop(str(tdata_dir))
        if not tdesk.isLoaded():
            return account_dir.name, False, "TDesktop failed to load (not authorized or invalid tdata)"

        # Read optional 2FA password from case-insensitive 2fa.txt
        password: Optional[str] = None
        pw_file = _find_case_insensitive_file(account_dir, "2fa.txt")
        if pw_file and pw_file.is_file():
            try:
                password = pw_file.read_text(encoding="utf-8").strip() or None
            except Exception:
                password = None
        # Fallback: recursively search for a .txt containing a 6-character string
        if password is None:
            password = _find_2fa_code_recursively(account_dir)

        # Perform conversion using CreateNewSession (QR login via existing tdata session)
        client = await tdesk.ToTelethon(session=str(session_path), flag=CreateNewSession, password=password)

        # Connect once to ensure the session is fully initialized and saved
        try:
            await client.connect()
            # Touch the API to ensure authorization completes and session persists
            await client.get_me()
            await client.PrintSessions()
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

        return account_dir.name, True, f"Saved to {session_path}"
    except Exception as e:
        return account_dir.name, False, f"{type(e).__name__}: {e}"


def _update_proxy_json(phone: str, proxy_url: Optional[str]) -> None:
    """Create or update the sessions_json/<phone>.json file with proxy field.
    If file exists, preserves other fields and only updates 'proxy'. If it doesn't exist, creates a minimal record.
    """
    _ensure_sessions_json_dir()
    json_path = SESSIONS_JSON_DIR / f"{phone}.json"
    data = {}
    if json_path.exists():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                data = {}
        except Exception:
            data = {}
    # preserve existing keys; set/update proxy
    data.setdefault("session_file", phone)
    data.setdefault("phone", phone)
    data["proxy"] = proxy_url if proxy_url else None
    # if creating new, add a few sane defaults
    data.setdefault("register_time", int(time.time()))
    data.setdefault("ipv6", False)

    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[Proxy JSON] {'updated' if json_path.exists() else 'created'}: {json_path}")


async def main() -> None:
    if not TDATA_ROOT.is_dir():
        raise SystemExit(f"TData root not found: {TDATA_ROOT}")

    # Ask once for proxy country code and port range
    while True:
        cc = input("Proxy country code (e.g., US): ").strip().upper()
        if cc:
            break
        print("Country code cannot be empty. Please try again.")

    port_start = port_end = None
    while True:
        raw_range = input("Port range (e.g., 10000-20000): ").strip()
        rng = _parse_port_range(raw_range)
        if rng:
            port_start, port_end = rng
            break
        print("Invalid port range. Please use the format start-end, e.g., 10000-20000")

    next_port = port_start

    # Iterate over subdirectories and process those with tdata inside, with a delay between each
    results = []
    entries = [e for e in sorted(TDATA_ROOT.iterdir()) if e.is_dir()]
    for idx, entry in enumerate(entries):
        # Per-account proxy choice
        use_proxy = input(f"Use proxy for {entry.name}? (y/n): ").strip().lower()
        use_proxy_flag = use_proxy.startswith('y')

        account, ok, msg = await convert_account(entry)
        print(f"[{ 'OK' if ok else 'FAIL' }] {account} - {msg}")
        results.append((account, ok, msg))

        # Assign proxy if requested
        if use_proxy_flag:
            # Choose current port and advance; wrap if needed
            if next_port > port_end:
                next_port = port_start
            proxy_url = _build_proxy_url(cc, next_port)
            next_port += 1
        else:
            proxy_url = None

        try:
            _update_proxy_json(account, proxy_url)
            if proxy_url:
                print(f"Assigned proxy -> {proxy_url}")
            else:
                print("No proxy assigned for this account.")
        except Exception as je:
            print(f"[WARN] Failed to write proxy JSON for {account}: {je}")

        if idx < len(entries) - 1:
            await asyncio.sleep(5)

    ok_count = sum(1 for _, ok, _ in results if ok)
    fail_count = len(results) - ok_count
    print(f"Done. Success: {ok_count}, Failed: {fail_count}, Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    asyncio.run(main())