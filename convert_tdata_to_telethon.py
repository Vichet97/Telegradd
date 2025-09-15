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

from opentele.td import TDesktop
from opentele.api import CreateNewSession, UseCurrentSession
from opentele.tl import TelegramClient as OTClient

# Paths resolved relative to the project (script) directory
BASE_DIR = Path(__file__).resolve().parent
TDATA_ROOT = BASE_DIR / "sessions" / "TData"
OUTPUT_DIR = BASE_DIR / "sessions" / "telethon_sessions"


def _find_case_insensitive_file(dir_path: Path, target_name: str) -> Optional[Path]:
    tn = target_name.casefold()
    try:
        for child in dir_path.iterdir():
            if child.is_file() and child.name.casefold() == tn:
                return child
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


async def main() -> None:
    if not TDATA_ROOT.is_dir():
        raise SystemExit(f"TData root not found: {TDATA_ROOT}")

    # Iterate over subdirectories and process those with tdata inside, with a delay between each
    results = []
    entries = [e for e in sorted(TDATA_ROOT.iterdir()) if e.is_dir()]
    for idx, entry in enumerate(entries):
        account, ok, msg = await convert_account(entry)
        print(f"[{ 'OK' if ok else 'FAIL' }] {account} - {msg}")
        results.append((account, ok, msg))
        if idx < len(entries) - 1:
            await asyncio.sleep(5)

    ok_count = sum(1 for _, ok, _ in results if ok)
    fail_count = len(results) - ok_count
    print(f"Done. Success: {ok_count}, Failed: {fail_count}, Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    asyncio.run(main())