import asyncio
import pathlib
import random
import csv
from datetime import datetime
import time
import json
from pathlib import Path
import shutil

from telegradd.adder.main_adder import main_adder, join_group, join_groups
from telegradd.connect.authorisation.main_auth import add_account, check_accounts_via_spambot, view_account, delete_banned, auth_for_test, \
    update_credentials, delete_duplicates_csv, delete_accounts, remove_from_restriction, add_to_restriction
from telegradd.parser.main_parser import parser_page
from telegradd.connect.authorisation.client import TELEGRADD_client
from telegradd.connect.authorisation.databased import Database
from telethon import errors
from telethon.tl.functions.messages import AddChatUserRequest, GetFullChatRequest
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.contacts import ImportContactsRequest, AddContactRequest
from telethon.tl.types import (
    InputPeerChannel, InputPeerUser, ChannelParticipantsAdmins, ChannelParticipantAdmin, ChannelParticipantCreator,
    Chat as TLChat, Channel as TLChannel, InputPeerSelf, InputPeerChannel
)
from telethon.tl.types import InputPhoneContact, InputPeerChannel
# Add participant lookup for verification (use Request class for compatibility)
from telethon.tl.functions.channels import GetParticipantRequest

# Admin capability helpers for promotion workflow
async def _get_self_admin_caps(client, entity):
    await _ensure_connected(client)
    try:
        me = await client.get_me()
        if isinstance(entity, TLChannel):
            try:
                res = await client(GetParticipantRequest(entity, me.id))
                part = getattr(res, 'participant', None)
                if isinstance(part, ChannelParticipantCreator):
                    return {'is_admin': True, 'is_creator': True, 'can_promote': True, 'can_invite': True}
                if isinstance(part, ChannelParticipantAdmin):
                    rights = getattr(part, 'admin_rights', None)
                    can_promote = bool(getattr(rights, 'add_admins', False)) if rights else False
                    can_invite = bool(getattr(rights, 'invite_users', False)) if rights else False
                    return {'is_admin': True, 'is_creator': False, 'can_promote': can_promote, 'can_invite': can_invite}
                return {'is_admin': False}
            except Exception:
                return {'is_admin': False}
        elif isinstance(entity, TLChat):
            try:
                full = await client(GetFullChatRequest(entity.id))
                parts = getattr(full.full_chat, 'participants', None)
                is_admin = False
                if parts and hasattr(parts, 'participants'):
                    for p in parts.participants:
                        if getattr(p, 'user_id', None) == getattr(me, 'id', None):
                            if p.__class__.__name__ in ('ChatParticipantAdmin', 'ChatParticipantCreator'):
                                is_admin = True
                                break
                return {'is_admin': is_admin, 'is_creator': False, 'can_promote': is_admin, 'can_invite': is_admin}
            except Exception:
                return {'is_admin': False}
        else:
            try:
                res = await client(GetParticipantRequest(entity, me.id))
                part = getattr(res, 'participant', None)
                if isinstance(part, ChannelParticipantCreator):
                    return {'is_admin': True, 'is_creator': True, 'can_promote': True, 'can_invite': True}
                if isinstance(part, ChannelParticipantAdmin):
                    rights = getattr(part, 'admin_rights', None)
                    can_promote = bool(getattr(rights, 'add_admins', False)) if rights else False
                    can_invite = bool(getattr(rights, 'invite_users', False)) if rights else False
                    return {'is_admin': True, 'is_creator': False, 'can_promote': can_promote, 'can_invite': can_invite}
                return {'is_admin': False}
            except Exception:
                return {'is_admin': False}
    except Exception:
        return {'is_admin': False}

async def _is_user_admin_in_target(client, target_entity, user_id):
    await _ensure_connected(client)
    try:
        if isinstance(target_entity, TLChannel):
            try:
                res = await client(GetParticipantRequest(target_entity, user_id))
                part = getattr(res, 'participant', None)
                return isinstance(part, (ChannelParticipantCreator, ChannelParticipantAdmin))
            except errors.UserNotParticipantError:
                return False
            except Exception:
                return False
        elif isinstance(target_entity, TLChat):
            try:
                full = await client(GetFullChatRequest(target_entity.id))
                parts = getattr(full.full_chat, 'participants', None)
                if parts and hasattr(parts, 'participants'):
                    for p in parts.participants:
                        if getattr(p, 'user_id', None) == user_id and p.__class__.__name__ in ('ChatParticipantAdmin', 'ChatParticipantCreator'):
                            return True
                return False
            except Exception:
                return False
        else:
            try:
                res = await client(GetParticipantRequest(target_entity, user_id))
                part = getattr(res, 'participant', None)
                return isinstance(part, (ChannelParticipantCreator, ChannelParticipantAdmin))
            except Exception:
                return False
    except Exception:
        return False

async def _promote_user_to_inviter_admin(client, target_entity, user_id, rank: str = 'Admin'):
    await _ensure_connected(client)
    try:
        if isinstance(target_entity, TLChannel):
            rights = ChatAdminRights(add_admins=True, invite_users=True)
            try:
                await client(EditAdminRequest(target_entity, user_id, rights, rank))
                return True
            except Exception:
                return False
        elif isinstance(target_entity, TLChat):
            try:
                await client(EditChatAdminRequest(target_entity.id, user_id, True))
                return True
            except Exception:
                return False
        else:
            rights = ChatAdminRights(add_admins=True, invite_users=True)
            try:
                await client(EditAdminRequest(target_entity, user_id, rights, rank))
                return True
            except Exception:
                return False
    except Exception:
        return False

# TData -> Telethon converter (opentele)
from convert_tdata_to_telethon import convert_account as _convert_tdata_account
from convert_tdata_to_telethon import TDATA_ROOT as _TDATA_ROOT, OUTPUT_DIR as _TL_OUT_DIR
from telegradd.connect.authorisation.system import WindowsDevice
from telegradd.connect.authorisation.app_id_hash import Apps
from telethon.tl.functions.channels import JoinChannelRequest, EditAdminRequest
from telethon.tl.functions.messages import EditChatAdminRequest
from telethon.tl.types import ChatAdminRights

# Feature toggles (configure behavior without code changes)
ADD_CONTACT_FALLBACK_ENABLED = True   # If True, on not mutual/invalid id, try ImportContacts by phone then retry invite
POST_INVITE_VERIFY_ENABLED = True     # If True, verify membership after inviting and log verification path

# Users directory for CSV persistence
_USERS_DIR = Path(pathlib.Path(__file__).resolve().parent, 'telegradd', 'users')
try:
    _USERS_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass

def _find_case_insensitive_file(dir_path: Path, target_name: str) -> Path | None:
    tn = target_name.casefold()
    try:
        for child in dir_path.iterdir():
            if child.is_file() and child.name.casefold() == tn:
                return child
    except Exception:
        pass
    return None


banners = [
    """
        ████████╗███████╗██╗     ███████╗ ██████╗ ██████╗  █████╗ ██████╗ ██████╗           
        ╚══██╔══╝██╔════╝██║     ██╔════╝██╔════╝ ██╔══██╗██╔══██╗          
           ██║   █████╗  ██║     █████╗  ██║  ███╗██████╔╝███████║██║  ██║██║  ██║          
           ██║   ██╔══╝  ██║     ██╔══╝  ██║   ██║██╔══██╗██╔══██╗██║  ██║██║  ██║          
           ██║   ███████╗███████╗███████╗╚██████╔╝██║  ██║██║  ██║██████╔╝██████╔╝          
           ╚═╝   ╚════════╝╚══════╝╚══════╝ ╚═╝  ╚═╝╚═╝  ╚═╝           
                                                                     By EvilBream
                                                          Telegram @malevolentkid
    """,
    """"
  88888888888 8888888888 888      8888888888  .d8888b.  8888888b.         d8888 8888888b.  8888888b.  
      888     888        888      888        d88P  Y88b 888   Y88b       d88888 888  "Y88b 888  "Y88b 
      888     888        888      888        888    888 888    888      d88P888 888    888 888    888 
      888     8888888    888      8888888    888        888   d88P     d88P 888 888    888 888    888 
      888     888        888      888        888  88888 8888888P"     d88P  888 888    888 888    888 
      888     888        888      888        888    888 888 T88b     d88P   888 888    888 888    888 
      888     888        888      888        Y88b  d88P 888  T88b   d8888888888 888  .d88P 888  .d88P 
      888     8888888888 88888888 8888888888  "Y8888P88 888   T88b d88P     888 8888888P"  8888888P"                                                                                              
                                                                               Telegram @malevolentkid      
    """
]

update_option = "  UPDATE OPTIONS:\n" \
                " (1) Update AP_ID\n" \
                " (2) Update AP_HASH\n" \
                " (3) Update Proxy\n" \
                " (4) Update System\n" \
                " (5) Update Password\n" \
                " (6) Update Restriction\n" \
                " (7) Update Phone\n"

# Ensure a Telethon client is connected and ready
async def _ensure_connected(client):
    try:
        if not client.is_connected():
            await client.connect()
    except Exception:
        # attempt reconnect
        try:
            await client.connect()
        except Exception:
            pass


def _ensure_dirs():
    base = Path(__file__).resolve().parent
    (base / 'sessions' / 'telethon_sessions').mkdir(parents=True, exist_ok=True)
    (base / 'sessions' / 'sessions_json').mkdir(parents=True, exist_ok=True)


def _generate_json_for_account(account_dir: Path) -> tuple[str, bool, str]:
    try:
        base = Path(__file__).resolve().parent
        json_out_dir = base / 'sessions' / 'sessions_json'
        json_out_dir.mkdir(parents=True, exist_ok=True)
        name = account_dir.name
        dest = json_out_dir / f"{name}.json"
        # If JSON already exists, do not override
        if dest.exists():
            return name, True, f"JSON already exists, skipped: {dest}"
        
        # Defaults
        device_str = WindowsDevice().device_list  # format: Model:OS:AppVer
        model, sdk, app_version = device_str.split(':', 2)
        app_id, app_hash = Apps().app_info
        now_ts = int(time.time())

        data = {
            "session_file": name,
            "phone": name,
            "register_time": now_ts,
            "app_id": app_id,
            "app_hash": app_hash,
            "sdk": sdk,
            "app_version": app_version,
            "device": model,
            "lang_pack": "tdesktop",
            "system_lang_pack": "en-US",
            "proxy": None,
            "first_name": "",
            "last_name": None,
            "username": None,
            "ipv6": False,
            "twoFA": ""
        }

        # Try to read enrichment JSON inside TData folder: <name>.json
        tjson = account_dir / f"{name}.json"
        if tjson.is_file():
            try:
                with open(tjson, 'r', encoding='utf-8') as f:
                    src = json.load(f)
                data["phone"] = src.get("phone") or data["phone"]
                data["app_id"] = src.get("app_id") or data["app_id"] or src.get("api_id")
                data["app_hash"] = src.get("app_hash") or data.get("app_hash")
                data["device"] = src.get("device_model") or src.get("device") or data["device"]
                data["sdk"] = src.get("system_version") or src.get("SDK") or src.get("sdk") or data["sdk"]
                data["app_version"] = src.get("app_version") or data["app_version"]
                data["system_lang_pack"] = src.get("system_lang_pack") or data["system_lang_pack"]
                data["lang_pack"] = src.get("lang_pack") or data["lang_pack"]
                data["username"] = src.get("username")
                data["first_name"] = src.get("first_name") or data["first_name"]
                data["last_name"] = src.get("last_name")
                data["register_time"] = src.get("register_time") or data["register_time"]
                # twoFA from file if present
                data["twoFA"] = src.get("twofa") or src.get("password") or data["twoFA"]
            except Exception:
                pass
        # 2Fa txt file (case-insensitive)
        pw_file = _find_case_insensitive_file(account_dir, "2fa.txt")
        if pw_file and pw_file.is_file():
            try:
                data["twoFA"] = pw_file.read_text(encoding="utf-8").strip()
            except Exception:
                pass
        else:
            # Fallback: search recursively for any .txt file whose content length is 6
            try:
                for cand in account_dir.rglob('*'):
                    try:
                        if cand.is_file() and cand.suffix.lower() == '.txt':
                            txt = cand.read_text(encoding="utf-8").strip()
                            if len(txt) == 6:
                                data["twoFA"] = txt
                                break
                    except Exception:
                        # ignore unreadable files and continue
                        pass
            except Exception:
                # ignore traversal errors
                pass

        with open(dest, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return name, True, f"JSON saved to {dest}"
    except Exception as e:
        return account_dir.name, False, f"{type(e).__name__}: {e}"


async def convert_tdata_to_sessions_and_json():
    _ensure_dirs()
    base = Path(__file__).resolve().parent
    root = _TDATA_ROOT
    if not Path(root).is_dir():
        print(f"TData root not found: {root}")
        return
    results = []
    for entry in sorted(Path(root).iterdir()):
        if not entry.is_dir():
            continue
        # If JSON already exists for this account, skip conversion and copying
        name = entry.name
        json_dest = base / 'sessions' / 'sessions_json' / f"{name}.json"
        if json_dest.is_file():
            print(f"[SKIP] {name} - JSON already exists, skipping conversion and copy")
            continue
        # Run conversion to Telethon session
        try:
            name, ok, msg = await _convert_tdata_account(entry)
            print(f"[{ 'OK' if ok else 'FAIL' }] {name} - {msg}")
            results.append((name, ok, msg))
            # After successful conversion, copy the generated .session to sessions_json as well
            if ok:
                src = _TL_OUT_DIR / f"{name}.session"
                dst_dir = base / 'sessions' / 'sessions_json'
                dst_dir.mkdir(parents=True, exist_ok=True)
                dst = dst_dir / f"{name}.session"
                try:
                    shutil.copy2(src, dst)
                    print(f"[OK] {name} (copy) - session copied to {dst}")
                except Exception as ce:
                    print(f"[FAIL] {name} (copy) - {type(ce).__name__}: {ce}")
        except Exception as e:
            name = entry.name
            print(f"[FAIL] {name} - {type(e).__name__}: {e}")
            results.append((name, False, str(e)))
        # Generate JSON sidecar
        jn, jok, jmsg = _generate_json_for_account(entry)
        print(f"[{ 'OK' if jok else 'FAIL' }] {jn} (json) - {jmsg}")

    ok_count = sum(1 for _, ok, _ in results if ok)
    fail_count = len(results) - ok_count
    print(f"Done. Telethon sessions: {ok_count} ok, {fail_count} fail. Output: {_TL_OUT_DIR}")
    print(f"JSON files saved under: {Path(__file__).resolve().parent / 'sessions' / 'sessions_json'}")


# Ensure a Telethon client is connected and ready
async def _ensure_connected(client):
    try:
        if not client.is_connected():
            await client.connect()
    except Exception:
        try:
            await client.connect()
        except Exception:
            pass


def home_page():
    page_text = "\n\n" \
                f"{random.choice (banners)}\n" \
                " LOGIN OPTIONS:\n" \
                " (1) Login with Phone Number\n" \
                " (2) Load Sessions  json files\n" \
                " (3) Load Tdata\n" \
                " (4) Load Pyrogram Sessions\n" \
                " (5) Load Telethon Sessions\n\n" \
                " SCRAPER OPTIONS:\n" \
                " (6) Participants Group Scraper\n" \
                " (7) Hidden Participants Scarper\n" \
                " (8) Comments Participants Scarper\n\n" \
                " ADDER OPTIONS:\n" \
                " (9) Add by Id\n" \
                " (10) Add by Username\n\n"\
                " ADDITIONAL OPTIONS:\n" \
                " (11) Warm Up Mode\n" \
                " (12) Delete banned accounts\n" \
                " (13) List accounts\n" \
                " (14) Join Chat(s) (comma-separated)\n" \
                " (15) Change Proxy/Password/Etc\n" \
                " (16) Test Authorisation\n" \
                " (17) Delete Duplicates\n" \
                " (18) Delete Account(s)\n" \
                " (19) Check Accounts Status via @SpamBot\n" \
                " (20) Remove from Restriction\n" \
                " (21) Add to Restriction\n" \
                " (22) Convert TData -> telethon_sessions  sessions_json\n" \
                " (23) Promote members to Admin (with Invite rights)\n" \
                 " (0)  Exit\n\n"
    print (page_text)
    usr_raw_input = input ("Choose an option (0-23) ~# ")
    try:
        option = int (usr_raw_input)
    except:
        option = 0
    if not (0 <= option <= 23):
        print ("You choose wrong option! try again ...")
        home_page()
    if 1 <= option <= 5:
        add_account(option)
    elif 6 <= option <= 8:
        # Run enhanced pipeline that includes source/target selection, account selection, limits and CSV output
        try:
            asyncio.run(enhanced_add_workflow())
        except KeyboardInterrupt:
            print('\nCancelled by user. Returning to menu...')
            return
    elif option == 9:
        asyncio.run(main_adder(how_to_add='id'))
    elif option == 10:
        asyncio.run(main_adder())
    elif option == 11:
        print('in progress')
    elif option == 12:
        delete_banned()
    elif option == 13:
        view_account()
    elif option == 14:
        asyncio.run(join_group())
    elif option == 15:
        print(update_option)
        raw_input = input ("Choose an option (1-7) ~# ")
        try:
            opt = int (raw_input)
            if not (1 <= opt <= 7):
                print ("You choose wrong option! try again ...")
                exit (0)
            update_credentials(opt)
        except:
            print ("Wrong option ...")
            exit (0)
    elif option == 16:
        asyncio.run(auth_for_test())
    elif option == 17:
        delete_duplicates_csv()
    elif option == 18:
        delete_accounts()
    elif option == 19:
        asyncio.run(check_accounts_via_spambot())
    elif option == 20:
        remove_from_restriction()
    elif option == 21:
        add_to_restriction()
    elif option == 22:
        asyncio.run(convert_tdata_to_sessions_and_json())
    elif option == 23:
        try:
            asyncio.run(promote_admin_workflow())
        except KeyboardInterrupt:
            print('\nCancelled by user. Returning to menu...')
            return
    elif option == 0:
        exit(0)


# Helper: choose a group from dialogs
async def _choose_group_dialog(client, prompt="Choose a group: "):
    dialogs = {}
    idx = 1
    await _ensure_connected(client)
    async for dialog in client.iter_dialogs():
        if dialog.is_group:
            dialogs[idx] = (dialog.id, getattr(dialog.entity, 'access_hash', None), dialog.name, getattr(dialog.entity, 'username', None))
            print(f"{idx}. {dialog.name}")
            idx += 1
    if not dialogs:
        print("No groups found for this account.")
        return None
    # Add back navigation option
    print("0. Back")
    while True:
        ch = input(f"{prompt}(0 to go back): ").strip()
        if ch == '0':
            return None
        if ch.isdigit():
            n = int(ch)
            if n in dialogs:
                return dialogs[n]
        print("Invalid choice, try again.")


# Helper: fetch members from source group
async def _fetch_source_members(client, group_id, limit=None):
    members = []
    await _ensure_connected(client)
    admin_ids = set()
    try:
        async for admin in client.iter_participants(group_id, filter=ChannelParticipantsAdmins()):
            admin_ids.add(admin.id)
    except Exception:
        admin_ids = set()
    async for user in client.iter_participants(group_id, limit=limit):
        if getattr(user, 'bot', False):
            continue
        # Exclude creators/admins if role information is available on the participant object
        p = getattr(user, 'participant', None)
        if p is not None and isinstance(p, (ChannelParticipantAdmin, ChannelParticipantCreator)):
            continue
        if user.id in admin_ids:
            continue
        members.append({
            'user_id': user.id,
            'first_name': getattr(user, 'first_name', '') or '',
            'username': getattr(user, 'username', None),
            'phone': getattr(user, 'phone', None),
            'access_hash': getattr(user, 'access_hash', None),  # capture access_hash to build InputPeerUser later
        })
    return members


# Helper: prefetch a user's InputPeerUser by hitting GetParticipantRequest to warm cache
async def _prefetch_user_from_source(client, source_group_id, user_id):
    await _ensure_connected(client)
    try:
        # Warm up the session cache with participant info; ignore failures
        await client(GetParticipantRequest(source_group_id, int(user_id)))
    except Exception:
        pass
    try:
        ip = await client.get_input_entity(int(user_id))
        uid = getattr(ip, 'user_id', None)
        uhash = getattr(ip, 'access_hash', None)
        if uid is not None and uhash is not None:
            return InputPeerUser(int(uid), int(uhash))
    except Exception:
        return None
    return None


def _format_member_label(rec: dict) -> str:
    name = (rec.get('first_name') or '').strip()
    username = rec.get('username')
    phone = rec.get('phone')
    if name and username:
        return f"{name} (@{username})"
    if name and phone:
        return f"{name} ({phone})"
    if name:
        return name
    if username:
        return f"@{username}"
    if phone:
        return str(phone)
    return str(rec.get('user_id'))

# Helper: existing member ids of target group
async def _get_existing_member_ids(client, group_id):
    exist = set()
    await _ensure_connected(client)
    try:
        async for user in client.iter_participants(group_id):
            exist.add(user.id)
    except (errors.UserNotParticipantError, errors.ChatAdminRequiredError, errors.ChannelPrivateError, errors.ChannelInvalidError, errors.ChatForbiddenError, errors.ChatWriteForbiddenError):
        # If we cannot list participants due to membership/permission issues, fall back to empty set
        return set()
    except Exception:
        # Unknown issues fetching participant list; be safe and return empty
        return set()
    return exist


# Helper: check if a user is a member of the given entity (group/channel)
async def _is_member(client, entity, user_id):
    if entity is None or user_id is None:
        return False
    await _ensure_connected(client)
    try:
        # Supergroups/channels
        if isinstance(entity, TLChannel):
            try:
                await client(GetParticipantRequest(entity, user_id))
                return True
            except errors.UserNotParticipantError:
                return False
            except Exception:
                # Unknown error while checking; return None-equivalent to signal uncertainty
                return None
        # Normal small groups (chats)
        elif isinstance(entity, TLChat):
            try:
                full = await client(GetFullChatRequest(entity.id))
                parts = getattr(full.full_chat, 'participants', None)
                if parts and hasattr(parts, 'participants'):
                    for p in parts.participants:
                        if getattr(p, 'user_id', None) == user_id:
                            return True
                return False
            except Exception:
                return None
        # Fallback: try channel-style lookup
        else:
            try:
                await client(GetParticipantRequest(entity, user_id))
                return True
            except errors.UserNotParticipantError:
                return False
            except Exception:
                return None
    except Exception:
        return None

# Helper: resolve target group entity/channel
async def _resolve_target_channel(client, target_choice):
    await _ensure_connected(client)
    # If user typed a URL or username
    if target_choice.startswith('http') or target_choice.startswith('@'):
        entity = await client.get_entity(target_choice)
    else:
        # assume numeric id
        try:
            entity = await client.get_entity(int(target_choice))
        except Exception:
            # fallback to treat as username
            entity = await client.get_entity(target_choice)
    return entity


# Helper: add single user with error handling; returns True on success, False on non-countable failure,
# raises on flood/restriction-worthy errors
async def _invite_one(client, target_entity, user_rec, source_group_id=None, verify=True, verbose=False, allow_contact_fallback=True):
    await _ensure_connected(client)
    try:
        # Prefer using access_hash captured from source to avoid cross-session resolution issues
        input_user = None
        resolution_steps = []  # track how we resolved InputPeerUser (for logging)
        # Attempt prefetch via source group to warm cache and get InputPeerUser
        if source_group_id and user_rec.get('user_id'):
            try:
                pref = await _prefetch_user_from_source(client, source_group_id, int(user_rec['user_id']))
                if pref:
                    input_user = pref
                    resolution_steps.append('prefetched_from_source')
            except Exception:
                pass
        if input_user is None:
            # Prefer session-local resolution first to avoid cross-session hash issues
            try:
                ip = await client.get_input_entity(int(user_rec['user_id']))
                uid = getattr(ip, 'user_id', None)
                uhash = getattr(ip, 'access_hash', None)
                if uid is not None and uhash is not None:
                    input_user = InputPeerUser(int(uid), int(uhash))
                    resolution_steps.append('get_input_entity')
            except Exception:
                pass
        if input_user is None and user_rec.get('username'):
            try:
                usr_ent = await client.get_entity(user_rec['username'])
                input_user = InputPeerUser(usr_ent.id, usr_ent.access_hash)
                resolution_steps.append('resolved_username')
            except Exception:
                input_user = None
        if input_user is None and user_rec.get('access_hash'):
            try:
                input_user = InputPeerUser(int(user_rec['user_id']), int(user_rec['access_hash']))
                resolution_steps.append('from_record')
            except Exception:
                input_user = None
        if input_user is None:
            return ('error', 'Could not resolve input user for invitation')

        async def do_invite(iu):
            # Refresh InputPeerUser with a session-local access_hash if possible to avoid cross-session mismatches
            try:
                base_uid = getattr(iu, 'user_id', None)
                if base_uid is None:
                    base_uid = int(user_rec.get('user_id')) if user_rec.get('user_id') else None
                if base_uid is not None:
                    ip2 = await client.get_input_entity(int(base_uid))
                    uid2 = getattr(ip2, 'user_id', None)
                    uhash2 = getattr(ip2, 'access_hash', None)
                    if uid2 is not None and uhash2 is not None:
                        iu = InputPeerUser(int(uid2), int(uhash2))
                        resolution_steps.append('refreshed_access_hash')
            except Exception as e:
                pass

            if isinstance(target_entity, TLChannel):
                channel = InputPeerChannel(target_entity.id, target_entity.access_hash)
                await client(InviteToChannelRequest(channel, [iu]))
            elif isinstance(target_entity, TLChat):
                await client(AddChatUserRequest(target_entity.id, iu, fwd_limit=0))
            else:
                channel = InputPeerChannel(target_entity.id, getattr(target_entity, 'access_hash', None))
                await client(InviteToChannelRequest(channel, [iu]))

            await asyncio.sleep(10)
        try:
            await do_invite(input_user)
            details = ''
            if verify:
                try:
                    uid_for_check = int(user_rec.get('user_id')) if user_rec.get('user_id') else None
                except Exception:
                    uid_for_check = None
                verified = None
                if uid_for_check:
                    try:
                        checked = await _is_member(client, target_entity, uid_for_check)
                        verified = bool(checked) if checked is not None else None
                    except Exception:
                        verified = None
                path_info = ','.join(resolution_steps) if resolution_steps else 'direct'
                if verified is True:
                    details = f'verified=True path={path_info}'
                elif verified is False:
                    details = f'verified=False path={path_info}'
                else:
                    details = f'verified=unknown path={path_info}'
            return ('added', details)
        except errors.UserIdInvalidError as e:
            # Handle invalid object id as a resolvable case (not 'not mutual'): retry via source-prefetch, username, then add-contact
            msg = str(e) or ''
            if 'Invalid object ID for a user' in msg or isinstance(e, errors.UserIdInvalidError):
                # 1) Try prefetching participant from source to warm cache and obtain correct access_hash
                try:
                    pref_iu = await _prefetch_user_from_source(client, source_group_id, int(user_rec['user_id']))
                except Exception:
                    pref_iu = None
                if pref_iu:
                    try:
                        await do_invite(pref_iu)
                        details = ''
                        if verify:
                            try:
                                checked = await _is_member(client, target_entity, int(user_rec.get('user_id') or 0))
                                v = bool(checked) if checked is not None else None
                                path_info = ','.join([*resolution_steps, 'prefetched_from_source_retry'])
                                if v is True:
                                    details = f'verified=True path={path_info}'
                                elif v is False:
                                    details = f'verified=False path={path_info}'
                                else:
                                    details = f'verified=unknown path={path_info}'
                            except Exception:
                                details = f"verified=unknown path={'prefetched_from_source_retry'}"
                        return ('added', details)
                    except Exception:
                        pass
                # 2) Attempt username re-resolve if available
                try:
                    if user_rec.get('username'):
                        usr_ent = await client.get_entity(user_rec['username'])
                        retry_iu = InputPeerUser(usr_ent.id, usr_ent.access_hash)
                        await do_invite(retry_iu)
                        details = ''
                        if verify:
                            try:
                                checked = await _is_member(client, target_entity, int(user_rec.get('user_id') or 0))
                                v = bool(checked) if checked is not None else None
                                path_info = ','.join([*resolution_steps, 'resolved_username_retry'])
                                if v is True:
                                    details = f'verified=True path={path_info}'
                                elif v is False:
                                    details = f'verified=False path={path_info}'
                                else:
                                    details = f'verified=unknown path={path_info}'
                            except Exception:
                                details = f"verified=unknown path={'resolved_username_retry'}"
                        return ('added', details)
                except Exception:
                    pass
                # 3) Attempt to add contact by id/hash to obtain access and retry
                if allow_contact_fallback:
                    try:
                        ip = await client.get_input_entity(int(user_rec['user_id']))
                        uid = getattr(ip, 'user_id', None)
                        uhash = getattr(ip, 'access_hash', None)
                        if uid is not None and uhash is not None:
                            retry_iu = InputPeerUser(int(uid), int(uhash))
                            # Add contact by id/hash before retrying invite
                            try:
                                first_name = user_rec.get('first_name') or 'User'
                                last_name = user_rec.get('last_name') or ''
                                await client(AddContactRequest(id=retry_iu, first_name=first_name, last_name=last_name, phone='', add_phone_privacy_exception=False))
                                await asyncio.sleep(2)
                            except Exception:
                                pass
                            await do_invite(retry_iu)
                            details = ''
                            if verify:
                                try:
                                    checked = await _is_member(client, target_entity, int(user_rec['user_id']))
                                    v = bool(checked) if checked is not None else None
                                    path_info = ','.join([*resolution_steps, 'add_contact_by_id'])
                                    if v is True:
                                        details = f'verified=True path={path_info}'
                                    elif v is False:
                                        details = f'verified=False path={path_info}'
                                    else:
                                        details = f'verified=unknown path={path_info}'
                                except Exception:
                                    details = f"verified=unknown path={'add_contact_by_id'}"
                            return ('added', details)
                    except Exception:
                        pass
                # If still failing after retries, classify distinctly (not as 'not mutual')
                return ('skipped_invalid_id', msg or 'Invalid object ID for a user')
            # If not recoverable, bubble as error result for logging
            return ('error', f"Invite failed: {msg or e.__class__.__name__}")
        except errors.UserPrivacyRestrictedError as e:
            return ('skipped_privacy', 'User restricts adding them to groups; invite via link instead')
        except errors.UserNotMutualContactError:
            # Fallback: add contact by id/hash and retry
            if allow_contact_fallback:
                try:
                    ip = await client.get_input_entity(int(user_rec['user_id']))
                    uid = getattr(ip, 'user_id', None)
                    uhash = getattr(ip, 'access_hash', None)
                    if uid is not None and uhash is not None:
                        retry_iu = InputPeerUser(int(uid), int(uhash))
                        # Add contact by id/hash before retry
                        try:
                            first_name = user_rec.get('first_name') or 'User'
                            last_name = user_rec.get('last_name') or ''
                            await client(AddContactRequest(id=retry_iu, first_name=first_name, last_name=last_name, phone='', add_phone_privacy_exception=False))
                            await asyncio.sleep(2)
                        except Exception:
                            pass
                        await do_invite(retry_iu)
                        path_info = ','.join([*resolution_steps, 'add_contact_by_id_on_not_mutual']) if resolution_steps else 'add_contact_by_id_on_not_mutual'
                        details = f'verified=unknown path={path_info}'
                        if verify:
                            try:
                                checked = await _is_member(client, target_entity, int(user_rec['user_id']))
                                v = bool(checked) if checked is not None else None
                                if v is True:
                                    details = f'verified=True path={path_info}'
                                elif v is False:
                                    details = f'verified=False path={path_info}'
                            except Exception:
                                pass
                        return ('added', details)
                except Exception:
                    pass
            return ('skipped_not_mutual', 'tried add_contact_by_id but failed')
        except errors.BadRequestError as e:
            # Ensure generic BadRequestError (including NotMutualContact text) does NOT cause account restriction
            msg = str(e) or ''
            low = msg.lower()
            if 'not a mutual contact' in low or 'not mutual contact' in low:
                return ('skipped_not_mutual', msg)
            # Fallback to error for other BadRequestError cases
            return ('error', f"Invite failed: {msg or e.__class__.__name__}")
        except errors.UserChannelsTooMuchError:
            return ('skipped_too_many_channels', '')
        except errors.UserKickedError:
            return ('skipped_kicked', '')
        except errors.PeerFloodError as e:
            raise e
        except errors.FloodWaitError as e:
            raise e
        except errors.UserBannedInChannelError as e:
            raise e
        except errors.UserBlockedError as e:
            return ('skipped_blocked', str(e))
        except Exception as e:
            return ('error', str(e))
    except Exception as e:
        return ('error', str(e))


async def enhanced_add_workflow():
    print("Enhanced Pipeline: Source -> Target adding with account distribution and limits")
    # 1) Pick a browsing account to choose source group
    print("Select an account to browse groups for source selection:")
    Database().view_all(admin=False)
    browse_raw = input('Enter a single account number to browse (digit, 0 to go back): ').strip()
    if browse_raw == '0':
        print('Returning to previous menu...')
        return
    while not browse_raw.isdigit():
        browse_raw = input('Enter a single account number to browse (digit, 0 to go back): ').strip()
        if browse_raw == '0':
            print('Returning to previous menu...')
            return
    browse_num = int(browse_raw)
    browse_clients = await TELEGRADD_client((browse_num,)).clients(restriction=False)
    if not browse_clients:
        print('Cannot authenticate the browsing account.')
        return
    browse_client = browse_clients[0]
    await _ensure_connected(browse_client)

    src = await _choose_group_dialog(browse_client, prompt='Choose source group number: ')
    if src is None:
        print('No source group available or operation cancelled.')
        return
    src_group_id = src[0]
    src_group_name = src[2]
    src_username = src[3]
    src_link_hint = f"https://t.me/{src_username}" if src_username else str(src_group_id)

    # Do not fetch members here; each account will fetch its own source members independently
    print(f"Selected source group: {src_group_name} (ID: {src_group_id}). Each account will fetch members independently.")
    print(f"Source link hint: {src_link_hint}")

    # 2) Target group selection
    print("Select target group from your dialogs below or paste link/@username.")
    await _ensure_connected(browse_client)
    tgt_dialogs = {}
    idx = 1
    async for dialog in browse_client.iter_dialogs():
        if dialog.is_group:
            tgt_dialogs[idx] = (dialog.id, getattr(dialog.entity, 'access_hash', None), dialog.name, getattr(dialog.entity, 'username', None))
            print(f"{idx}. {dialog.name}")
            idx += 1
    if not tgt_dialogs:
        print('No target group available for this account.')
        return
    print("0. Back")
    tgt_choice = input('Enter number or paste link/@username (0 to go back): ').strip()
    if tgt_choice == '0':
        print('Returning to previous menu...')
        return
    if tgt_choice.isdigit() and int(tgt_choice) in tgt_dialogs:
        tgt = tgt_dialogs[int(tgt_choice)]
        target_entity = await browse_client.get_entity(int(tgt[0]))
        target_link_hint = f"https://t.me/{tgt[3]}" if tgt[3] else str(tgt[0])
    else:
        target_entity = await browse_client.get_entity(tgt_choice)
        target_link_hint = tgt_choice

    target_group_name = getattr(target_entity, 'title', None) or getattr(target_entity, 'username', None) or str(target_entity.id)
    target_group_id = target_entity.id

    # 3) Account selection for adding
    print("Select accounts for adding:")
    Database().view_all(admin=False)
    raw = input("Enter digits (all/ranges like 2-5/specific space-separated, 0 to go back): ").lower().strip()
    if raw == '0':
        print('Returning to previous menu...')
        return
    tokens = [t for t in raw.split(' ') if t]
    selected = []
    if tokens and tokens[0] == 'all':
        auth_tuple = ('all',)
    else:
        for t in tokens:
            if '-' in t:
                a, b = (part.strip() for part in t.split('-', 1))
                if a.isdigit() and b.isdigit():
                    start, end = int(a), int(b)
                    if start <= end:
                        selected.extend(range(start, end + 1))
                    else:
                        selected.extend(range(end, start + 1))
            elif t.isdigit():
                selected.append(int(t))
        selected = sorted(set(selected))
        auth_tuple = tuple(selected) if selected else None
    if auth_tuple is None:
        print('No valid accounts selected.')
        return

    # Automatically exclude restricted accounts
    clients = await TELEGRADD_client(auth_tuple).clients(restriction=True)
    if not clients:
        print('No available (unrestricted) accounts found.')
        return

    # Per requirement: do NOT auto-join target; skip accounts that are not current members of selected groups

    # Daily limit per account
    try:
        daily_raw = input('Set daily adding limit per account [default 5] (0 to go back): ').strip()
    except (KeyboardInterrupt, EOFError):
        print('\nCancelled by user. Returning to menu...')
        return

    if daily_raw == '0':
        print('Returning to previous menu...')
        return

    if daily_raw == '':
        daily_limit = 5
    else:
        limit_set = False
        if daily_raw.isdigit():
            daily_limit = int(daily_raw)
            limit_set = True
        while not limit_set and not daily_raw.isdigit():
            try:
                daily_raw = input('Set daily adding limit per account (digit, blank for 5, 0 to go back): ').strip()
            except (KeyboardInterrupt, EOFError):
                print('\nCancelled by user. Returning to menu...')
                return
            if daily_raw == '0':
                print('Returning to previous menu...')
                return
            if daily_raw == '':
                daily_limit = 5
                limit_set = True
            elif daily_raw.isdigit():
                daily_limit = int(daily_raw)
                limit_set = True

    # Prepare CSV persistence and pre-load processed ids for this target group
    _CSV_PATH = _USERS_DIR / 'added_results.csv'
    _CSV_HEADER = ['user_id', 'username', 'group_name', 'group_id', 'member_label', 'status', 'full error message']
    
    # Track existing (user_id, group_id) pairs to prevent duplicate rows in CSV
    existing_pairs = set()
    
    
    def _ensure_added_csv_header():
        try:
            # If file doesn't exist or empty, create with correct header
            if not _CSV_PATH.exists() or _CSV_PATH.stat().st_size == 0:
                with open(_CSV_PATH, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(_CSV_HEADER)
                return
            # If exists, verify header and upgrade if needed (non-destructive)
            with open(_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
                reader = csv.reader(f)
                try:
                    current_header = next(reader)
                except StopIteration:
                    current_header = []
            if current_header != _CSV_HEADER:
                # Upgrade: read existing rows with DictReader using current_header, then rewrite with new header
                tmp_path = _CSV_PATH.with_suffix('.tmp.csv')
                rows = []
                try:
                    with open(_CSV_PATH, 'r', newline='', encoding='utf-8') as rf:
                        if current_header:
                            dict_reader = csv.DictReader(rf, fieldnames=current_header)
                            # If first row was header, skip it
                            first_row = next(dict_reader, None)
                            # Heuristic: if first_row values equal headers, skip, else include
                            if first_row and any(first_row.get(h) for h in current_header if h is not None):
                                # It was data; keep it
                                rows.append(first_row)
                            for r in dict_reader:
                                rows.append(r)
                        else:
                            # No header; treat as no data
                            rows = []
                except Exception:
                    rows = []
                # Write new file with proper header
                with open(tmp_path, 'w', newline='', encoding='utf-8') as wf:
                    writer = csv.writer(wf)
                    writer.writerow(_CSV_HEADER)
                    for r in rows:
                        # Map existing columns, leave new ones blank if missing
                        out = [
                            r.get('user_id', '') if isinstance(r, dict) else '',
                            r.get('username', '') if isinstance(r, dict) else '',
                            r.get('group_name', '') if isinstance(r, dict) else '',
                            r.get('group_id', '') if isinstance(r, dict) else '',
                            r.get('member_label', '') if isinstance(r, dict) else '',
                            r.get('status', '') if isinstance(r, dict) else '',
                            r.get('full error message', '') if isinstance(r, dict) else '',
                        ]
                        writer.writerow(out)
                try:
                    shutil.move(str(tmp_path), str(_CSV_PATH))
                except Exception:
                    pass
        except Exception:
            pass
    
    
    def _append_added_csv_row(user_id, username, group_name, group_id, member_label, status, full_error_msg=''):
        _ensure_added_csv_header()
        try:
            # Normalize error/status text
            try:
                fem = ''
                if isinstance(full_error_msg, (list, tuple)):
                    fem = ' '.join(str(x) for x in full_error_msg)
                else:
                    fem = str(full_error_msg or '')
            except Exception:
                fem = str(full_error_msg or '')
            fem_l = fem.lower()
            status_norm = str(status or '').strip().lower()

            # Blacklist: do not log rate limit noise
            if 'too many requests' in fem_l:
                return

            # Whitelist: log when there is a verification flag, any 'Skipped' status, or "not a mutual contact"
            has_verified_flag = ('verified=true' in fem_l) or ('verified=false' in fem_l) or ('verified=unknown' in fem_l)
            is_not_mutual = ('not a mutual contact' in fem_l) or ('not mutual contact' in fem_l)
            is_skipped_status = status_norm.startswith('skipped')
            if not (has_verified_flag or is_not_mutual or is_skipped_status):
                return

            # Skip intermediate verify-false paths; allow final summary row where status == 'Verification failed'
            if ('verified=false' in fem_l) and (status_norm != 'verification failed'):
                return

            # Enforce de-duplication by (user_id, group_id)
            uid = str(user_id).strip() if user_id is not None else ''
            gid = str(group_id).strip() if group_id is not None else ''
            pair = (uid, gid)
            # Only deduplicate when both keys are present
            if uid and gid:
                # Lazy-load existing pairs on first use
                if not existing_pairs:
                    try:
                        with open(_CSV_PATH, 'r', newline='', encoding='utf-8') as rf:
                            reader = csv.DictReader(rf)
                            for row in reader:
                                ru = str(row.get('user_id', '')).strip()
                                rg = str(row.get('group_id', '')).strip()
                                if ru and rg:
                                    existing_pairs.add((ru, rg))
                    except Exception:
                        pass
                if pair in existing_pairs:
                    return  # skip duplicate
            with open(_CSV_PATH, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([user_id, username or '', group_name, group_id, member_label, status, full_error_msg or ''])
            if uid and gid:
                existing_pairs.add(pair)
        except Exception:
            pass

    # Build processed user ids for this target group from CSV (only for this target group)
    _ensure_added_csv_header()
    processed_ids = set()
    try:
        with open(_CSV_PATH, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if str(row.get('group_id', '')).strip() == str(target_group_id):
                    uid = row.get('user_id')
                    if uid is None or uid == '':
                        continue
                    try:
                        processed_ids.add(int(str(uid).strip()))
                    except Exception:
                        pass
    except Exception:
        pass

    # Track cross-account attempts per user_id
    attempt_counts = {}
    last_failure = {}

    # Initialize account states for all clients
    account_states = []
    total_accounts = len(clients)
    for idx, client in enumerate(clients):
        state = {'name': 'unknown', 'phone': None, 'user_id': None, 'added': 0, 'remaining': daily_limit, 'restricted': False, 'vf_consec': 0, 'initialized': False, 'is_member': False, 'is_src_member': False}
        account_states.append(state)

    # Build global candidate list from first available account
    global_candidates = []
    existing_ids = set()
    
    # Find first working account to build candidate list
    for idx, client in enumerate(clients):
        try:
            await _ensure_connected(client)
            me = await client.get_me()
            account_states[idx]['name'] = getattr(me, 'username', None) or getattr(me, 'first_name', None) or str(getattr(me, 'id', 'me'))
            account_states[idx]['user_id'] = getattr(me, 'id', None)
            account_states[idx]['phone'] = getattr(me, 'phone', None)
            account_states[idx]['initialized'] = True

            # Load or initialize daily stats from DB (per target)
            try:
                _db = Database()
                _today = datetime.now().strftime('%Y-%m-%d')
                _phone = account_states[idx]['phone']
                if _phone:
                    _stats = _db.get_target_daily_stats(_phone, target_group_id, _today)
                    if _stats is None:
                        # Initialize today's per-target counters with configured daily_limit
                        _db.increment_target_daily_counters(_phone, target_group_id, _today, added_inc=0, remaining=int(daily_limit))
                        account_states[idx]['added'] = 0
                        account_states[idx]['remaining'] = int(daily_limit)
                    else:
                        _added, _remaining = _stats
                        account_states[idx]['added'] = int(_added)
                        account_states[idx]['remaining'] = int(_remaining)
                        if account_states[idx]['remaining'] <= 0:
                            account_states[idx]['restricted'] = True
                try:
                    _db.close()
                except Exception:
                    pass
            except Exception:
                # Fallback silently if DB unavailable
                pass

            # Re-resolve target entity to avoid cross-client entity issues
            target_entity_local = target_entity
            try:
                if 'target_link_hint' in locals() and target_link_hint:
                    target_entity_local = await client.get_entity(target_link_hint)
            except Exception:
                target_entity_local = target_entity

            # Check membership and auto-join if needed
            try:
                checked = await _is_member(client, target_entity_local, getattr(me, 'id', None))
                is_member = bool(checked)
            except Exception:
                is_member = False
            
            if not is_member:
                try:
                    if isinstance(target_entity_local, TLChannel):
                        joined = False
                        try:
                            await client(JoinChannelRequest(target_entity_local))
                            joined = True
                        except errors.UserAlreadyParticipantError:
                            is_member = True
                        except Exception:
                            if 'target_link_hint' in locals() and isinstance(target_link_hint, str) and (('t.me/' in target_link_hint) or target_link_hint.startswith('@')):
                                try:
                                    link_entity = await client.get_entity(target_link_hint)
                                    await client(JoinChannelRequest(link_entity))
                                    joined = True
                                except errors.UserAlreadyParticipantError:
                                    is_member = True
                                except Exception:
                                    pass
                        if not is_member and joined:
                            try:
                                await asyncio.sleep(1)
                                checked = await _is_member(client, target_entity_local, getattr(me, 'id', None))
                                is_member = bool(checked)
                            except Exception:
                                pass
                except Exception:
                    pass
            
            account_states[idx]['is_member'] = is_member

            # Ensure source membership and auto-join if needed for this client
            is_src_member = False
            try:
                source_entity_local = None
                try:
                    if 'src_link_hint' in locals() and src_link_hint:
                        source_entity_local = await client.get_entity(src_link_hint)
                except Exception:
                    source_entity_local = None
                if source_entity_local is None:
                    try:
                        source_entity_local = await client.get_entity(int(src_group_id))
                    except Exception:
                        source_entity_local = int(src_group_id)
                try:
                    checked_src = await _is_member(client, source_entity_local, getattr(me, 'id', None))
                    is_src_member = bool(checked_src)
                except Exception:
                    is_src_member = False
                if not is_src_member and isinstance(source_entity_local, TLChannel):
                    joined_src = False
                    try:
                        await client(JoinChannelRequest(source_entity_local))
                        joined_src = True
                    except errors.UserAlreadyParticipantError:
                        is_src_member = True
                    except Exception:
                        try:
                            if 'src_link_hint' in locals() and isinstance(src_link_hint, str) and (("t.me/" in src_link_hint) or src_link_hint.startswith('@')):
                                link_entity = await client.get_entity(src_link_hint)
                                await client(JoinChannelRequest(link_entity))
                                joined_src = True
                        except errors.UserAlreadyParticipantError:
                            is_src_member = True
                        except Exception:
                            pass
                    if not is_src_member and joined_src:
                        try:
                            await asyncio.sleep(1)
                            checked_src = await _is_member(client, source_entity_local, getattr(me, 'id', None))
                            is_src_member = bool(checked_src)
                        except Exception:
                            pass
            except Exception:
                pass
            account_states[idx]['is_src_member'] = is_src_member
            
            if is_member and is_src_member:
                # Fetch source members and existing members
                try:
                    source_members_local = await _fetch_source_members(client, src_group_id)
                    uniq_map_local = {}
                    for m in source_members_local:
                        uid_key = m.get('user_id')
                        if uid_key is not None:
                            uniq_map_local[uid_key] = m
                    source_members_local = list(uniq_map_local.values())
                    
                    existing_ids = await _get_existing_member_ids(client, target_group_id)
                    
                    # Build global candidate list
                    global_candidates = [m for m in source_members_local if (m.get('user_id') not in existing_ids and m.get('user_id') not in processed_ids and attempt_counts.get(m.get('user_id'), 0) < 3)]
                    
                    source_count = len(source_members_local)
                    target_count = len(existing_ids)
                    filtered_count = len(global_candidates)
                    print(f"[INFO] Source members: {source_count} | Target members: {target_count} | Candidates after filtering: {filtered_count}\n")
                    
                    # Candidates kept in-memory for processing; CSV persistence disabled per requirements
                    pass
                    break
                except Exception as e:
                    print(f"[{account_states[idx]['name']}] Failed to fetch source members: {e}")
                    continue
        except Exception as e:
            print(f"Failed to initialize account {idx}: {e}")
            continue

    # Warm up all adder accounts by fetching participants from the source group entity
    try:
        for idx, client in enumerate(clients):
            try:
                await _ensure_connected(client)
                # Resolve source entity for this client
                source_entity_local = None
                try:
                    if 'src_link_hint' in locals() and src_link_hint:
                        source_entity_local = await client.get_entity(src_link_hint)
                except Exception:
                    source_entity_local = None
                if source_entity_local is None:
                    try:
                        source_entity_local = await client.get_entity(int(src_group_id))
                    except Exception:
                        source_entity_local = int(src_group_id)
                # Fetch participants to warm cache
                try:
                    await client.get_participants(source_entity_local)
                    acct_name = account_states[idx]['name'] if account_states[idx].get('initialized') else f"account {idx}"
                    print(f"[Warmup] {acct_name} fetched source participants")
                except Exception as e:
                    acct_name = account_states[idx]['name'] if account_states[idx].get('initialized') else f"account {idx}"
                    print(f"[Warmup] {acct_name} failed to fetch participants: {e}")
                await asyncio.sleep(random.uniform(0.2, 0.8))
            except Exception as e:
                print(f"[Warmup] Failed to warm account {idx}: {e}")
    except Exception:
        pass

    # Initialize remaining selected accounts (names/ids/phones and membership) for proper rotation and reporting
    for idx, client in enumerate(clients):
        state = account_states[idx]
        if not state['initialized']:
            try:
                await _ensure_connected(client)
                me = await client.get_me()
                state['name'] = getattr(me, 'username', None) or getattr(me, 'first_name', None) or str(getattr(me, 'id', 'me'))
                state['user_id'] = getattr(me, 'id', None)
                state['phone'] = getattr(me, 'phone', None)
                state['initialized'] = True
            except Exception:
                state['restricted'] = True
                continue
        # Check membership in target and attempt to join if not a member
        try:
            target_entity_local = target_entity
            try:
                if 'target_link_hint' in locals() and target_link_hint:
                    target_entity_local = await client.get_entity(target_link_hint)
            except Exception:
                target_entity_local = target_entity

            try:
                checked = await _is_member(client, target_entity_local, state.get('user_id'))
                is_member = bool(checked)
            except Exception:
                is_member = False

            if not is_member:
                if isinstance(target_entity_local, TLChannel):
                    joined = False
                    try:
                        await client(JoinChannelRequest(target_entity_local))
                        joined = True
                    except errors.UserAlreadyParticipantError:
                        is_member = True
                    except Exception:
                        if 'target_link_hint' in locals() and isinstance(target_link_hint, str) and (("t.me/" in target_link_hint) or target_link_hint.startswith('@')):
                            try:
                                link_entity = await client.get_entity(target_link_hint)
                                await client(JoinChannelRequest(link_entity))
                                joined = True
                            except errors.UserAlreadyParticipantError:
                                is_member = True
                            except Exception:
                                pass
                    if not is_member and joined:
                        try:
                            await asyncio.sleep(1)
                            checked = await _is_member(client, target_entity_local, state.get('user_id'))
                            is_member = bool(checked)
                        except Exception:
                            pass
            account_states[idx]['is_member'] = is_member

            # Also check membership in source and try auto-join if not a member
            try:
                is_src_member = False
                source_entity_local = None
                try:
                    if 'src_link_hint' in locals() and src_link_hint:
                        source_entity_local = await client.get_entity(src_link_hint)
                except Exception:
                    source_entity_local = None
                if source_entity_local is None:
                    try:
                        source_entity_local = await client.get_entity(int(src_group_id))
                    except Exception:
                        source_entity_local = int(src_group_id)
                try:
                    checked_src = await _is_member(client, source_entity_local, state.get('user_id'))
                    is_src_member = bool(checked_src)
                except Exception:
                    is_src_member = False
                if not is_src_member and isinstance(source_entity_local, TLChannel):
                    joined_src = False
                    try:
                        await client(JoinChannelRequest(source_entity_local))
                        joined_src = True
                    except errors.UserAlreadyParticipantError:
                        is_src_member = True
                    except Exception:
                        try:
                            if 'src_link_hint' in locals() and isinstance(src_link_hint, str) and (("t.me/" in src_link_hint) or src_link_hint.startswith('@')):
                                link_entity = await client.get_entity(src_link_hint)
                                await client(JoinChannelRequest(link_entity))
                                joined_src = True
                        except errors.UserAlreadyParticipantError:
                            is_src_member = True
                        except Exception:
                            pass
                    if not is_src_member and joined_src:
                        try:
                            await asyncio.sleep(1)
                            checked_src = await _is_member(client, source_entity_local, state.get('user_id'))
                            is_src_member = bool(checked_src)
                        except Exception:
                            pass
                account_states[idx]['is_src_member'] = is_src_member
            except Exception:
                pass
        except Exception:
            pass

    # Fallback: if selected accounts couldn't fetch candidates, use browsing account to fetch
    if not global_candidates:
        try:
            print("[INFO] Selected accounts could not build candidate list. Falling back to browsing account to fetch source members...")
            source_members_local = await _fetch_source_members(browse_client, src_group_id)
            uniq_map_local = {}
            for m in source_members_local:
                uid_key = m.get('user_id')
                if uid_key is not None:
                    uniq_map_local[uid_key] = m
            source_members_local = list(uniq_map_local.values())
            existing_ids = await _get_existing_member_ids(browse_client, target_group_id)
            filtered = [m for m in source_members_local if (m.get('user_id') not in existing_ids and m.get('user_id') not in processed_ids and attempt_counts.get(m.get('user_id'), 0) < 3)]
            source_count = len(source_members_local)
            target_count = len(existing_ids)
            filtered_count = len(filtered)
            print(f"[INFO] Source members: {source_count} | Target members: {target_count} | Candidates after filtering: {filtered_count}\n")
            global_candidates = filtered
            # Candidates kept in-memory for processing; CSV persistence disabled per requirements
            pass
        except Exception as e:
            print(f"[WARN] Fallback fetch via browsing account failed: {e}")

    if not global_candidates:
        print("No candidates found or no working accounts available.")
        return

    # Helper function to find next available account (prefer others; fallback to current if it's the only one)
    def _find_next_account(start_idx):
        # Prefer other accounts first to keep round-robin behavior
        for i in range(1, total_accounts):  # 1..total_accounts-1
            idx = (start_idx + i) % total_accounts
            state = account_states[idx]
            if state['remaining'] > 0 and not state['restricted'] and state['is_member'] and state.get('is_src_member'):
                return idx
        # If none of the other accounts qualify, allow current account if it still qualifies
        cur = account_states[start_idx]
        if cur['remaining'] > 0 and not cur['restricted'] and cur['is_member'] and cur.get('is_src_member'):
            return start_idx
        return None

    # Round-robin processing: one member per account rotation
    current_account_idx = 0
    candidate_idx = 0
    no_progress_rounds = 0
    MAX_NO_PROGRESS_ROUNDS = total_accounts * 3

    while candidate_idx < len(global_candidates) and no_progress_rounds < MAX_NO_PROGRESS_ROUNDS:
        # Find next available account starting from current_account_idx
        account_idx = _find_next_account(current_account_idx)
        if account_idx is None:
            print("No more available accounts.")
            break

        # Update current_account_idx immediately for proper rotation
        current_account_idx = account_idx

        client = clients[account_idx]
        state = account_states[account_idx]
        
        # Initialize account if not done yet
        if not state['initialized']:
            try:
                await _ensure_connected(client)
                me = await client.get_me()
                state['name'] = getattr(me, 'username', None) or getattr(me, 'first_name', None) or str(getattr(me, 'id', 'me'))
                state['user_id'] = getattr(me, 'id', None)
                state['phone'] = getattr(me, 'phone', None)
                state['initialized'] = True
            except Exception:
                state['restricted'] = True
                current_account_idx = (account_idx + 1) % total_accounts
                continue

        # Visual header showing current and next account
        # Compute the next account relative to the current_account_idx (excluding current for display)
        nxt_idx_display = None
        for i in range(1, total_accounts):
            idx2 = (current_account_idx + i) % total_accounts
            st2 = account_states[idx2]
            if st2['remaining'] > 0 and not st2['restricted'] and st2['is_member'] and st2.get('is_src_member'):
                nxt_idx_display = idx2
                break
        next_name = '-' if nxt_idx_display is None else account_states[nxt_idx_display]['name']
        print(f"\n=== Account [{account_idx + 1}/{total_accounts}] current: {state['name']} | next: {next_name} | remaining accounts: {total_accounts - account_idx - 1} | per-account remaining: {state['remaining']} ===")

        # Get current candidate
        user_rec = global_candidates[candidate_idx]
        uid = user_rec['user_id']
        label = _format_member_label(user_rec)

        # Re-resolve target entity for this client
        target_entity_local = target_entity
        try:
            if 'target_link_hint' in locals() and target_link_hint:
                target_entity_local = await client.get_entity(target_link_hint)
        except Exception:
            target_entity_local = target_entity
        try:
            status, msg = await _invite_one(client, target_entity_local, user_rec, source_group_id=src_group_id, verify=POST_INVITE_VERIFY_ENABLED, allow_contact_fallback=ADD_CONTACT_FALLBACK_ENABLED)
            
            if status == 'added':
                fem = str(msg or '')
                if POST_INVITE_VERIFY_ENABLED and (('verified=False' in fem) or ('verified=unknown' in fem.lower())):
                    # Treat verification failed/unknown as a failed attempt with retry up to 3 times
                    attempt_counts[uid] = attempt_counts.get(uid, 0) + 1
                    reason_label = 'Verification failed' if 'verified=False' in fem else 'Verification unknown'
                    last_failure[uid] = (reason_label, fem)
                    print(f"[RETRY] {label} {reason_label.lower()} (attempt {attempt_counts[uid]}/3); will retry with next account.")
                    try:
                        state['vf_consec'] = int(state.get('vf_consec', 0)) + 1
                    except Exception:
                        state['vf_consec'] = int(state.get('vf_consec', 0)) + 1
                    if attempt_counts[uid] >= 3:
                        processed_ids.add(uid)
                        _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, reason_label, fem)
                        print(f"[FAIL] After 3 attempts: {label} -> {reason_label}")
                        candidate_idx += 1  # Move to next candidate
                    await asyncio.sleep(random.randint(10, 15))
                else:
                    state['added'] += 1
                    state['remaining'] -= 1
                    processed_ids.add(uid)
                    existing_ids.add(uid)
                    # Reset retry tracking on success
                    attempt_counts.pop(uid, None)
                    last_failure.pop(uid, None)
                    _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, 'Added to group', msg or '')
                    print(f"[ OK ] Added {label} -> {target_group_name} (remaining {state['remaining']})")
                    # Persist per-target daily counters to DB and log added member
                    try:
                        _db = Database()
                        _today = datetime.now().strftime('%Y-%m-%d')
                        if state.get('phone'):
                            _db.increment_target_daily_counters(state['phone'], int(target_group_id), _today, added_inc=1, remaining=int(state['remaining']))
                            try:
                                _uid = int(uid)
                            except Exception:
                                _uid = None
                            try:
                                _db.log_daily_added_member(state['phone'], int(target_group_id), _uid, user_rec.get('username'))
                            except Exception:
                                pass
                        try:
                            _db.close()
                        except Exception:
                            pass
                    except Exception:
                        pass
                    # If daily limit exhausted, mark restricted and persist (per target + global DB restriction)
                    if state['remaining'] <= 0:
                        print(f"[INFO] Account {state['name']} reached daily limit; marking as RESTRICTED for this run.")
                        state['restricted'] = True
                        try:
                            _db = Database()
                            _today = datetime.now().strftime('%Y-%m-%d')
                            if state.get('phone'):
                                _db.upsert_target_daily_stats(state['phone'], int(target_group_id), _today, added=int(state['added']), remaining=int(state['remaining']))
                                # Also persist global restriction for ~24h so account is excluded across flows
                                try:
                                    _db.update_restriction(f"true:{datetime.now().strftime('%Y:%m:%d:%H')}", phone=state['phone'])
                                except Exception:
                                    pass
                            try:
                                _db.close()
                            except Exception:
                                pass
                        except Exception:
                            pass
                    # Reset consecutive verification failures on success
                    state['vf_consec'] = 0
                    candidate_idx += 1  # Move to next candidate
                    await asyncio.sleep(random.randint(10, 15))
            elif status.startswith('skipped'):
                # Do not retry on any 'Skipped' outcomes; finalize immediately
                reason_map = {
                    'skipped_privacy': 'Restricted (privacy)',
                    'skipped_not_mutual': 'Skipped (not mutual)',
                    'skipped_invalid_id': 'Skipped (invalid ID)',
                    'skipped_too_many_channels': 'Skipped (too many channels)',
                    'skipped_kicked': 'Skipped (kicked)',
                    'skipped_blocked': 'Skipped (blocked)',
                }
                reason = reason_map.get(status, 'Skipped')
                processed_ids.add(uid)
                _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, reason, msg or '')
                print(f"[FAIL] {label} -> {reason}")
                # Reset counters and move to next candidate
                state['vf_consec'] = 0
                attempt_counts.pop(uid, None)
                last_failure.pop(uid, None)
                candidate_idx += 1  # Move to next candidate
            else:
                # Treat returned 'error' or other unexpected statuses
                if msg and ('A wait of' in msg and 'seconds is required' in msg):
                    # Account restriction detected; mark and break to next account
                    attempt_counts[uid] = attempt_counts.get(uid, 0) + 1
                    last_failure[uid] = ('Restricted', msg)
                    print(f"[RST ] {label} -> Restricted: {msg} (attempt {attempt_counts[uid]}/3)")
                    if attempt_counts[uid] >= 3:
                        processed_ids.add(uid)
                        # Do NOT log FloodWait immediately unless it is the 3rd failure per requirements
                        _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, 'Restricted', (msg or ''))
                        print(f"[FAIL] After 3 attempts: {label} -> Restricted")
                        candidate_idx += 1  # Move to next candidate
                    if state['phone']:
                        try:
                            Database().update_restriction(f"true:{datetime.now().strftime('%Y:%m:%d:%H')}", phone=state['phone'])
                            state['restricted'] = True
                        except Exception:
                            pass
                # Generic error path
                else:
                    attempt_counts[uid] = attempt_counts.get(uid, 0) + 1
                    last_failure[uid] = ('Restricted', msg or '')
                    if attempt_counts[uid] >= 3:
                        processed_ids.add(uid)
                        _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, 'Restricted', (msg or ''))
                        print(f"[FAIL] After 3 attempts: {label} -> Restricted")
                        candidate_idx += 1  # Move to next candidate
                    else:
                        print(f"[RETRY] {label} -> Restricted: {msg} (attempt {attempt_counts[uid]}/3); will retry with next account.")
                    if state['phone']:
                        try:
                            Database().update_restriction(f"true:{datetime.now().strftime('%Y:%m:%d:%H')}", phone=state['phone'])
                            state['restricted'] = True
                        except Exception:
                            pass
                    # On restriction-like errors, skip the rest of this account immediately
                    try:
                        wait_seconds = None
                        if msg:
                            import re
                            m = re.search(r"(\d)\sseconds", str(msg))
                            wait_seconds = int(m.group(1)) if m else None
                    except Exception:
                        wait_seconds = None
                    if wait_seconds:
                        print(f"[INFO] Account {state['name']} marked RESTRICTED for ~{wait_seconds}s. Skipping remaining users for this account; restriction auto-clears in 24h.")
                    else:
                        print(f"[INFO] Account {state['name']} marked RESTRICTED. Skipping remaining users for this account; restriction auto-clears in 24h.")
        except (errors.PeerFloodError, errors.FloodWaitError, errors.UserBannedInChannelError) as e:
            # Count as a failed attempt for this user, but do not finalize until 3 attempts
            attempt_counts[uid] = attempt_counts.get(uid, 0) + 1
            last_failure[uid] = ('Restricted', str(e))
            print(f"[RST ] {label} -> Restricted: {e} (attempt {attempt_counts[uid]}/3)")
            if attempt_counts[uid] >= 3:
                processed_ids.add(uid)
                # Do NOT write to CSV before 3rd failure; at 3rd, log the failure
                _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, 'Restricted', str(e))
                print(f"[FAIL] After 3 attempts: {label} -> Restricted")
                candidate_idx += 1  # Move to next candidate
            if state['phone']:
                try:
                    Database().update_restriction(f"true:{datetime.now().strftime('%Y:%m:%d:%H')}", phone=state['phone'])
                    state['restricted'] = True
                except Exception:
                    pass
        except Exception as e:
            # Generic exception handling
            attempt_counts[uid] = attempt_counts.get(uid, 0) + 1
            last_failure[uid] = ('Error', str(e))
            print(f"[ERR ] {label} -> Error: {e} (attempt {attempt_counts[uid]}/3)")
            if attempt_counts[uid] >= 3:
                processed_ids.add(uid)
                _append_added_csv_row(uid, user_rec.get('username'), target_group_name, target_group_id, label, 'Error', str(e))
                print(f"[FAIL] After 3 attempts: {label} -> Error")
                candidate_idx += 1  # Move to next candidate
        
        # Move to next account for round-robin (ALWAYS rotate after each attempt)
        # current_account_idx is already updated at loop start
        
        # Check if we made progress
        if candidate_idx < len(global_candidates):
            no_progress_rounds = 0
        else:
            no_progress_rounds += 1

    # Summary only; CSV was appended per-attempt, do not overwrite
    print(f"\nAppended records to {_CSV_PATH}")
    print("\nFinal Summary (all accounts):")
    for st in account_states:
        status = 'RESTRICTED' if st['restricted'] else 'ACTIVE'
        print(f" - {st['name']} (phone: {st.get('phone','?')}, id: {st.get('user_id','?')}): added {st['added']}, remaining {st['remaining']} [{status}]")


async def _choose_promotable_target_dialog(client, prompt="Choose promotable target number: "):
    dialogs = {}
    idx = 1
    await _ensure_connected(client)
    async for dialog in client.iter_dialogs():
        if dialog.is_group:
            try:
                caps = await _get_self_admin_caps(client, dialog.entity)
                if caps.get('is_admin') and caps.get('can_promote'):
                    # Optionally require invite capability too (safer to assign invite rights)
                    if not caps.get('can_invite'):
                        continue
                    dialogs[idx] = (dialog.id, getattr(dialog.entity, 'access_hash', None), dialog.name, getattr(dialog.entity, 'username', None))
                    print(f"{idx}. {dialog.name} | can_promote={caps.get('can_promote')} can_invite={caps.get('can_invite')}")
                    idx += 1
            except Exception:
                continue
    if not dialogs:
        print("No promotable targets found (need admin with promote+invite rights).")
        return None
    while True:
        ch = input(f"{prompt}").strip()
        if ch.isdigit():
            n = int(ch)
            if n in dialogs:
                return dialogs[n]
        print("Invalid choice, try again.")


async def promote_admin_workflow():
    print("Promotion Workflow: Promote selected members to Admin (with Invite rights)")
    # 1) Pick an admin/owner account to operate
    print("Select an account to browse promotable groups/channels:")
    Database().view_all(admin=False)
    browse_raw = input('Enter a single account number to browse (digit): ').strip()
    while not browse_raw.isdigit():
        browse_raw = input('Enter a single account number to browse (digit): ').strip()
    browse_num = int(browse_raw)
    browse_clients = await TELEGRADD_client((browse_num,)).clients(restriction=False)
    if not browse_clients:
        print('Cannot authenticate the browsing account.')
        return
    browse_client = browse_clients[0]
    await _ensure_connected(browse_client)

    # 2) Choose a target where this account has promote rights
    target = await _choose_promotable_target_dialog(browse_client, prompt='Choose target number: ')
    if target is None:
        return
    target_entity = await browse_client.get_entity(int(target[0]))
    target_group_name = getattr(target_entity, 'title', None) or getattr(target_entity, 'username', None) or str(target_entity.id)

    # Double-check capability and show role
    caps = await _get_self_admin_caps(browse_client, target_entity)
    is_admin = bool(caps.get('is_admin'))
    can_promote = bool(caps.get('can_promote'))
    can_invite = bool(caps.get('can_invite'))
    print(f"[ROLE] Operating account role on target '{target_group_name}': {'admin/owner' if is_admin else 'member'} | can_promote={can_promote} | can_invite={can_invite}")
    if not (is_admin and can_promote and can_invite):
        print('[INFO] This account lacks required privileges (promote+invite). Aborting.')
        return

    # 3) Select multiple accounts (members) to promote
    print("Select accounts to promote (they must already be members of the target):")
    Database().view_all(admin=False)
    raw = input("Enter digits (all/ranges like 2-5/specific space-separated): ").lower().strip()
    tokens = [t for t in raw.split(' ') if t]
    selected = []
    if tokens and tokens[0] == 'all':
        auth_tuple = ('all',)
    else:
        for t in tokens:
            if '-' in t:
                a, b = (part.strip() for part in t.split('-', 1))
                if a.isdigit() and b.isdigit():
                    start, end = int(a), int(b)
                    if start <= end:
                        selected.extend(list(range(start, end + 1)))
            elif t.isdigit():
                selected.append(int(t))
        auth_tuple = tuple(sorted(set(selected))) if selected else tuple()
    if not auth_tuple:
        print('No accounts selected.')
        return

    # Fetch the clients for selected accounts to extract their user IDs
    promote_targets = []  # list of dicts: {num, client, user_id, username, phone}
    failed_accounts = []
    for n in auth_tuple:
        try:
            clients = await TELEGRADD_client((n,)).clients(restriction=False)
            if not clients:
                print(f"[SKIP] Account #{n}: cannot authenticate.")
                failed_accounts.append(n)
                continue
            c = clients[0]
            await _ensure_connected(c)
            me = await c.get_me()
            promote_targets.append({'num': n, 'client': c, 'user_id': int(getattr(me, 'id', 0) or 0), 'username': getattr(me, 'username', None), 'phone': getattr(me, 'phone', None), 'access_hash': getattr(me, 'access_hash', None)})
        except Exception as e:
            print(f"[SKIP] Account #{n}: error {e}")
            failed_accounts.append(n)

    # 4) Promote each selected account if already a member and not already admin
    print(f"Starting promotion on '{target_group_name}' ...")
    success, skipped, not_member, already_admin, errors_cnt = 0, 0, 0, 0, 0
    for rec in promote_targets:
        uid = rec['user_id']
        label = f"{rec['username'] or rec['phone'] or uid}"
        try:
            # Check membership from browsing/admin client perspective
            is_member = await _is_member(browse_client, target_entity, uid)
            if is_member is None:
                is_member = False
            if not is_member:
                # Extra diagnostics: show what the browser client sees for this user in this entity
                debug_member = None
                try:
                    if isinstance(target_entity, TLChannel):
                        print(f"[DBG ] Membership check via GetParticipantRequest for {label} in channel...")
                        try:
                            in_user = None
                            # Try to resolve to InputPeerUser with access_hash to avoid entity resolution issues
                            try:
                                ah = rec.get('access_hash')
                            except Exception:
                                ah = None
                            if ah:
                                in_user = InputPeerUser(uid, ah)
                            # Fallback: resolve via get_entity if needed
                            if in_user is None:
                                in_user = await browse_client.get_input_entity(uid)
                            try:
                                ch_hash2 = getattr(target_entity, 'access_hash', None)
                                if ch_hash2 is not None:
                                    in_channel2 = InputPeerChannel(target_entity.id, ch_hash2)
                                    await browse_client(GetParticipantRequest(in_channel2, in_user))
                                else:
                                    await browse_client(GetParticipantRequest(target_entity, in_user))
                            except Exception:
                                # fallback to original entity if InputPeerChannel path fails for some reason
                                await browse_client(GetParticipantRequest(target_entity, in_user))
                            print(f"[DBG ] GetParticipantRequest says: MEMBER")
                            debug_member = True
                        except errors.UserNotParticipantError:
                            print(f"[DBG ] GetParticipantRequest says: NOT MEMBER")
                            debug_member = False
                        except Exception as e2:
                            print(f"[DBG ] GetParticipantRequest error: {e2}")
                        # Secondary self-check using the candidate's own client to avoid cross-session peer issues
                        try:
                            cand_client = rec.get('client')
                            ch_hash = getattr(target_entity, 'access_hash', None)
                            if cand_client and ch_hash is not None:
                                in_channel = InputPeerChannel(target_entity.id, ch_hash)
                                await cand_client(GetParticipantRequest(in_channel, InputPeerSelf()))
                                print(f"[DBG ] Self-check (candidate client) says: MEMBER")
                                debug_member = True
                        except errors.UserNotParticipantError:
                            print(f"[DBG ] Self-check (candidate client) says: NOT MEMBER")
                        except Exception as e4:
                            print(f"[DBG ] Self-check error: {e4}")
                    elif isinstance(target_entity, TLChat):
                        print(f"[DBG ] Membership check via GetFullChatRequest for {label} in chat...")
                        try:
                            full = await browse_client(GetFullChatRequest(target_entity.id))
                            parts = getattr(full.full_chat, 'participants', None)
                            in_list = False
                            if parts and hasattr(parts, 'participants'):
                                for p in parts.participants:
                                    if getattr(p, 'user_id', None) == uid:
                                        in_list = True
                                        break
                            print(f"[DBG ] GetFullChatRequest list says: {'MEMBER' if in_list else 'NOT MEMBER'}")
                            debug_member = in_list
                        except Exception as e3:
                            print(f"[DBG ] GetFullChatRequest error: {e3}")
                except Exception:
                    pass
                if debug_member:
                    is_member = True
                if not is_member:
                    print(f"[SKIP] {label}: not a member of '{target_group_name}', skipping.")
                    not_member += 1
                    continue

            # Check if already admin
            if await _is_user_admin_in_target(browse_client, target_entity, uid):
                print(f"[ OK ] {label}: already an admin in '{target_group_name}'.")
                already_admin += 1
                continue

            # Promote to admin with invite rights
            promoted = await _promote_user_to_inviter_admin(browse_client, target_entity, uid)
            if promoted:
                print(f"[ADM ] Promoted {label} to admin with invite rights in '{target_group_name}'")
                success += 1
            else:
                print(f"[ERR ] Failed to promote {label} (insufficient rights or API error)")
                errors_cnt += 1
        except Exception as e:
            print(f"[ERR ] Promotion error for {label}: {e}")
            errors_cnt += 1
        await asyncio.sleep(random.randint(3, 6))

    print(f"Done. Success={success}, AlreadyAdmin={already_admin}, NotMember={not_member}, Errors={errors_cnt}, FailedAccountsAuth={len(failed_accounts)}")


if __name__ == '__main__':
    home_page()
