import asyncio
import os
import pathlib
import shutil
from datetime import datetime
import random

import aiofiles
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from telegradd.connect.authorisation.client import TELEGRADD_client
from telegradd.connect.authorisation.databased import Auth, Database


def add_account(option: int):
    load = 'CUSTOM'
    if option == 2:
        load = 'JS'
    elif option == 3:
        load = 'TDATA'
    elif option == 4:
        load = 'PYROGRAM'
    elif option == 5:
        load = 'TELETHON'

    try:
        Auth(load).add_account()
    except Exception as err:
        print(err)
        
async def check_accounts_via_spambot():
    try:
        # Show accounts list to help user choose by number
        try:
            Database().view_all(admin=False)
        except Exception:
            pass

        try:
            raw = input('Choose accounts. Enter digits via spaces (all - to use all, ranges like 2-5 are supported): ').lower().strip(' ')
        except (KeyboardInterrupt, EOFError):
            print('\nCancelled by user. Returning to menu...')
            return

        tokens = [t for t in raw.split(' ') if t]

        # Default to all accounts unless user specifies numbers/ranges
        auth_tuple = ('all',)
        if tokens and tokens[0] != 'all':
            selected = []
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
            if selected:
                auth_tuple = tuple(selected)
            else:
                print('U choose wrong options, try again')
                return

        clients = await TELEGRADD_client(auth_tuple).clients(restriction=False)
        if not clients:
            return
        EN_GOOD = "Good news, no limits are currently applied to your account. You’re free as a bird!"
        ES_GOOD = "Buenas noticias, no existen limitaciones aplicadas a tu cuenta actualmente. ¡Eres libre como un pájaro!"

        # Predefined short, respectful appeal messages. One will be chosen per account.
        APPEAL_MESSAGES = [
            "Hello, it seems my account was restricted by mistake. I follow the rules. Please review and lift the restriction.",
            "Hi Telegram team, I believe my account was flagged incorrectly. Kindly check and restore normal access.",
            "I respect Telegram guidelines and do not spam. Please recheck my status and remove the limitation.",
            "This appears to be an error. I use my account responsibly. Please verify and unblock.",
            "Kindly review my account. I think the restriction was applied in error. Thank you.",
            "Requesting a review: I did not intend any spam-like activity. Please lift the limits if possible.",
            "Hello, I’m facing a limitation that seems unintended. Please reassess and restore functionality.",
            "I always try to follow the rules. Please review my case and remove the restriction if appropriate.",
            "Apologies if anything looked suspicious. I will be careful. Please recheck and unblock my account.",
            "Please consider my appeal. I believe I was limited mistakenly and will comply fully with the rules.",
            "My account usage is for normal communication only. Kindly review and lift any accidental limits.",
            "I would appreciate a manual review of my restriction. I follow community guidelines.",
            "I understand safety measures. However, this restriction seems incorrect. Please verify and remove it.",
            "This is likely a false positive. Please re-evaluate my account and restore access.",
            "Hello team, requesting a fair review of my account’s restriction. Thank you for your help.",
            "I use Telegram responsibly. Please check my account again and lift the limitation.",
            "Please help verify my account. I think the warning was automatic and not applicable to my usage.",
            "I had no intent to spam. Kindly review my profile and remove the restriction.",
            "My account is used for personal communication. Please reconsider the current limitation.",
            "Thank you for keeping Telegram safe. I believe my account was limited by mistake. Please review.",
            "I will ensure compliant behavior. Please check my account and remove the block if possible.",
            "I suspect an automated flag. Please re-check and lift the restriction from my account.",
            "I value the platform’s rules. Requesting a re-evaluation of my account status.",
            "Hello, I think there was a misunderstanding. Please verify my activity and restore access.",
            "Kindly look into my case. I don’t spam and I follow guidelines. Please remove the limits.",
            "I appreciate your time. Please review my account and lift the unnecessary restriction.",
            "I will be mindful of limits to avoid false flags. Please restore normal access.",
            "This restriction interrupts normal use. Please double-check and unblock my account.",
            "I respectfully request a review of my restriction. Thank you for your assistance.",
            "Please examine my recent activity. I believe I was flagged accidentally.",
            "I’m committed to following all rules. Please remove any mistaken limitation on my account.",
            "I rely on Telegram for regular communication. Please review and restore my abilities.",
            "Hi, I believe the limitation is a false positive. Kindly reassess.",
            "I did not spam or break rules intentionally. Please lift the restriction after review.",
            "I’m open to feedback to ensure compliance. Please re-check my account status.",
            "My usage is normal and non-abusive. Please remove the restriction applied to my account.",
            "Requesting support: My account seems restricted in error. Please help restore access.",
            "I understand anti-spam policies. I think I was flagged incorrectly. Please review.",
            "A review would be appreciated. I strive to use Telegram responsibly.",
            "Hello, could you please verify my account? I believe the restriction is not warranted.",
            "I am careful with my messaging. Please remove limits set on my account.",
            "Thanks for your work. Please consider lifting the mistaken block on my account.",
            "I’m requesting an appeal for my restriction. I follow all rules. Please verify.",
            "If I triggered a limit accidentally, I apologize. Please re-check and unblock.",
            "Please confirm my account is safe. I believe this restriction is an error.",
            "I will adjust my usage to avoid triggering limits. Please restore normal access.",
            "I’m not engaging in spam. Please review and remove the limitation.",
            "I value Telegram’s community standards. Please reconsider my account’s restriction.",
            "Kindly review my account status and lift the block if feasible.",
            "Thank you for reviewing my appeal. I believe my account is compliant and safe."
        ]

        async def _send_spambot_appeal(client: TelegramClient, acc_label: str):
            """Navigate @SpamBot's UI safely and send a short appeal only when text input is expected.
            Handles FloodWait and retries with conservative pacing.
            """
            # Small helper: sleep with jitter
            async def _nap(base: float = 1.0, spread: float = 0.6):
                await asyncio.sleep(base + random.random() * spread)
            
            # Helper: extract button texts from a Message if present
            def _get_button_texts(msg) -> list[str]:
                texts = []
                try:
                    # Telethon "Message" exposes .buttons as list[list[Button]] or None
                    rows = getattr(msg, 'buttons', None) or []
                    for row in rows:
                        for btn in row:
                            t = getattr(btn, 'text', None)
                            if t:
                                texts.append(t)
                except Exception:
                    pass
                return texts
            
            # Helper: whether current text indicates bot expects free-form text
            def _expects_text_input(text: str) -> bool:
                if not text:
                    return False
                low = text.lower()
                keywords = [
                    'explain', 'explanation', 'appeal', 'describe', 'details', 'message', 'write', 'brief',
                    'send your appeal', 'send me a message', 'reason', 'why you think'
                ]
                return any(k in low for k in keywords)
            
            # Helper: try clicking a button by candidate list (case-insensitive)
            async def _try_click(msg, candidates: list[str]) -> tuple[bool, any]:
                btn_texts = _get_button_texts(msg)
                if not btn_texts:
                    return False, msg
                low_map = {t.lower(): t for t in btn_texts}
                for cand in candidates:
                    key = cand.lower()
                    for bt_low, orig in low_map.items():
                        if key in bt_low:
                            try:
                                await _nap(0.6, 0.5)
                                # Use Message.click(text=..) for robustness
                                new_msg = await msg.click(text=orig)
                                print(f'[{acc_label}] @SpamBot (appeal) clicked -> {orig}')
                                return True, new_msg or msg
                            except FloodWaitError as fw:
                                wait = min(int(getattr(fw, 'seconds', 5)), 60)
                                print(f'[{acc_label}] FloodWait during appeal click. Sleeping {wait}s...')
                                await asyncio.sleep(wait)
                                return False, msg
                            except Exception as e:
                                print(f'[{acc_label}] Appeal click failed for "{orig}": {e}')
                                return False, msg
                    return False, msg
            
            # The main flow
            try:
                bot = await client.get_entity('SpamBot')
            except Exception:
                bot = 'SpamBot'
            
            try:
                async with client.conversation(bot, timeout=45) as conv:
                    # Start conversation
                    await conv.send_message('/start')
                    reply = await conv.get_response()
                    print(f'[{acc_label}] @SpamBot (appeal) start -> {getattr(reply, "message", None) or getattr(reply, "text", "")}')
                    await _nap(0.8, 0.8)
            
                    # Progressive button navigation before sending text
                    # Candidate waves: we attempt these in order until we either expect text or run out of clicks
                    candidate_waves = [
                        ['start', 'continue', "let's start", "let's do it", 'ok'],
                        ['this is a mistake', 'mistake', 'i disagree', 'appeal', 'my account was limited by mistake'],
                        ['yes', 'continue', 'next']
                    ]
            
                    max_clicks = 6
                    clicks = 0
                    last_text = (getattr(reply, 'message', None) or getattr(reply, 'text', '') or '').strip()
            
                    while clicks < max_clicks and not _expects_text_input(last_text):
                        clicked_any = False
                        for wave in candidate_waves:
                            ok, _ = await _try_click(reply, wave)
                            if ok:
                                clicked_any = True
                                clicks += 1
                                # After clicking, wait for next response from bot to update state
                                try:
                                    reply = await conv.get_response()
                                except TimeoutError:
                                    break
                                last_text = (getattr(reply, 'message', None) or getattr(reply, 'text', '') or '').strip()
                                print(f'[{acc_label}] @SpamBot (appeal) state -> {last_text}')
                                await _nap(0.8, 0.8)
                                # If now it looks like text is expected, break
                                if _expects_text_input(last_text):
                                    break
                        if not clicked_any:
                            # No known buttons matched; stop clicking
                            break
            
                    # Decide whether to send text now
                    last_text = (getattr(reply, 'message', None) or getattr(reply, 'text', '') or '').strip()
                    if _expects_text_input(last_text) or not _get_button_texts(reply):
                        # Send appeal text
                        message = random.choice(APPEAL_MESSAGES)
                        try:
                            await _nap(0.8, 0.8)
                            resp3 = await conv.send_message(message)
                            print(f'[{acc_label}] @SpamBot (appeal) msg -> {getattr(resp3, "message", None) or getattr(resp3, "text", "")}')
                        except FloodWaitError as fw:
                            wait = min(int(getattr(fw, 'seconds', 10)), 120)
                            print(f'[{acc_label}] FloodWait during appeal. Sleeping {wait}s...')
                            await asyncio.sleep(wait)
                        except Exception as e:
                            print(f'[{acc_label}] Appeal failed: {e}')
                    else:
                        # Still requires button interaction; do not send text to avoid "Please use buttons..."
                        print(f'[{acc_label}] @SpamBot (appeal) still shows buttons; skipped sending free text to comply with bot flow.')
                        # Optionally try one more generic click wave
                        ok, _ = await _try_click(reply, ['appeal', 'this is a mistake', 'continue', 'yes'])
                        if ok:
                            try:
                                reply = await conv.get_response()
                                print(f'[{acc_label}] @SpamBot (appeal) state -> {getattr(reply, "message", None) or getattr(reply, "text", "")}')
                            except TimeoutError:
                                pass

            except Exception as e:
                print(f'[{acc_label}] Failed during conversation with @SpamBot: {e}')

        async def _maybe_flag_restriction(me, text: str|None, acc_label: str, client: TelegramClient):
            try:
                ok = (text is not None) and (text.strip() == EN_GOOD or text.strip() == ES_GOOD)
                if not ok:
                    phone = getattr(me, 'phone', None)
                    Database().update_restriction(f"true:{datetime.now().strftime('%Y:%m:%d:%H')}", phone=phone)
                    print(f"[{acc_label}] Marked as restricted based on @SpamBot reply: {text!r}")
                    # Send an appeal safely
                    await _send_spambot_appeal(client, acc_label)
                else:
                    print(f"[{acc_label}] Account appears unrestricted per @SpamBot")
            except Exception as e:
                print(f"[{acc_label}] Failed to update restriction state: {e}")

        for idx, client in enumerate(clients, start=1):
            try:
                async with client:
                    me = await client.get_me()
                    acc_label = (getattr(me, 'username', None) or getattr(me, 'phone', None) or str(getattr(me, 'id', 'unknown')))
                    text = None
                    try:
                        bot = await client.get_entity('SpamBot')
                    except Exception:
                        bot = 'SpamBot'
                    try:
                        async with client.conversation(bot, timeout=25) as conv:
                            await conv.send_message('/start')
                            resp = await conv.get_response()
                            text = getattr(resp, 'message', None) or getattr(resp, 'text', '') or ''
                            print(f'[{acc_label}] @SpamBot -> {text}')
                    except asyncio.TimeoutError:
                        # Fallback: try to fetch the last incoming message from @SpamBot
                        try:
                            msgs = await client.get_messages(bot, limit=3)
                            reply = next((m for m in msgs if not getattr(m, 'out', False)), None)
                            if reply:
                                text = getattr(reply, 'message', None) or getattr(reply, 'text', '') or ''
                                print(f'[{acc_label}] @SpamBot (fallback) -> {text}')
                            else:
                                print(f'[{acc_label}] No response from @SpamBot (timeout)')
                        except Exception as ie:
                            print(f'[{acc_label}] Error fetching fallback messages: {ie}')
                    except Exception as e:
                        print(f'[{acc_label}] Failed during conversation with @SpamBot: {e}')
                    # Decide, update restriction, and possibly appeal
                    await _maybe_flag_restriction(me, text, acc_label, client)
            except Exception as e:
                print(f'[Error] Failed to check via @SpamBot: {e}')
            # Gentle delay with jitter to avoid rate-limits across many accounts
            await asyncio.sleep(2.0 + 1.5 * (idx % 3) / 2)
    except KeyboardInterrupt:
        print('\nCancelled by user. Returning to menu...')
        return

def view_account():
    mode = input('Use admin mode (y/n)?: ')
    admin = True if mode == 'y' else False
    Database().view_all(admin=True) if admin else Database().view_all()


def delete_banned():
    path = pathlib.Path(pathlib.Path(__file__).parents[1], 'sessions', 'session_store')
    # Safely get accounts list (handle empty/False)
    rows = Database().get_all(('all', ))
    accounts = [account[1] for account in rows] if rows else []

    # If session_store directory doesn't exist, skip gracefully
    if not path.exists() or not path.is_dir():
        print('No session_store directory found; skipping banned sessions cleanup.')
        return

    # Collect existing sessions and move unknown ones to banned
    sessions = [str(file).rstrip('.session') for file in os.listdir(path) if str(file).endswith('.session')]
    banned_dir = pathlib.Path(pathlib.Path(__file__).parents[1], 'sessions', 'banned')
    banned_dir.mkdir(parents=True, exist_ok=True)
    for file in sessions:
        if file not in accounts:
            shutil.move(pathlib.Path(path, f'{file}.session'), banned_dir / f'{file}.session')


async def auth_for_test():  # -> TelegramClient|bool:
    mode = input('Use admin mode (y/n)?: ')
    admin = True if mode == 'y' else False
    Database().view_all(admin=True) if admin else Database().view_all()
    raw = input('Choose accounts. Enter digits via spaces (all - to use all, ranges like 2-5 are supported): ').lower().strip(' ')
    tokens = [t for t in raw.split(' ') if t]
    if tokens and tokens[0] == 'all':
        await TELEGRADD_client().clients(restriction=False)
    else:
        selected = []
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
        if selected:
            await TELEGRADD_client(tuple(selected)).clients(restriction=False)
        else:
            print('U choose wrong options, try again')
            return


def update_credentials(opt: int):
    Database().view_all(admin=True)
    account = input('Choose an account: ')
    try:
        account = int(account)
    except Exception:
        opt = 8
    if opt == 1:
        api_id = input('Enter new app_id: ')
        Database().update_id(api_id, account)
    elif opt == 2:
        app_hash = input('Enter new app_hash: ')
        Database().update_hash(app_hash, account)
    elif opt == 3:
        proxy = input('Enter new Proxy: ')
        Database().update_proxy(proxy, account)
    elif opt == 4:
        system = input('Enter new system. Format: device model:system:app version: ')
        Database().update_system(system, account)
    elif opt == 5:
        password = input('Enter new password: ')
        Database().update_password(password, account)
    elif opt == 6:
        restr = input('Enter new restriction')
        Database().update_restriction(restr, account)
    elif opt == 7:
        phone = input('Enter new phone: ')
        Database().update_phone(phone, account)
    elif opt == 8:
        print('Wrong option...')
        return


def delete_duplicates_csv():
    path_user = pathlib.Path(pathlib.Path(__file__).parents[2], 'users', 'users.csv')
    with open(path_user, encoding='UTF-8') as f:
        users = {line for line in f.readlines() if not line.startswith('user_id:first_name')}

    with open(path_user, 'w', encoding='UTF-8') as f:
        f.write('user_id:first_name:username:access_hash:phone:group\n')
        for user in users:
            f.write(user)


def delete_accounts():
    Database().view_all(admin=False)
    delete = input('Enter number of an account (all - delete all): ').strip().lower()
    if delete == 'all':
        rows = Database().get_all(('all',))
        if not rows:
            print('No accounts found to delete.')
            return
        for row in rows:
            try:
                Database().delete_account(num=row[0])
            except Exception as e:
                print(f'Failed to delete account id={row[0]}: {e}')
    else:
        if delete.isdigit():
            idx = int(delete)
            rows = Database().get_all((idx,))
            if not rows:
                print('No account found with that number.')
                return
            try:
                Database().delete_account(num=rows[0][0])
            except Exception as e:
                print(f'Failed to delete account id={rows[0][0]}: {e}')
        else:
            print('Wrong input...')
            return


def remove_from_restriction():
    """Clear the Restrictions flag for one or all accounts."""
    Database().view_all(admin=False)
    choice = input('Enter number of an account to clear restriction (all - clear all, ranges like 2-5, or specific space-separated like 2 5 14): ').strip().lower()

    selected_accounts = []

    if choice == 'all':
        rows = Database().get_all(('all',))
        if not rows:
            print('No accounts found.')
            return
        for row in rows:
            try:
                Database().update_restriction('False', num=row[0])
            except Exception as e:
                print(f'Failed to clear restriction for id={row[0]}: {e}')
        print('Restrictions cleared for all accounts.')
        return

    # Parse input for ranges and individual selections
    tokens = [t for t in choice.split(' ') if t]
    for token in tokens:
        if '-' in token:
            # Handle range like "2-5"
            try:
                start, end = token.split('-', 1)
                start_num = int(start.strip())
                end_num = int(end.strip())
                if start_num <= end_num:
                    selected_accounts.extend(range(start_num, end_num + 1))
                else:
                    selected_accounts.extend(range(end_num, start_num + 1))
            except ValueError:
                print(f'Invalid range format: {token}')
                return
        elif token.isdigit():
            # Handle individual number
            selected_accounts.append(int(token))
        else:
            print(f'Invalid input: {token}')
            return

    if not selected_accounts:
        print('No valid account numbers provided.')
        return

    # Remove duplicates and sort
    selected_accounts = sorted(set(selected_accounts))

    # Clear restrictions for selected accounts
    cleared_count = 0
    for account_num in selected_accounts:
        rows = Database().get_all((account_num,))
        if not rows:
            print(f'No account found with number {account_num}.')
            continue
        try:
            Database().update_restriction('False', num=rows[0][0])
            print(f'Restriction cleared for account #{account_num} (id={rows[0][0]})')
            cleared_count += 1
        except Exception as e:
            print(f'Failed to clear restriction for account #{account_num} (id={rows[0][0]}): {e}')

    print(f'Restrictions cleared for {cleared_count} account(s).')


def add_to_restriction():
    """Manually set the Restrictions flag for one or all accounts to a current timestamp."""
    Database().view_all(admin=False)
    choice = input('Enter number of an account to add restriction (all - add all): ').strip().lower()
    stamp = f"true:{datetime.now().strftime('%Y:%m:%d:%H')}"
    if choice == 'all':
        rows = Database().get_all(('all',))
        if not rows:
            print('No accounts found.')
            return
        for row in rows:
            try:
                Database().update_restriction(stamp, num=row[0])
            except Exception as e:
                print(f'Failed to add restriction for id={row[0]}: {e}')
        print('Restrictions added for all accounts.')
    elif choice.isdigit():
        idx = int(choice)
        rows = Database().get_all((idx,))
        if not rows:
            print('No account found with that number.')
            return
        try:
            Database().update_restriction(stamp, num=rows[0][0])
            print(f'Restriction added for account id={rows[0][0]}')
        except Exception as e:
            print(f'Failed to add restriction for id={rows[0][0]}: {e}')
    else:
        print('Wrong input...')
        return


if __name__ == '__main__':
    delete_accounts()