import asyncio
import os
import pathlib
import shutil
from datetime import datetime

import aiofiles
from telethon import TelegramClient

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

        async def _maybe_flag_restriction(me, text: str|None, acc_label: str):
            try:
                ok = (text is not None) and (text.strip() == EN_GOOD or text.strip() == ES_GOOD)
                if not ok:
                    phone = getattr(me, 'phone', None)
                    Database().update_restriction(f"true:{datetime.now().strftime('%Y:%m:%d:%H')}", phone=phone)
                    print(f"[{acc_label}] Marked as restricted based on @SpamBot reply: {text!r}")
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
                    # Decide and update restriction based on text
                    await _maybe_flag_restriction(me, text, acc_label)
            except Exception as e:
                print(f'[Error] Failed to check via @SpamBot: {e}')
            # Gentle delay to avoid rate-limits across many accounts
            await asyncio.sleep(1.2)
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


async def auth_for_test():# -> TelegramClient|bool:
    mode = input ('Use admin mode (y/n)?: ')
    admin = True if mode == 'y' else False
    Database ().view_all (admin=True) if admin else Database ().view_all ()
    raw = input('Choose accounts. Enter digits via spaces (all - to use all, ranges like 2-5 are supported): ').lower().strip(' ')
    tokens = [t for t in raw.split(' ') if t]
    if tokens and tokens[0] == 'all':
        await TELEGRADD_client ().clients (restriction=False)
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
            await TELEGRADD_client (tuple(selected)).clients (restriction=False)
        else:
            print('U choose wrong options, try again')
            return


def update_credentials(opt: int):
    Database ().view_all (admin=True)
    account = input('Choose an account: ')
    try:
        account = int(account)
    except:
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
        users = {line for line in f.readlines () if not line.startswith('user_id:first_name')}

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
    choice = input('Enter number of an account to clear restriction (all - clear all): ').strip().lower()
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
    elif choice.isdigit():
        idx = int(choice)
        rows = Database().get_all((idx,))
        if not rows:
            print('No account found with that number.')
            return
        try:
            Database().update_restriction('False', num=rows[0][0])
            print(f'Restriction cleared for account id={rows[0][0]}')
        except Exception as e:
            print(f'Failed to clear restriction for id={rows[0][0]}: {e}')
    else:
        print('Wrong input...')
        return


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