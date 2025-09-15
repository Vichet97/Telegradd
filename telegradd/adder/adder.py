import asyncio
import time

from telethon import TelegramClient

import asyncio
import random
import typing
from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, InviteToChannelRequest, GetParticipantRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
from telethon.tl.types import InputPeerChannel, InputPeerUser
from telethon import errors
import datetime

from telegradd.connect.authorisation.client import TELEGRADD_client
from telegradd.connect.authorisation.databased import Database
from telegradd.utils import get_from_csv

class ADDER:
    def __init__(self, client: TelegramClient):
        self.client = client

    async def join_group(self, group_link: str) -> bool|None:
        if group_link.startswith('https://t.me/'):
            self.group_link = group_link
        else:
            self.group_link = 'https://t.me/' + group_link
        async with self.client:
            try:
                link = self.group_link
                # If it's an invite link (t.me/+<hash> or t.me/joinchat/<hash>), import the invite first
                if '/+' in link or '/joinchat/' in link:
                    if '/+' in link:
                        invite_hash = link.rsplit('/+', 1)[1]
                    else:
                        invite_hash = link.rsplit('joinchat/', 1)[1]
                    try:
                        # ImportChatRequest
                        await self.client(ImportChatInviteRequest(invite_hash))
                        name = await self.client.get_entity('me')
                        print(f'{name.first_name} successfully joined via invite {self.group_link}')
                        return True
                    except errors.UserAlreadyParticipantError:
                        name = await self.client.get_entity('me')
                        print(f'{name.first_name} is already a member of {self.group_link}')
                        return True
                # Otherwise handle as public username/channel link
                group = await self.client.get_entity(self.group_link)
                # Pre-check membership to avoid unnecessary join attempts
                try:
                    me = await self.client.get_entity('me')
                    await self.client(GetParticipantRequest(group, me))
                    # If no exception, user is already a participant
                    print(f'{me.first_name} is already a member of {self.group_link}')
                    return True
                except errors.UserNotParticipantError:
                    # Not a member yet, proceed to join
                    pass
                except Exception:
                    # If membership check fails for some reason, proceed to join attempt
                    pass
                try:
                    await self.client(JoinChannelRequest(group))
                    name = await self.client.get_entity('me')
                    print(f'{name.first_name} successfully joined {self.group_link}')
                    return True
                except errors.UserAlreadyParticipantError:
                    name = await self.client.get_entity('me')
                    print(f'{name.first_name} is already a member of {self.group_link}')
                    return True
            except Exception as err:
                if isinstance(err, ValueError):
                    print("group with this username doesn't seem to exist")
                    return False
                elif isinstance(err, (errors.InviteHashInvalidError, errors.InviteHashExpiredError)):
                    print("Invite link is invalid or expired", err)
                    return False
                else:
                    print(f'Something went wrong {err}')
                    return None

    async def meet_all_groups(self, show_dict=False) -> typing.Dict:
        chat_dict = {}
        num = 1
        async with self.client:
            async for dialog in self.client.iter_dialogs():
                df = dialog.id
                if show_dict:
                    if dialog.is_group:
                        chat_dict[num] = (dialog.id, dialog.name)
                        num += 1
            return chat_dict

    async def meet_users(self, group_id):
        user_list = []
        n = 1
        async with self.client:
            print(f'Skim over users, pls wait...')
            async for user in self.client.iter_participants(group_id):
                user_list.append((user.id, user.access_hash, user.first_name))
                n += 1
                if str(n).endswith('00'):
                    print(f'skimmed through {n} users')

    async def add_via_id(self, filename: str, group_link: str):
        async with self.client:
            me = await self.client.get_entity('me')
            # Normalize group link similar to add_via_username
            grp_link = group_link if group_link.startswith('https://t.me/') else f'https://t.me/{group_link}'
            group = await self.client.get_entity(grp_link)
            chat = InputPeerChannel(group.id, group.access_hash)
            for user_info in get_from_csv(filename):
                try:
                    # user_info = (user_id, first_name, username, access_hash)
                    uid_str = user_info[0]
                    username = user_info[2] if len(user_info) > 2 else None
                    ah_str = user_info[3] if len(user_info) > 3 else None
                    uid = int(uid_str)
                    user: InputPeerUser
                    if ah_str and ah_str != 'None':
                        try:
                            uhash = int(ah_str)
                        except ValueError:
                            uhash = None
                        if uhash is not None:
                            user = InputPeerUser(user_id=uid, access_hash=uhash)
                        else:
                            # fallback to resolving entity
                            ent = await self.client.get_entity(username) if username and username != 'None' else await self.client.get_entity(uid)
                            user = InputPeerUser(user_id=ent.id, access_hash=ent.access_hash)
                    else:
                        # No access_hash in CSV: try resolving via username if present, else by id (may fail)
                        ent = await self.client.get_entity(username) if username and username != 'None' else await self.client.get_entity(uid)
                        user = InputPeerUser(user_id=ent.id, access_hash=ent.access_hash)
                except Exception as err:
                    print(f"Skip (other): {uid_str} -> {err}")
                    continue
                try:
                    me = await self.client.get_entity('me')
                    await self.client(InviteToChannelRequest(chat, [user]))
                    print(f'added {user_info[1]} by {me.first_name}')
                    await asyncio.sleep(random.randint(10, 15))
                except errors.PeerFloodError:
                    handle_db_errors(me.phone, me.username, 'Flood error')
                    break
                except errors.UserPrivacyRestrictedError:
                    print(f"can't add {user_info[1]} due to the user privacy setting")
                    continue
                except errors.UserNotMutualContactError:
                    print('User probably was in this group early, but leave it')
                    continue
                except errors.UserChannelsTooMuchError:
                    print(f"{user_info[1]} is already in too many channels/supergroups.")
                    continue
                except errors.UserKickedError:
                    print(f"{user_info[1]} was kicked from this supergroup/channel")
                except errors.UserBannedInChannelError:
                    handle_db_errors(me.phone, me.username,
                                            'was banned from sending messages in supergroups/channels')
                    break
                except errors.UserBlockedError:
                    handle_db_errors(me.phone, me.username, 'User blocked')
                    break
                except errors.FloodWaitError as e:
                    handle_db_errors(me.phone, me.username, f'Flood error: {e}')
                    break
                except Exception as err:
                    # Robust fallback: if the error text contains a Telegram wait notice, treat it as restriction
                    msg = str(err)
                    if 'A wait of' in msg and 'seconds is required' in msg:
                        handle_db_errors(me.phone, me.username, f'Flood error (text): {msg}')
                        break
                    print(f'Unhandled error pls send it to me - tg @malevolentkid {err}')
                    continue

    async def add_via_username(self, filename: str, group_link: str):
        if group_link.startswith('https://t.me/'):
            self.group_link = group_link
        else:
            self.group_link = 'https://t.me/' + group_link
        async with self.client:
            me = await self.client.get_entity ('me')
            group = await self.client.get_entity (self.group_link)
            chat = InputPeerChannel (group.id, group.access_hash)
            for user_info in get_from_csv (filename):
                if user_info[2] != 'None':
                    user = await self.client.get_entity(user_info[2])
                else:
                    print(f"User with id {user_info[0]} doesn't have username")
                    continue
                user = InputPeerUser (user_id=user.id, access_hash=user.access_hash)
                try:
                    me = await self.client.get_entity('me')
                    await self.client (InviteToChannelRequest (chat, [user]))
                    print (f'added {user_info[2]} by {me.first_name}')
                    await asyncio.sleep (random.randint (10, 15))
                except errors.PeerFloodError:
                    handle_db_errors(me.phone, me.username, 'Flood error')
                    break
                except errors.UserPrivacyRestrictedError:
                    print (f"Can't add {user_info[2]} due to the user privacy setting")
                    continue
                except errors.UserNotMutualContactError:
                    print (f'{user_info[2]}  is not a mutual contact')
                    continue
                except errors.UserChannelsTooMuchError:
                    print(f'{user_info[1]} is already in too many channels/supergroups.')
                    continue
                except errors.UserKickedError:
                    print(f'{user_info[2]} was kicked from this supergroup/channel')
                except errors.UserBannedInChannelError:
                    handle_db_errors(me.phone, me.username, 'was banned from sending messages in supergroups/channels')
                    break
                except errors.UserBlockedError:
                    handle_db_errors(me.phone, me.username, 'User blocked')
                    break
                except errors.FloodWaitError as err:
                    handle_db_errors(me.phone, me.username, f'Flood error: {err}')
                    break
                except Exception as err:
                    msg = str(err)
                    if 'A wait of' in msg and 'seconds is required' in msg:
                        handle_db_errors(me.phone, me.username, f'Flood error (text): {msg}')
                        break
                    print(f'Unhandled error pls send it to me - tg @malevolentkid {err}')
                    continue


def handle_db_errors(phone: str, username: str, error: str): # adding restriction to db with time
    print (f'{username} {error}')
    try:
        Database().update_restriction (f'true:{datetime.datetime.now().strftime("%Y:%m:%d:%H")}', phone=phone)
    except Exception as err:
        try:
            time.sleep(random.uniform(0.8, 4))
            Database().update_restriction (f'true:{datetime.datetime.now().strftime("%Y:%m:%d:%H")}', phone=phone)
        except Exception as err:
            print (f'Have some problem with database {err}')


async def auth_for_adding():
    Database ().automatically_delete_restrictions ()
    Database ().view_all (admin=False)
    raw = input ('Choose accounts. Enter digits via spaces (all - to use all, ranges like 2-5 are supported): ').lower ().strip (' ')
    tokens = [t for t in raw.split (' ') if t]
    if tokens and tokens[0] == 'all':
        skip_account = input ('Automatically skip an account with restriction (y/n)? ').lower ()
        if skip_account == 'y':
            clients = await TELEGRADD_client ().clients (restriction=True)
            return clients
        else:
            clients = await TELEGRADD_client ().clients (restriction=False)
            return clients
    else:
        # Parse numbers and ranges like '2-5'
        selected: list[int] = []
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
            clients = await TELEGRADD_client (tuple(selected)).clients (restriction=False)
            return clients
        else:
            print('U choose wrong options, try again')
            return None








