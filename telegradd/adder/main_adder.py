import logging
import asyncio
import os
import typing
import random

from telethon import events

from telegradd.connect.authorisation.client import TELEGRADD_client
from telegradd.connect.authorisation.databased import Database
from telegradd.adder.adder import ADDER, auth_for_adding
from telegradd.utils import split_ac


# Normalize a user-provided entity string into a form acceptable by ADDER.join_group
# - Strips whitespace and quotes/backticks
# - Converts @username to username
# - Converts t.me/, http://t.me/ to https://t.me/
# - Leaves bare usernames as-is (ADDER.join_group will prefix https://t.me/)
def _normalize_entity(entity: str) -> str | None:
    if not entity:
        return None
    e = entity.strip().strip('`"\'')
    if not e:
        return None
    if e.startswith('@'):
        return e[1:]
    if e.startswith('http://t.me/'):
        return 'https://t.me/' + e[len('http://t.me/'):]
    if e.startswith('https://t.me/'):
        return e
    if e.startswith('t.me/'):
        return 'https://t.me/' + e[len('t.me/'):]
    return e


async def join_groups(clients: typing.List, group_links, *, safe_delay: bool = True, per_account_delay_range: tuple[float, float] = (5.0, 12.0), max_none_retries: int = 2) -> typing.Optional[str]:
    # Accept either a single string or an iterable of strings
    if isinstance(group_links, str):
        group_links = [_normalize_entity(group_links)]
    else:
        group_links = [_normalize_entity(gl) for gl in group_links]
    # Drop any Nones/empties after normalization
    group_links = [gl for gl in group_links if gl]

    last_link = None
    for link in group_links:
        last_link = link
        results: list[typing.Optional[bool]] = []
        if safe_delay:
            # Sequential, with jitter between accounts and bounded retries on None (transient)
            for idx, cl in enumerate(clients, start=1):
                res = await ADDER(cl).join_group(link)
                # Retry a few times only for None results (unknown/transient), with exponential backoff
                if res is None and max_none_retries > 0:
                    backoff = random.uniform(6.0, 10.0)
                    for attempt in range(1, max_none_retries + 1):
                        await asyncio.sleep(backoff)
                        res = await ADDER(cl).join_group(link)
                        if res is not None:
                            break
                        backoff *= 1.7 + random.uniform(0.0, 0.6)
                results.append(res)
                # Jitter delay between accounts to avoid bursts
                await asyncio.sleep(random.uniform(*per_account_delay_range))
        else:
            # Legacy behavior: fan-out in parallel
            join_group_tasks = [ADDER(cl).join_group(link) for cl in clients]
            results = await asyncio.gather(*join_group_tasks)
        # Keep legacy retry prompt only when a single link was provided and all attempts failed with False
        if len(group_links) == 1:
            while False in results:
                if None in results:
                    break
                new_input = input('Enter group link without @, like "group_link" or "https://t.me/group_link": ')
                link = _normalize_entity(new_input)
                if not link:
                    break
                if safe_delay:
                    results = []
                    for cl in clients:
                        r = await ADDER(cl).join_group(link)
                        results.append(r)
                        await asyncio.sleep(random.uniform(*per_account_delay_range))
                else:
                    join_group_tasks = [ADDER(cl).join_group(link) for cl in clients]
                    results = await asyncio.gather(*join_group_tasks)
            return link
    # Return the last processed link for compatibility
    return last_link


def already_skimmed():
    skimmed = input ('Users already skimmed (y/n): ').lower ()
    while skimmed not in ['y', 'n']:
        skimmed = input ('Users already skimmed (y/n): ').lower ()
    skim = True if skimmed == 'y' else False
    return skim


async def main_adder(how_to_add='username'):
    if how_to_add == 'id':
        print ("WARNING: U can't add via ID a user or interact with a chat through id, that your current session hasn’t met yet."
               "That's why more errors may occur and additional actions would be required!!")
    clients = await auth_for_adding()
    # join group to add users
    if clients:
        group_link = input('Enter group link: ')
        await join_groups(clients, group_link)  # join group
    else:
        return

    # choose how to add users to group and add users to group
    client_num = len(clients)
    user_num = hows_to_add()
    # split csv
    try:
        split_ac(client_num, int(user_num))
    except TypeError:
        print('it seems there are not enough users in users.csv file. Try add more users in it or reduce the number '
              'of accounts or users via ac')
        return

    # adding to group
    add_user_objects = [ADDER(cl) for cl in clients]
    if how_to_add == 'id':
        how_to_act = get_by_id()
        if how_to_act == 'y':
            if not already_skimmed():
                show_groups = [obj.meet_all_groups () for obj in add_user_objects[1:]]
                show_groups.append (add_user_objects[0].meet_all_groups (show_dict=True))
                res = await asyncio.gather (*show_groups)
                group_id = choose_dialog(res[-1])
                await asyncio.gather(*[cl.meet_users(group_id) for cl in add_user_objects])

            num = 0
            client_list = []

            for client in clients:
                client_list.append (ADDER (client).add_via_id(f'users{num}.csv', group_link))
                num += 1

            # run loop per 5 accounts
            if len (client_list) > 5:
                for client in get_batch_acc (batch_size=5, clients=client_list):
                    await asyncio.gather (*client, return_exceptions=True)
            else:
                await asyncio.gather (*client, return_exceptions=True)

        else:
            group_lin = await join_groups(clients, how_to_act)  # join group from which users were parsed, link returned
            await asyncio.gather (*[cl.meet_users (group_lin) for cl in add_user_objects])
            num = 0
            client_list = []
            for client in clients:
                client_list.append (ADDER (client).add_via_id (f'users{num}.csv', group_link))
                num += 1

            if len (client_list) > 5:
                for client in get_batch_acc (batch_size=5, clients=client_list):
                    await asyncio.gather (*client, return_exceptions=True)
            else:
                await asyncio.gather (*client, return_exceptions=True)

    elif how_to_add == 'username':
        num = 0
        client_list = []
        for client in clients:
            client_list.append(ADDER(client).add_via_username(f'users{num}.csv', group_link))
            num += 1
        #  with loop will add thread
        if len(client_list) > 5:
            for client in get_batch_acc(batch_size=5, clients=client_list):
                await asyncio.gather (*client, return_exceptions=True)
                #tasks = [asyncio.to_thread(*client)]
                #res = await asyncio.gather(*client)
                #res = await asyncio.gather (*client, return_exceptions=True)
        else:
            await asyncio.gather (*client_list, return_exceptions=True)

def get_batch_acc(batch_size: int, clients):
    batch = 0
    for _ in range (len (clients) // batch_size + 1):
        if not clients[batch:batch + batch_size]:
            break
        yield clients[batch:batch + batch_size]
        batch += batch_size


def choose_dialog(dialog_dict: typing.Dict) -> int:
    for k, v in dialog_dict.items ():
        print (k, v[1])
    ch_num = input('Choose num of the group from which users were parsed: ')
    while not ch_num.isdigit():
        ch_num = input ('Choose number of the group from which users were parsed (digit): ')
    return dialog_dict[int(ch_num)][0]


def get_by_id() -> str:
    is_joined = input("Are all the accounts from which the adding will take place joined the group from where "
                      "the users were parsed (y/n)?\n - ").lower()
    while not ((is_joined == 'y') or (is_joined == 'n')):
        is_joined = input ("Are all the accounts from which the adding will take place joined the group from where "
                           "the users were parsed (y/n)?\n - ").lower()

    if is_joined == 'n':
        group_link = input ('Enter group link from which users were parsed without @, like "group_link" or '
                            '"https://t.me/group_link": ')
        return group_link
    else:
        return is_joined


def hows_to_add():
    users_num = input("How many users do u want to add via one account? Recommended: 60 or less\n - ")
    while not users_num.isdigit():
        users_num = input ("How many users do u want to add via one account? Recommended: 60 or less. Pls type the digit\n - ")

    return users_num


async def join_group():# -> TelegramClient|bool:
    mode = input ('Use admin mode (y/n)?: ')
    admin = True if mode == 'y' else False
    Database ().view_all (admin=True) if admin else Database ().view_all ()
    raw = input('Choose accounts. Enter digits via spaces (all - to use all, ranges like 2-5 are supported): ').lower().strip(' ')
    tokens = [t for t in raw.split(' ') if t]
    group_links_raw = input('Enter one or multiple group links/usernames to join (comma-separated): ')
    # Split by comma and keep non-empty trimmed tokens
    group_links_list = [t.strip() for t in group_links_raw.split(',') if t.strip()]
    if tokens and tokens[0] == 'all':
        clients = await TELEGRADD_client ().clients (restriction=False)
        await join_groups(clients, group_links_list)
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
            clients = await TELEGRADD_client (tuple(selected)).clients (restriction=False)
            await join_groups (clients, group_links_list)
        else:
            print('U choose wrong options, try again')
            return

if __name__ == '__main__':
    asyncio.run(main_adder())





