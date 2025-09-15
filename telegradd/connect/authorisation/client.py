import asyncio
import pathlib
import random
import os
from dataclasses import dataclass
import traceback
from typing import Optional, List

import telethon
from telethon import TelegramClient, connection
import TelethonFakeTLS
from pathlib import Path
import shutil

from telegradd.connect.authorisation.databased import Database

# Global choice for starting sessions without proxy: None | 'all_yes' | 'all_no'
_WITHOUT_PROXY_GLOBAL_CHOICE = None

# Registry of open TelegramClient instances by absolute session path
_OPEN_CLIENTS: dict[str, TelegramClient] = {}
# Reference counts for each open client (session path)
_OPEN_CLIENTS_RC: dict[str, int] = {}


def _cleanup_sqlite_sidecars(db_path: str):
    """Remove stale SQLite sidecar files (-journal, -wal, -shm) for a given DB path."""
    for suffix in ("-journal", "-wal", "-shm"):
        p = f"{db_path}{suffix}"
        try:
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            # Non-fatal if we can't remove a sidecar
            pass


def _session_lock_path(db_path: str) -> str:
    return f"{db_path}.lock"


async def _acquire_session_lock(db_path: str, wait_seconds: float = 5.0) -> bool:
    """Try to acquire an exclusive session lock by creating <db_path>.lock atomically.
    Wait up to wait_seconds; return False if still locked."""
    lock = _session_lock_path(db_path)
    attempts = max(1, int(wait_seconds / 0.25))
    for _ in range(attempts):
        try:
            fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                with os.fdopen(fd, 'w') as f:
                    f.write(f"pid={os.getpid()}\n")
                return True
            except Exception:
                try:
                    os.close(fd)
                except Exception:
                    pass
                raise
        except FileExistsError:
            await asyncio.sleep(0.25)
        except Exception:
            # Unexpected error creating lock; do not block start because of this
            break
    return False


def _release_session_lock(db_path: str):
    lock = _session_lock_path(db_path)
    try:
        if os.path.exists(lock):
            os.remove(lock)
    except Exception:
        pass


def _inc_ref(session_path: str):
    _OPEN_CLIENTS_RC[session_path] = _OPEN_CLIENTS_RC.get(session_path, 0) + 1


def _dec_ref(session_path: str) -> int:
    if session_path in _OPEN_CLIENTS_RC:
        _OPEN_CLIENTS_RC[session_path] -= 1
        if _OPEN_CLIENTS_RC[session_path] <= 0:
            _OPEN_CLIENTS_RC.pop(session_path, None)
            return 0
        return _OPEN_CLIENTS_RC[session_path]
    return 0


class Client:
    EXTENSION = '.session'
    def __init__(self, session_name: str, api_id: int, api_hash: str, device_model: str, system_version: str,
                 app_version: str, phone=None, proxy: str = '', password=None):
        self._session_name = session_name
        self._api_id = api_id
        self._api_hash = api_hash
        self._device_model = device_model
        self._system_version = system_version
        self._app_version = app_version
        self._phone = phone
        self._proxy = proxy
        self._password = password

    @property
    def session_name(self):
        # Use internal session_store under telegradd/connect/sessions
        name = Path(pathlib.Path(__file__).parents[1], 'sessions', 'session_store', f'{self._session_name}{self.EXTENSION}')
        return str(name)

    @property
    def proxy(self) -> tuple | dict | str:
        return self._proxy

    def proxy_setter(self, str_proxy):
        proxy_list = str_proxy.split(":")
        if proxy_list[0] == 'mtp':
            proxy = (proxy_list[1], int(proxy_list[2]), proxy_list[3])
            self._proxy = (proxy_list[0], proxy)
        elif proxy_list[0] == 'fakeTls':
            proxy = (proxy_list[1], int(proxy_list[2]), proxy_list[3], proxy_list[4])
            self._proxy = (proxy_list[0], proxy)
        elif proxy_list[0] == 'socks5':
            proxy = {'proxy_type': proxy_list[0],
                     'addr': proxy_list[1],
                     'port': int(proxy_list[2]),
                     'username': proxy_list[3],
                     'password': proxy_list[4],
                     'rdns': True}

            self._proxy = proxy
        else:
            self._proxy = ''  # unsupported proxy format or no proxy

    async def client(self) -> TelegramClient:
        if self.proxy == '':
            return TelegramClient(session=self.session_name, api_id=self._api_id, api_hash=self._api_hash, device_model=self._device_model,
                                   system_version=self._system_version, app_version=self._app_version)
        elif self.proxy[0] == 'mtp':
            self.proxy_setter(self._proxy)
            return TelegramClient(session=self.session_name, api_id=self._api_id, api_hash=self._api_hash, device_model=self._device_model,
                                   system_version=self._system_version, app_version=self._app_version,
                                   connection=connection.ConnectionTcpMTProxyRandomizedIntermediate, proxy=self.proxy)
        elif self.proxy[0] == 'fakeTls':
            self.proxy_setter(self._proxy)
            return TelegramClient(session=self.session_name, api_id=self._api_id, api_hash=self._api_hash, device_model=self._device_model,
                                   system_version=self._system_version, app_version=self._app_version, proxy=self.proxy,
                                   connection=TelethonFakeTLS.ConnectionTcpMTProxyFakeTLS)
        else:
            self.proxy_setter(self._proxy)
            print(self.proxy)
            return TelegramClient(session=self.session_name, api_id=self._api_id, api_hash=self._api_hash, device_model=self._device_model, system_version=self._system_version, app_version=self._app_version,
                                   proxy=self.proxy)

    @property
    async def start(self) -> TelegramClient | None:
        global _WITHOUT_PROXY_GLOBAL_CHOICE

        # Reuse an already-open client for this session if available
        existing = _OPEN_CLIENTS.get(self.session_name)
        if existing is not None:
            _inc_ref(self.session_name)
            print(f'Reusing already open session: {Path(self.session_name).parts[-1]}')
            return existing

        if self.proxy == '':
            # Respect global choice if already set
            if _WITHOUT_PROXY_GLOBAL_CHOICE == 'all_no':
                print(f'Ok, Skipping {Path(self.session_name).parts[-1]}')
                return None
            elif _WITHOUT_PROXY_GLOBAL_CHOICE == 'all_yes':
                pass  # proceed without asking
            else:
                without_proxy = input(
                    f'Do u want to start {Path(self.session_name).parts[-1]} without proxy (y/n/ya/na): ')
                without_proxy = without_proxy.lower()
                if without_proxy == 'ya':
                    _WITHOUT_PROXY_GLOBAL_CHOICE = 'all_yes'
                elif without_proxy == 'na':
                    _WITHOUT_PROXY_GLOBAL_CHOICE = 'all_no'
                    print(f'Ok, Skipping {Path(self.session_name).parts[-1]}')
                    return None
                elif without_proxy != 'y':
                    print(f'Ok, Skipping {Path(self.session_name).parts[-1]}')
                    return None

        # Acquire per-session lock before creating/starting TelegramClient
        acquired = await _acquire_session_lock(self.session_name, wait_seconds=5.0)
        if not acquired:
            print(f'Session is currently in use, skipping {Path(self.session_name).parts[-1]}')
            return None

        print(f'Starting log in to {Path(self.session_name).parts[-1]}')
        try:
            client = await self.client()
            await client.start(self._phone, password=self._password, max_attempts=2)

            # Wrap disconnect to ensure registry cleanup and consistent disconnection
            orig_disconnect = client.disconnect
            
            async def _wrapped_disconnect(*args, **kwargs):
                # Decrement refcount; only truly disconnect when last user exits
                remaining = _dec_ref(self.session_name)
                if remaining > 0:
                    return True
                try:
                    return await orig_disconnect(*args, **kwargs)
                finally:
                    # Remove from registry only if it still points to this instance
                    if _OPEN_CLIENTS.get(self.session_name) is client:
                        _OPEN_CLIENTS.pop(self.session_name, None)

            client.disconnect = _wrapped_disconnect

            # Register this open client instance and set initial refcount
            _OPEN_CLIENTS[self.session_name] = client
            _inc_ref(self.session_name)

            if self.proxy != '':
                print(f'Succesfully log in to {Path(self.session_name).parts[-1]} with {self.proxy}')
            else:
                print(f'Succesfully log in to {Path(self.session_name).parts[-1]} without proxy')
            profileInfo = await client.get_me()
            print(profileInfo.id, profileInfo.first_name, profileInfo.last_name)
            await client.PrintSessions()
            return client
        except telethon.errors.PhoneNumberBannedError:
            print('This account was banned, deleting from db...')
            # delete from bd and move to banned accounts
            Database().delete_account(name=self._session_name)
            return None
        except RuntimeError:
            ses = input('Cant log in with this credentials, delete session (y/n): ')
            if ses == 'y':
                Database().delete_account(name=self._session_name)
        except Exception as err:
            # Handle locked Telethon session DB: attempt cleanup and one retry
            if 'database is locked' in str(err).lower():
                _cleanup_sqlite_sidecars(self.session_name)
                await asyncio.sleep(0.5)
                try:
                    client = await self.client()
                    await client.start(self._phone, password=self._password, max_attempts=2)

                    # Wrap disconnect and register (in case first attempt failed before we did)
                    orig_disconnect = client.disconnect

                    async def _wrapped_disconnect2(*args, **kwargs):
                        remaining = _dec_ref(self.session_name)
                        if remaining > 0:
                            return True
                        try:
                            return await orig_disconnect(*args, **kwargs)
                        finally:
                            if _OPEN_CLIENTS.get(self.session_name) is client:
                                _OPEN_CLIENTS.pop(self.session_name, None)

                    client.disconnect = _wrapped_disconnect2
                    _OPEN_CLIENTS[self.session_name] = client
                    _inc_ref(self.session_name)

                    if self.proxy != '':
                        print(f'Succesfully log in to {Path(self.session_name).parts[-1]} with {self.proxy} (after unlocking)')
                    else:
                        print(f'Succesfully log in to {Path(self.session_name).parts[-1]} without proxy (after unlocking)')
                    return client
                except Exception as e2:
                    print(f'Failed to connect to {self.session_name} after unlocking: {e2}')
                    return None
            print(f'Something went wrong, failed to connect to {self.session_name} cuz {err}')
            traceback.print_exc()
            return None
        finally:
            _release_session_lock(self.session_name)

TABLE = """CREATE TABLE IF NOT EXISTS 
                Accounts (
                Number INTEGER PRIMARY KEY NOT NULL, 0
                Name TEXT,  1
                Api_id INTEGER, 2
                Api_hash TEXT,  3
                System TEXT, 4
                Proxy TEXT, 5
                Phone TEXT,  6
                Password TEXT, 7
                Restrictions TEXT 8
                )"""


class TELEGRADD_client:
    def __init__(self, auth: tuple = ('all', )):
        self._auth = auth

    async def clients(self, restriction=False):
        Database().automatically_delete_restrictions()
        clients = []
        credentials = Database().get_all(self._auth)
        if credentials and (restriction is False):
            clients = [await Client(data[1], int(data[2]), data[3], (data[4]).split(":")[0], (data[4]).split(":")[1],
                                   (data[4]).split(":")[2], phone=data[6], proxy=data[5], password=data[7]).start for data in credentials]
        elif credentials and restriction:
            clients = [await Client(data[1], int(data[2]), data[3], (data[4]).split(":")[0], (data[4]).split(":")[1],
                                   (data[4]).split(":")[2], phone=data[6], proxy=data[5], password=data[7]).start for data in credentials if data[8] == 'False']
        else:
            print('U havent any accounts with the given number(s)')
            return

        clients = [client for client in clients if client is not None]
        if clients:
            return clients
        else:
            print('None of ur accounts can be used')
            return False

    async def iter_clients(self, restriction=False):
        """Async generator that yields connected clients and ensures they disconnect after use.
        Reuses current open sessions and reference counts them to avoid premature disconnects.
        Usage:
            async for client in TELEGRADD_client(('all',)).iter_clients():
                # use client here
                ...
        """
        Database().automatically_delete_restrictions()
        credentials = Database().get_all(self._auth)
        if not credentials:
            print('U havent any accounts with the given number(s)')
            return
        for data in credentials:
            if restriction and data[8] != 'False':
                continue
            client = await Client(data[1], int(data[2]), data[3], (data[4]).split(":")[0], (data[4]).split(":")[1],
                                  (data[4]).split(":")[2], phone=data[6], proxy=data[5], password=data[7]).start
            if client is None:
                continue
            # Ensure disconnect after caller finishes with this iteration
            try:
                async with client:
                    yield client
            except Exception:
                # If any exception occurs during usage, ensure disconnect is triggered by exiting context
                pass


async def main():
    clients = await TELEGRADD_client(('all',)).clients(restriction=False)
    if not clients:
        return
    for client in clients:
        async with client:
            me = await client.get_me()
            print(me)


if __name__ == '__main__':
    asyncio.run(main())
