import datetime
import json
import time
from urllib.parse import urlparse

import requests

from tronpy import Tron
from tronpy.providers import HTTPProvider

from .config import TronFullnode, config
from .db import query_db2
from .logging import logger
from .exceptions import AllServersOffline, NoServerSet


class ConnectionManager:

    instance = None

    @classmethod
    def get_instance(cls) -> "ConnectionManager":
        if not cls.instance:
            cls.instance = cls()
        return cls.instance

    @classmethod
    def client(cls) -> Tron:
        return cls.get_instance().get_client()

    @classmethod
    def manager(cls) -> "ConnectionManager":
        return cls.get_instance()

    def __init__(self) -> None:
        if config.MULTISERVER_CONFIG_JSON:
            self.servers = config.MULTISERVER_CONFIG_JSON
        elif config.FULLNODE_URL:
            url = urlparse(config.FULLNODE_URL)
            new_netloc = (
                f"{config.TRON_NODE_USERNAME}:{config.TRON_NODE_PASSWORD}@{url.netloc}"
            )
            url_with_creds = url._replace(netloc=new_netloc).geturl()
            self.servers = [TronFullnode(name=url.hostname, url=url_with_creds)]
        else:
            raise Exception(
                "No FULLNODE_URL or MULTISERVER_CONFIG_JSON env variables are set!"
            )

    def get_client(self) -> Tron:
        server_id = self.get_current_server_id()
        if server_id is None:
            raise NoServerSet("Current server is not set.")
        client = self.get_client_for_server_id(server_id)
        return client

    def get_client_for_server_id(self, server_id) -> Tron:
        provider = HTTPProvider(self.servers[server_id].url)
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=100)
        provider.sess.mount("http://", adapter)
        provider.sess.mount("https://", adapter)
        return Tron(provider)

    def get_current_server_id(self):
        row = query_db2(
            'SELECT value FROM settings WHERE name = "current_server_id"', one=True
        )
        if row:
            server_id = int(row["value"])
        else:
            server_id = None
        return server_id

    def set_current_server_id(self, server_id):
        query_db2(
            'UPDATE settings SET value = ? WHERE name = "current_server_id"',
            (server_id,),
        )
        logger.info(f"Current server ID is set to: {server_id}")

    def get_servers_status(self):
        servers_status = []
        for server_id, server in enumerate(self.servers):
            try:
                # node info
                resp = requests.get(f"{server.url}/wallet/getnodeinfo")
                resp.raise_for_status()
                node_info = resp.json()

                # remove unneeded info
                del node_info["peerList"]
                del node_info["machineInfo"]["memoryDescInfoList"]

                # convert "Num:XXX,ID:YYY" to XXX
                node_info["block"] = int(
                    [j for i in node_info["block"].split(",") for j in i.split(":")][1]
                )

                # last block info
                resp = requests.post(
                    f"{server.url}/wallet/getblockbynum",
                    json={"num": node_info["block"]},
                )
                resp.raise_for_status()
                block = resp.json()
                node_info["block_ts"] = (
                    block["block_header"]["raw_data"]["timestamp"] // 1000
                )

                # Lag
                now_ts = int(datetime.datetime.now().timestamp())
                delta = now_ts - node_info["block_ts"]
                delta = 0 if delta < 0 else delta
                node_info["lag"] = str(datetime.timedelta(seconds=delta))

                status = {
                    "id": server_id,
                    "is_active": self.get_current_server_id() == server_id,
                    **server.model_dump(),
                    "status": "success",
                    "node_info": node_info,
                }
            except Exception as e:
                logger.info(f"Failed to get server {server.url} status: {e}")
                status = {
                    "id": server_id,
                    "is_active": self.get_current_server_id() == server_id,
                    **server,
                    "status": "error",
                    "error": str(e),
                }
            finally:
                servers_status.append(status)
        return servers_status

    def get_best_server_id(self):
        if len(self.servers) == 1:
            return 0
        servers = self.get_servers_status()
        # filter out unreachable servers
        online_servers = list(filter(lambda x: x["status"] == "success", servers))
        if not online_servers:
            raise AllServersOffline("All servers are unreachable!")
        # get server with max block height
        server_id = max(online_servers, key=lambda x: x["node_info"]["block"])["id"]
        return server_id

    def refresh_best_server(self) -> bool:
        best_server_id = self.get_best_server_id()
        if best_server_id != self.get_current_server_id():
            self.set_current_server_id(best_server_id)
            logger.info(f"Best server {best_server_id} is set as current server.")
            return True
        return False

    def refresh_best_server_thread_handler(self):

        if self.get_current_server_id() is None:
            while True:
                try:
                    server_id = self.get_best_server_id()
                    query_db2(
                        'INSERT INTO settings VALUES ("current_server_id", ?)',
                        (server_id,),
                    )
                    logger.info(f"Current server set to: {server_id}")
                    break
                except Exception as e:
                    logger.warning(f"Current server set error: {e}")
                    time.sleep(config.MULTISERVER_REFRESH_BEST_SERVER_PERIOD)

        while True:
            try:
                self.refresh_best_server()
            except Exception as e:
                logger.info(f"Exception in best server refresh loop: {e}")
            finally:
                time.sleep(config.MULTISERVER_REFRESH_BEST_SERVER_PERIOD)
