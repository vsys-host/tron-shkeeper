import functools
import time
import requests
import prometheus_client
from prometheus_client import generate_latest, Info, Gauge

from . import metrics_blueprint
from ..block_scanner import BlockScanner
from ..connection_manager import ConnectionManager


prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)


def get_ttl_hash(seconds=1*60*60*24):
    """Return the same value withing `seconds` time period"""
    return round(time.time() / seconds)

@functools.lru_cache(maxsize=2)
def get_latest_release(ttl_hash=None):
    data = requests.get('https://api.github.com/repos/tronprotocol/java-tron/releases/latest').json()
    version = data["tag_name"].split('-v')[1]
    info = { key:data[key] for key in ["name", "tag_name", "published_at"] }
    info['version'] = version
    return info
tron_fullnode_last_release = Info(
    'tron_fullnode_last_release',
    'Version of the latest release from https://github.com/tronprotocol/java-tron/releases'
)

tron_fullnode_status = Gauge('tron_fullnode_status', '', ('server',))
tron_fullnode_version = Info('tron_fullnode_version', '', ('server',))
tron_fullnode_last_block = Gauge('tron_fullnode_last_block', '', ('server',))
tron_fullnode_last_block_ts = Gauge('tron_fullnode_last_block_ts', '', ('server',))
tron_wallet_last_block = Gauge('tron_wallet_last_block', '')
tron_wallet_last_block_ts = Gauge('tron_wallet_last_block_ts', '')
tron_has_alive_servers = Gauge('tron_has_alive_servers', '')

@metrics_blueprint.get("/metrics")
def get_metrics():
    bs = BlockScanner()
    last_seen_block_num = bs.get_last_seen_block_num()
    tron_wallet_last_block.set(last_seen_block_num)
    tron_wallet_last_block_ts.set(bs.download_block(last_seen_block_num)['block_header']['raw_data']['timestamp'] // 1000)
    tron_fullnode_last_release.info(get_latest_release(ttl_hash=get_ttl_hash()))

    for server in ConnectionManager.manager().get_servers_status():
        if server['status'] == "success":
            tron_has_alive_servers.set(1)
            tron_fullnode_status.labels(server=server['name']).set(1)
            tron_fullnode_version.labels(server=server['name']).info({'version': server["node_info"]["configNodeInfo"]["codeVersion"]})
            tron_fullnode_last_block.labels(server=server['name']).set(server["node_info"]["block"])
            tron_fullnode_last_block_ts.labels(server=server['name']).set(server["node_info"]["block_ts"])
        else:
            tron_fullnode_status.labels(server=server['name']).set(0)
    return generate_latest().decode()
