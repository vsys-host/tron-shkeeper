import requests
import prometheus_client
from prometheus_client import generate_latest, Info, Gauge

from . import metrics_blueprint
from ..block_scanner import BlockScanner
from ..connection_manager import ConnectionManager


prometheus_client.REGISTRY.unregister(prometheus_client.GC_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PLATFORM_COLLECTOR)
prometheus_client.REGISTRY.unregister(prometheus_client.PROCESS_COLLECTOR)

def get_latest_release():
    data = requests.get('https://api.github.com/repos/tronprotocol/java-tron/releases/latest').json()
    version = data["tag_name"].split('-v')[1]
    info = { key:data[key] for key in ["name", "tag_name", "published_at"] }
    info['version'] = version
    return info
tron_fullnode_last_release = Info(
    'tron_fullnode_last_release',
    'Version of the latest release from https://github.com/tronprotocol/java-tron/releases'
)
tron_fullnode_last_release.info(get_latest_release())

tron_fullnode_version = Info('tron_fullnode_version', '', ('server', 'version'))
tron_fullnode_last_block = Gauge('tron_fullnode_last_block', '', ('server',))
tron_wallet_last_block = Gauge('tron_wallet_last_block', '').set_function(lambda: BlockScanner().get_last_seen_block_num())

@metrics_blueprint.get("/metrics")
def get_metrics():
    for server in filter(lambda x: x["status"] == "success", ConnectionManager.manager().get_servers_status()):
        tron_fullnode_version.labels(server=server['name'], version=server["node_info"]["configNodeInfo"]["codeVersion"])
        tron_fullnode_last_block.labels(server=server['name']).set(server["node_info"]["block"])
    return generate_latest().decode()
