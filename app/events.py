import asyncio
from collections import defaultdict
import datetime
import json
import logging
import os
import traceback

import httpx
import websockets

from .config import config
from .db import save_event
from .logging import logger


logging.getLogger("websockets").setLevel(getattr(logging, config['LOGGING_LEVEL']))
logging.getLogger("websockets").addHandler(logging.StreamHandler())

FILTER = {}

def analyze_filter(f):
    v = defaultdict(list)
    for key, value in sorted(f.items()):
        v[value].append(key)
    return { key: len(v[key]) for key in v }

def apply_filter(msg):
    cond = []

    try:
        cond = [
            msg['triggerName'] == 'solidityEventTrigger',
            msg['eventName'] == 'Transfer',
            msg['topicMap']['to'] in FILTER,
        ]
    except Exception as e:
        # logger.exception(f"Exception while appling filter to {msg}:")
        pass
    return bool(cond and all(cond))

async def notify_shkeeper(symbol, txid):
    while True:
        try:
            async with httpx.AsyncClient() as client:
                r = await client.post(
                    f'http://{config["SHKEEPER_HOST"]}/api/v1/walletnotify/{symbol}/{txid}',
                    headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']}
                )
                return r
        except Exception as e:
            logger.warning(f'Shkeeper notification failed for {symbol}/{txid}: {e}')
            await asyncio.sleep(10)

async def ws_main():

    while not FILTER:
        logger.debug('Waiting for filter to be set.')
        await asyncio.sleep(1)
    logger.info(f'Filter has been set. Total accounts: {analyze_filter(FILTER)}')

    ws_url = f"ws://{config['TRON_NODE_USERNAME']}:{config['TRON_NODE_PASSWORD']}@{config['EVENT_SERVER_HOST']}"
    logger.info(f"Connecting to the event server at {ws_url}...")
    async for websocket in websockets.connect(ws_url, ping_timeout=None):
        logger.info(f'Connected to {ws_url}')
        try:
            async for message in websocket:
                try:
                    event = json.loads(message)

                    logger.debug(f'Event received: {datetime.datetime.fromtimestamp(event["timeStamp"] / 1000)} {event["transactionId"]}')
                    if apply_filter(event):
                        save_event(event['transactionId'], message)
                        symbol = FILTER[event['topicMap']['to']]
                        await notify_shkeeper(symbol, event['transactionId'])
                    else:
                        logger.debug(f'Event ignored: {datetime.datetime.fromtimestamp(event["timeStamp"] / 1000)} {event["transactionId"]}')

                except Exception as e:
                    logger.exception(f"Message processing exception: {message} {traceback.format_exc()}")
                    continue
        except websockets.ConnectionClosed:
            logger.info('Server closed the connection, reconneting.')
        except Exception as e:
            logger.exception(f"Exception in event listener")

def events_listener():
    asyncio.run(ws_main(), debug=False)
