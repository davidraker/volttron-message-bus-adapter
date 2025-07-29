import json
import logging
import gevent
from gevent import joinall, sleep, spawn

from protocol_proxy.ipc import callback, ProtocolHeaders, ProtocolProxyMessage
from protocol_proxy.manager import ProtocolProxyManager
from protocol_proxy.mqtt import MQTTProxy

logging.basicConfig(filename='protoproxy.log', level=logging.DEBUG,
                    format='%(asctime)s - %(message)s')
_log = logging.getLogger(__name__)


class MessageBusAdapter:
    def __init__(self):
        self.ppm = ProtocolProxyManager.get_manager(MQTTProxy)
        self.ppm.register_callback(self.handle_publish_local, 'PUBLISH_LOCAL')

    def run(self):
        self.ppm.get_proxy(('mqtt','proxy', 1))
        joinall([spawn(self.main_loop), spawn(self.ppm.select_loop),
                 gevent.spawn_later(10, self.subscribe, peer=self.ppm.get_proxy_id(('mqtt', 'proxy', 1)),
                                    topics=['$SYS/#'])])  # TODO: Use PPM.wait_peer_registered once it exists.

    def main_loop(self):
        while not self.ppm._stop:
           # _log.debug(f'{self.ppm.proxy_id}: IN MAIN LOOP')
            sleep(0.1)

    @callback
    def handle_publish_local(self,  headers: ProtocolHeaders, raw_message: bytes):
        message = json.loads(raw_message.decode('utf8'))
        _log.debug(f"RECEIVED MESSAGE FROM MQTT: \n\tTOPIC: {message['topic']}\n\tMESSAGE: {message['payload']}")

    def subscribe(self, peer, topics):
        # _log.debug('MBA: IN SUBSCRIBE.')
        message = ProtocolProxyMessage(
            method_name='SUBSCRIBE_REMOTE',
            payload=json.dumps({'topics': topics}).encode('utf8')
        )
        # _log.debug(f'IN SUBSCRIBE, PROXY_ID IS APPEARS TO BE: {self.ppm.proxy_id}')
        # _log.debug(f'IN SUBSCRIBE, PEERS APPEARS TO BE: {self.ppm.peers}')
        # _log.debug(f'IN SUBSCRIBE, UNIQUE_IDS ARE: {self.ppm.unique_ids}')
        self.ppm.send(self.ppm.peers[peer], message)
        # _log.debug('MBA: SUBSCRIBE COMPLETED.')

    def publish(self, peer, topic, payload):
        # _log.debug('MBA: IN PUBLISH.')
        message = ProtocolProxyMessage(
            method_name='PUBLISH_REMOTE',
            payload=json.dumps({'topic': topic, 'payload': payload}).encode('utf8')
        )
        self.ppm.send(self.ppm.peers[peer], message)
