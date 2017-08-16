import sys
import json
import time
try:
    import Queue
except ImportError:
    import queue as Queue
import requests
import signal
import logging
import gevent
from gevent import monkey
from pymongo import MongoClient
from logging.config import dictConfig
from collections import defaultdict
from procbridge import procbridge
monkey.patch_all()
import socket
import select

timeout = 1000
HOST = '192.168.1.135'
MCAST_ADDR = '234.5.6.7'
MCAST_PORT_LIST = range(59433, 59447)
TCP_SERVER_PORT = 50001
READ_ONLY = (select.POLLIN | select.POLLPRI | select.POLLHUP | select.POLLERR)
READ_WRITE = (READ_ONLY | select.POLLOUT)
DATA_UPLOAD_API = "http://127.0.0.1:3000/api/nano_grids/dataUpload"
proc_service = procbridge.ProcBridge('127.0.0.1', 8200)

device_map = {
    "WyNfGzjR6dVwPM4jN9ay8n": {"name": "PEM", "port": 59439},
    "FnT5gWCUwLEoMZUFhpAfAm": {"name": "FR", "port": 59437},
    "D3eNJiXb4irfNzUofTFbJb": {"name": "HMI", "port": 59448},
    "Uw5LrQ4VQqivpWk4ShcW8": {"name": "BatA", "port": 59435},
    "vEhjAmaEX87QbMcK8jixYn": {"name": "BatB", "port": 59436},
    "GsyVca5AXgXjfRrctDmMnH": {"name": "Load", "port": 59442},
    "JiJtiAqxXnDMVRB4T8H8vW": {"name": "PCS Test", "port": 59447},
    "suUx5wwxcgVQMgnaZzSzDH": {"name": "Solar", "port": 59434},
    "PfNnkvCPB3WymwQSLX4zqR": {"name": "H2G", "port": 59441},
    "uVQH7GmNsiwqdUYvoqenEQ": {"name": "Wind", "port": 59433},
    "jpXwNvVX4umZqkWgNsVDKj": {"name": "Switch", "port": 59446},
    "JdbL6SjeTVJC4P9bqz7CVM": {"name": "SOC", "port": 59440},
    "vbRJrDp3uFzUCafEDtkA9Q": {"name": "Flow", "port": 59438},
    "YYeqV8tpBS2zb3qcv7oCJU": {"name": "AI_Air", "port": 59443}
}

logging_config = dict(
    version=1,
    formatters={
        'f': {'format': '[%(levelname)s] %(asctime)s %(name)s [%(lineno)d] %(message)s'}
    },
    handlers={
        'h': {'class': 'logging.StreamHandler', 'formatter': 'f', 'level': logging.DEBUG}
    },
    root={'handlers': ['h'], 'level': logging.DEBUG}
)
dictConfig(logging_config)
logger = logging.getLogger('NanoGrid')

db = MongoClient('mongodb://eos:eos12345678@127.0.0.1:27017/NanoGridData').NanoGridData

message_queues = Queue.Queue()
data_queue = Queue.Queue()


def signal_handler(sig, frame):
    logger.info('Caught signal: %s, process quit now ...', sig)
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


def exec_time(func):
    def _func(*args, **args2):
        t0 = time.time()
        logger.debug("@%s, {%s} start" % (time.strftime("%X", time.localtime()), func.__name__))
        back = func(*args, **args2)
        logger.debug("@%s, {%s} end" % (time.strftime("%X", time.localtime()), func.__name__))
        logger.info("@%.3fs taken for {%s}" % (time.time() - t0, func.__name__))
        return back
    return _func


def get_device_port_by_id(device_id):
    return device_map.get(device_id, {}).get('port')


def gen_udp_socket(port):
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 32)
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            s.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF, socket.inet_aton(HOST))
            s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, socket.inet_aton(MCAST_ADDR)
                         + socket.inet_aton(HOST))

            s.bind((MCAST_ADDR, port))
            return s
        except socket.error as e:
            logger.error(e)
            time.sleep(1)


def gen_tcp_socket(port):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setblocking(False)
            s.bind(('', port))
            s.listen(10)
            return s
        except socket.error as e:
            logger.error(e)
            return None


def cmd_server():
    tcp_poller = select.poll()
    server = gen_tcp_socket(TCP_SERVER_PORT)
    if not server:
        return

    tcp_poller.register(server, READ_ONLY)
    fd_to_socket = {server.fileno(): server}

    while True:
        events = tcp_poller.poll(timeout)
        for fd, flag in events:
            s = fd_to_socket[fd]
            if flag & (select.POLLPRI | select.POLLIN):
                if s is server:
                    connection, client_address = s.accept()
                    logger.info("Connection from: %s:%d", client_address[0], client_address[1])
                    connection.setblocking(False)
                    fd_to_socket[connection.fileno()] = connection
                    tcp_poller.register(connection, READ_ONLY)
                else:
                    data = s.recv(1024)
                    if not data:
                        peer_addr = s.getpeername()
                        logger.info("Client %s:%d closed", peer_addr[0], peer_addr[1])
                        tcp_poller.unregister(s)
                        s.close()
                    else:
                        try:
                            d = json.loads(data.decode('utf-8'))
                            logger.info(d)
                            if all([d.get('device_id'), d.get('cmd'), d.get('para_len')]):
                                message_queues.put(d)
                            else:
                                logger.error('Invalid msg: %s', data)
                        except ValueError as e:
                            logger.error(e)
                            tcp_poller.unregister(s)
                            s.close()
            elif flag & select.POLLHUP:
                logger.info("Client %s:%d Closed (HUP)", s.getpeername()[0], s.getpeername()[1])
                tcp_poller.unregister(s)
                s.close()
            elif flag & select.POLLERR:
                logger.error("Exception on %s:%d", s.getpeername()[0], s.getpeername()[1])
                tcp_poller.unregister(s)
                s.close()


def recv_device_data():
    udp_poller = select.poll()
    fd_to_socket = {}

    with open('sensor_map.json') as f:
        try:
            sensor_map = json.loads(f.read())
        except ValueError as e:
            logger.error('sensor_map load failed: %s', e)
            return

    with open('device_token_map.json') as f_token_map:
        try:
            device_token_map = json.loads(f_token_map.read())
        except (ValueError, IOError) as e:
            logger.error(e)
            return

    server_list = map(gen_udp_socket, MCAST_PORT_LIST)
    for _ in server_list:
        udp_poller.register(_, READ_WRITE)
        fd_to_socket[_.fileno()] = _

    while True:
        events = udp_poller.poll(timeout)
        for fd, flag in events:
            if flag & (select.POLLIN | select.POLLPRI):
                s = fd_to_socket.get(fd)
                if not s:
                    logger.error("Unknown server")
                    continue
                try:
                    d, a = s.recvfrom(10240)
                    if not d:
                        continue
                except socket.error as e:
                    logger.error(e)
                    continue

                try:
                    data = json.loads(d.decode('utf-8'))
                except ValueError as e:
                    logger.error(e)
                    logger.error(d)
                    continue

                if data.get('cmd'):
                    logger.info('Got cmd: %s', d)
                    continue

                data['timestamp'] = int(time.time() * 1000)
                dev_id = data.get('dev_id')

                try:
                    db.nano_grid_origin.insert_one(data)
                except Exception as e:
                    logger.error(e)

                data_list = list(filter(lambda _: _.get('sensor_id'), map(lambda x: {
                    'sensor_id': sensor_map.get(dev_id, {}).get(x[0], {}).get('sensor_id'),
                    'data_type': sensor_map.get(dev_id, {}).get(x[0], {}).get('data_type', -1),
                    'timestamp': int(time.time() * 1000),
                    'payload': x[1],
                    'desc': x[0]
                }, data.get('runData', {}).items())))

                token = device_token_map.get(dev_id.upper(), {}).get('token', '')
                data_queue.put({'token': token, 'data': data_list, 'address': a, 'dev_id': dev_id})
            if flag & select.POLLOUT:
                try:
                    msg = message_queues.get_nowait()
                except Queue.Empty:
                    continue
                port = get_device_port_by_id(msg.get('device_id'))
                if not port:
                    logger.error("Unknown device: %s", msg.get('device_id'))
                    continue
                s = fd_to_socket.get(fd)
                logger.debug(msg)
                msg_to_send = {
                    'cmd': msg.get('cmd'),
                    'para_len': msg.get('para_len'),
                    'parameter': msg.get('parameter', [])
                }
                s.sendto(json.dumps(msg_to_send).encode('utf-8'), (MCAST_ADDR, port))
                logger.debug(msg_to_send)
            elif flag & select.POLLHUP:
                logger.info('POLLHUP')
            elif flag & select.POLLERR:
                logger.info('POLLERR')
        gevent.sleep(0.2)


@exec_time
def upload_data_to_cloud(task_id=-1):
    log_count = defaultdict(lambda: 0)

    while True:
        logger.info("[%d] data_queue length: %d", task_id, data_queue.qsize())
        try:
            msg = data_queue.get(timeout=3)
        except Queue.Empty:
            logger.error('Empty data queue')
            gevent.sleep(1)
            continue
        try:
            address, port = msg.get('address')
            dev_id = msg.get('dev_id')
            token = msg.get('token', '')
            # logger.info('token: %s', token)
            data_list = msg.get('data', [])
            t1 = time.time()
            r = proc_service.request('data', {'data': data_list})
            t2 = time.time()
            rsp = requests.post(DATA_UPLOAD_API, json={'token': token, 'data': data_list}).content
            t3 = time.time()
            logger.info("[%d] t2-t1: %.3f, t3-t2: %.3f", task_id, t2-t1, t3-t2)
            logger.info(rsp)
        except Exception as e:
            logger.error(e)
            continue

        if log_count[dev_id] >= 60:
            log_count[dev_id] = 0
            logger.info('[%s:%d] dev_id: %-12s send_len: %d response: %s',
                        address, port, dev_id, len(data_list), r)
        log_count[dev_id] += 1


if __name__ == '__main__':
    gevent.joinall([gevent.spawn(cmd_server), gevent.spawn(recv_device_data)] +
                   list(map(lambda _: gevent.spawn(upload_data_to_cloud, _), range(20))))
