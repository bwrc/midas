#!/usr/bin/env python3

# This file is part of the MIDAS system.
# Copyright 2014
# Andreas Henelius <andreas.henelius@ttl.fi>,
# Jari Torniainen <jari.torniainen@ttl.fi>
# Finnish Institute of Occupational Health
#
# This code is released under the MIT License
# http://opensource.org/licenses/mit-license.php
#
# Please see the file LICENSE for details.

import sys
import zmq
import time
import select
import socket
import os.path
import threading
import configparser
from multiprocessing import Lock, Value


class Beacon(object):

    """ A UDP broadcast beacon with some functions allowing easy use. """

    def __init__(self,
                 name='',
                 node_type='',
                 node_id='',
                 ip=None,
                 port='',
                 protocol='tcp',
                 status='',
                 port_broadcast=5670,
                 interval=5):
        """ Create the beacon and set some properties, but do not start it. """

        self.name = name
        self.type = node_type
        self.id = node_id
        self.ip = ip
        self.port = port
        self.protocol = protocol
        self.status = status
        self.is_running = False
        self.data = ''
        self.port_broadcast = port_broadcast
        self.interval = interval
    # -------------------------------------------------------------------------

    def start(self):
        """ Start broadcasting data on the beacon, i.e., make it visible. """
        if self.ip is None:
            self.ip = get_ip()

        self.update_data()

        self.is_running = True
        t = threading.Thread(target=self.broadcast)
        t.start()

    def broadcast(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        while self.is_running:
            try:
                s.sendto(self.data, ('<broadcast>', self.port_broadcast))
            except OSError:
                s.sendto(self.data, ('localhost', self.port_broadcast))
            time.sleep(self.interval)

    def stop(self):
        """ Stop the beacon. """
        if self.is_running:
            self.is_running = False

    def update_data(self):
        url_node = '{}://{}:{}'.format(self.protocol, self.ip, self.port)

        data = ';'.join(['midas',
                         str(self.name),
                         str(self.type),
                         str(self.id),
                         url_node,
                         str(self.status)
                         ])
        self.data = str.encode(data)

    def set_status(self, status):
        """ Set the status of the node.
            If the node is already broadcasting, change the message in the
            broadcast.
        """
        self.status = status

        if self.is_running:
            self.stop()
            self.start()


class DataState(object):

    """ Thread-safe boolean that can, e.g., be used to keep track of whether
        there is new data or not
    """

    def __init__(self, initial_state=0):
        self.state = Value('i', initial_state)
        self.lock = Lock()

    def setstate(self, val):
        with self.lock:
            self.state.value = val

    def flipstate(self):
        with self.lock:
            self.state.value ^= True

    def getstate(self):
        return(self.state.value)


def discover_all_nodes(timeout=10, port_broadcast=5670):
    """ Discover all MIDAS nodes and return them as a dictionary."""

    # Loop until the socket is free.
    # This is needed in order to avoid conflicts when multiple dispatchers
    # are used on the same host.
    while True:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.bind(('', port_broadcast))
            s.setblocking(0)
            break
        except OSError:
            pass

    buffersize = 1024
    t_start = time.time()

    tmp_list = []
    node_dict = {}

    while(time.time() - t_start < timeout):
        result = select.select([s], [], [], timeout)
        if result[0]:
            message = result[0][0].recv(buffersize)
            message = message.decode('ascii')
            if message.startswith('midas'):
                message = validate_message(message)
                if message not in tmp_list:
                    tmp_list.append(message)
                    node_dict[message['name']] = message

    s.close()

    return node_dict


def validate_message(message):
    """ Validate a received message to make sure that it
        is a valid message in the MIDAS framework and return
        a dictionary containing the information sent by the beacon.
    """
    message = message.split(';')
    result = None

    if message[0] == 'midas':
        k = ['name', 'type', 'id', 'address', 'status']
        result = dict(zip(k, message[1:]))

    return result


def filter_nodes(node_dict, f={}):
    """ Filter nodes based on criteria in the filter dictionary.

       Args:
            node_dict   : dictionary with nodes from discover_all_nodes()
            f           : a dictionary with criteria to filter nodes from
                          node_dict.

      Returns: a new dictionary with only nodes matching the filter.
    """

    if len(f) > 0:
        matching_nodes = {}

        # build the template string
        tk = sorted(f.keys())
        template = make_string(f, tk)

        # compare the template with all candidates
        for n in node_dict:
            if make_string(node_dict[n], tk) == template:
                matching_nodes[n] = node_dict[n]
    else:
        matching_nodes = node_dict

    return matching_nodes


def make_string(d, key_list):
    """ Make a string from dictionary values using keys given as a list. """
    return ';'.join([str(d[k]) for k in key_list])


def midas_send(socket, message_type, message, address=None):
    """ Temporary messasing functions for debuggings. """
    if address:
        socket.send(address, zmq.SNDMORE)
        socket.send(b"", zmq.SNDMORE)
        socket.send_string(message)
    else:
        socket.send_string(message_type, zmq.SNDMORE)
        socket.send_string(message)


def midas_recv(socket):
    address = socket.recv()
    socket.recv()  # Empty sequence
    msg_type = socket.recv_string()
    message = socket.recv_string()
    return address, msg_type, message


def get_ip():
    """ Return the current IP address."""

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.connect(('<broadcast>', 0))
        ip = s.getsockname()[0]
    except OSError:
        ip = '127.0.0.1'

    return(ip)


def get_config_options(otype):
    """ Return list of valid configuration options for nodes and dispatcher."""
    if otype is 'node':
        return ['node_name', 'node_type', 'node_id', 'node_description', 'primary_node', 'ip', 'port_frontend', 'port_backend', 'port_publisher', 'n_responders', 'lsl_stream_name', 'primary_n_channels', 'primary_channel_names', 'primary_channel_descriptions', 'primary_sampling_rate', 'primary_buffer_size_s', 'run_publisher', 'secondary_node', 'secondary_n_channels', 'secondary_buffer_size', 'secondary_channel_names', 'secondary_channel_descriptions', 'default_channel']
    elif otype is 'dispatcher':
        return ['node_list', 'port', 'ip', 'n_threads', 'run_pubsub_proxy', 'proxy_port_in', 'proxy_port_out']
    else:
        return None

    
def midas_parse_config(nodeclass, *args):
    """ Parse configuration for a node, and if valid return a node"""
    # Read configuration from an INI file given as a command-line argument
    args = args[0]

    if len(args) < 2:
        print('Error! No INI file provided.')
        return None
    else:
        if os.path.isfile(args[1]):
            cfg = configparser.ConfigParser()
            cfg.read(args[1])
        else:
            print('Error! INI file does not exist.')
            return None

    if len(args) == 2:
        if len(cfg.sections()) > 1:
            print('Error! Multiple sections in the INI file.'
                  'Provide section name.')
            return None
        else:
            tmp = dict(cfg.items(cfg.sections()[0]))

    if len(args) == 3:
        if cfg.has_section(args[2]):
            tmp = dict(cfg.items(args[2]))
        else:
            print('Error! Section not found in INI file.')
            return None

    # Determine the object type (node or dispatcher)
    clist = [i.__name__ for i in nodeclass.__mro__]
    otype = None

    if 'BaseNode' in clist:
        otype = 'node'
    if 'Dispatcher' in clist:
        otype = 'dispatcher'
    if otype is None:
        print('Error! Unrecognised MIDAS object type.')
        return None
    
    # Check supported options for the given object type
    unrecognised_options = set(list(tmp.keys())) - set(get_config_options(otype))
    if (len(unrecognised_options) > 0):
        print('Error! Unrecognised configuration options provided. Please fix!')
        print('The following options are not recognised:')
        for i in unrecognised_options:
            print('\t' + i)
        return None

    # Create the node
    return nodeclass(tmp)


def parse_config_to_dict(cfg_file, section):
    """ Reads config file and returns a dict of parameters.

    Args:
        cfg_file: <String> path to the configuration ini-file
        section: <String> section of the configuration file to read
    Returns:
        cfg: <dict> configuration parameters of 'section' as a dict
    """
    cfg = configparser.ConfigParser()
    cfg.read(cfg_file)

    if cfg.has_section(section):
        return dict(cfg.items(section))
    else:
        print("Section '%s' not found in file %s!" % (section, cfg_file))
        return None


def python_version():
    """ Return the major Python version (2 or 3) """

    return(float(sys.version[0]))


def make_url(ip, port, protocol='tcp'):
    """ Return a URL """

    return '{}://{}:{}'.format(protocol, ip, port)


def str2bool(x):
    """ Convert a string to a boolean. """

    return x.lower() in ("true", "1")


def listify(config, key, sep=','):
    """ Create a list from a string containing list elements separated by
        sep.
    """

    return [i.strip() for i in config[key].split(sep)]


def find_range(array, win):
    """ Find indices corresponding to win[0] and win[1] inside array.

    Args:
        array: <list> an array of values sorted in descending order
        win: <tuple> window ranges
    Returns:
        i0: <int> index of the first window limit
        i1: <int> index of the second window limit
    """

    a = array[:]

    i0 = None
    i1 = None

    for idx, val in enumerate(a):
        if i0 is None and win[0] >= val:
            i0 = idx
        if i1 is None and win[1] >= val:
            i1 = idx

    return i0, i1


def LRU_queue_broker(url_frontend, url_backend, NBR_WORKERS, run_state):
    """ Least-recently used queue broker.

    Args:
        url_backend: the router url used for backend (workers)
        url_frontend: the router url used for frontend (clients)
        NBR_workers: the number of workers (worker processes / threads)
        run_state: <integer> boolean "poison pill" to signal termination to the
                             process

    This function is modified from http://zguide.zeromq.org/py:lruqueue
    originally written by Guillaume Aubert (gaubert)
    <guillaume(dot)aubert(at)gmail(dot)com>.

    Original code licensed under the MIT/X11.
    http://zguide.zeromq.org/page:all#Getting-the-Examples
    """
    # Logic of LRU loop
    #
    # - Poll backend always, frontend only if 1+ worker ready
    # - If worker replies, queue worker as ready and forward reply
    #   to client if necessary
    # - If client requests, pop next worker and send request to it

    # Prepare our context and sockets
    context = zmq.Context()

    frontend = context.socket(zmq.ROUTER)
    frontend.bind(url_frontend)

    backend = context.socket(zmq.ROUTER)
    backend.bind(url_backend)

    # Queue of available workers
    available_workers = 0
    workers_list = []

    # init poller
    poller = zmq.Poller()

    # Always poll for worker activity on backend
    poller.register(backend, zmq.POLLIN)

    # Poll front-end only if we have available workers
    poller.register(frontend, zmq.POLLIN)

    while run_state.value:
        socks = dict(poller.poll(5000))

        # Handle worker activity on backend
        if (backend in socks and socks[backend] == zmq.POLLIN):

            # Queue worker address for LRU routing
            worker_addr = backend.recv(zmq.NOBLOCK)

            assert available_workers < NBR_WORKERS

            # add worker back to the list of workers
            available_workers += 1
            workers_list.append(worker_addr)

            # Second frame is empty
            empty = backend.recv(zmq.NOBLOCK)
            assert empty == b""

            # Third frame is READY or else a client reply address
            client_addr = backend.recv(zmq.NOBLOCK)

            # If client reply, send rest back to frontend
            if client_addr != b"READY":

                # Following frame is empty
                empty = backend.recv(zmq.NOBLOCK)
                assert empty == b""

                # reply = backend.recv()
                more = True
                reply = []
                while more:
                    reply.append(backend.recv_string(zmq.NOBLOCK))
                    more = backend.getsockopt(zmq.RCVMORE)

                frontend.send(client_addr, zmq.SNDMORE)
                frontend.send(b"", zmq.SNDMORE)

                for r in reply[:-1]:
                    frontend.send_string(r, zmq.SNDMORE)
                frontend.send_string(str(reply[-1]))

        # poll on frontend only if workers are available
        if available_workers > 0:

            if (frontend in socks and socks[frontend] == zmq.POLLIN):
                # Now get next client request, route to LRU worker
                # Client request is [address][empty][request]

                client_addr = frontend.recv(zmq.NOBLOCK)

                empty = frontend.recv(zmq.NOBLOCK)
                assert empty == b""

                more = True
                request = []
                while more:
                    request.append(frontend.recv_string(zmq.NOBLOCK))
                    more = frontend.getsockopt(zmq.RCVMORE)

                # Dequeue and drop the next worker address
                available_workers -= 1
                worker_id = workers_list.pop()

                backend.send(worker_id, zmq.SNDMORE, zmq.NOBLOCK)
                backend.send(b"", zmq.SNDMORE, zmq.NOBLOCK)
                backend.send(client_addr, zmq.SNDMORE, zmq.NOBLOCK)
                backend.send(b"", zmq.SNDMORE, zmq.NOBLOCK)

                for r in request[:-1]:
                    backend.send_string(r, zmq.SNDMORE, zmq.NOBLOCK)
                backend.send_string(str(request[-1]), zmq.NOBLOCK)

    # Clean up when exiting
    frontend.close()
    backend.close()
    context.term()
