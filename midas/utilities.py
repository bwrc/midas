#!/usr/bin/env python3

import zmq
import time
import ipaddress
import socket
import configparser
import os.path
import sys
import json
from pyre import zbeacon, zhelper
from multiprocessing import Process, Manager, Value, Array, RawArray, Lock

from . import pylsl_python3 as lsl

# -------------------------------------------------------------------------------
# Beacons - used for service discovery
# -------------------------------------------------------------------------------

class Beacon(object):
    """ A zbeacon with some functions allowing easy use. """

    def __init__(self, 
                 service = '', 
                 port = '', 
                 id = '', 
                 status_online = '', 
                 status_description = '', 
                 ip = None, description = '', 
                 protocol = 'tcp', 
                 port_broadcast = 5670):
        """ Create the beacon and set some properties, but do not start it. """

        self.service = service
        self.id = id
        self.status_online = status_online
        self.status_description = status_description
        self.port = port
        self.description = description
        self.protocol = protocol
        self.ip = ip
        self.ctx = zmq.Context()
        self.zb = zbeacon.ZBeacon(self.ctx, port_broadcast)
        self.zb.set_noecho()
        self.is_visible = False

    def set_status(self, status_online, status_description = ''):
        """ Set the status of the node.
            If the node is already broadcasting, change the message in the broadcast. 
        """
        self.status_online = status_online
        self.status_description = status_description

        if (self.is_visible):
            self.publish()

    def publish(self):
        """ Start publishing data on the beacon, i.e., make it visible. """
        if self.ip is None:
            self.ip = self.zb.get_hostname()

        url_node = str(self.protocol) + '://' + str(self.ip) + ':' + str(self.port)

        data = ';'.join(['midas', str(self.service), str(self.id), 
                            str(self.status_online), 
                            str(self.status_description), 
                            url_node, str(self.description)])
        data = str.encode(data)
        self.is_visible = True
        self.zb.publish(data)

    def subscribe(self, topic = ''):
        """ Subscribe to a topic. If the length of the topic is empty (default),
            the beacon receives all messages.
        """
        self.zb.subscribe(str.encode(topic))

    # --- service

    def set_service(self, service):
        self.service = service

    def get_service(self):
        return self.service

    # --- port

    def set_port(self, port):
        self.port = port

    def get_port(self):
        return self.port

    # --- ip
    def set_ip(self, ip):
        self.ip = ip

    def get_ip(self):
        return self.ip

    # --- id

    def set_id(self, id):
        self.id = id

    def get_id(self):
        return self.id

    # --- description

    def set_description(self, description):
        self.description = description

    def get_description(self):
        return self.description


    def get_hostname(self):
        return self.zb.get_hostname()

    def validate_message(self, message):
        """ Validate a received message to make sure that it
            is a valid message in the MIDAS framework and return
            a dictionary containing the information sent by the beacon.
        """
        ip = message[0]
        message = message[1].split(';')
        if message[0] == 'midas':
            k = ['service', 'id', 'status_online', 'status_description', 
                    'address', 'description']
            return dict(zip(k, message[1:]))
        else:
            return None

    
    def discover_all_services(self, timeout = 5):
        """ Discover all MIDAS services and return them as a list."""

        print('Discovering all MIDAS services.')
        poller = zmq.Poller()
        pipe = self.zb.get_socket()
        poller.register(pipe, zmq.POLLIN)
        t_start = time.time()
        service_list = {}
        tmp_list = []

        while(time.time() - t_start < timeout):
            items = dict(poller.poll(timeout * 1000))
            if pipe in items and items[pipe] == zmq.POLLIN:
                more = True
                message = []
                while more:
                    message.append(pipe.recv_string(zmq.NOBLOCK))
                    more = pipe.getsockopt(zmq.RCVMORE)
                message = self.validate_message(message)
                if message:
                    if message not in tmp_list:
                        tmp_list.append(message)
                        service_list[message['service']]=message

        return service_list

    

    def discover_service(self, service = None, id = None, timeout = 5):
        """ Discover MIDAS services basd on service name and id. """

        poller = zmq.Poller()
        pipe = self.zb.get_socket()
        poller.register(pipe, zmq.POLLIN)
        t_start = time.time()
        while(time.time() - t_start < timeout):
            items = dict(poller.poll(timeout * 1000))
            if pipe in items and items[pipe] == zmq.POLLIN:
                more = True
                message = []
                while more:
                    message.append(pipe.recv_string(zmq.NOBLOCK))
                    more = pipe.getsockopt(zmq.RCVMORE)
                message = self.validate_message(message)

                # validate the filter criteria
                if message:
                    valid = False
                    if id is not None:
                        if (message['service'] == service) and (message['id'] == id):
                            valid = True
                    else:
                        if message['service'] == service:
                            valid = True

                    if valid:
                        return message
        return(None)


# -------------------------------------------------------------------------------
# Messages
# -------------------------------------------------------------------------------

def midas_send_message(socket, message_type, request):
    """ Send a message using the MIDAS Messaging Protocol.

    Args:
       socket: the ZMQ socket to use
       message_type: a string indicating the message type:
            'metric'
            'data'
            'command'
       request: an array containing the request for the message type:
            'metric': an array, containing the metrics as strings,
                      and the time window as a string as the last item.
            'index':  an array, containing the indices as strings,
                      and the time window as a string as the last item.
            'data':   an array, containing the data type as strings,
                      and the time window as a string as the last item.
            'command': a command and parameters for the receiving MIDAS node
    """

    socket.send_string(message_type, zmq.SNDMORE)

    for m in request[:-1]:
        socket.send_string(m, zmq.SNDMORE)
    socket.send_string(request[-1])

# -------------------------------------------------------------------------------


def midas_receive_message(socket):
    """ Receive a message using the MIDAS Messaging Protocol.

    Args:
       socket: the ZMQ socket to use

    Returns:
       the message as an array
    """

    # receive requests
    more = True
    message = {}
    request = []

    message['address'] = socket.recv()
    empty              = socket.recv()
    message['type']    = socket.recv_string()

    while more:
        request.append(str(socket.recv_string()))
        more = socket.getsockopt(zmq.RCVMORE)

    if message['type'] in ['metric', 'data']:
        if ':' in request[-1]:
            tmp = request[-1].split(':')
            tmp = [float(i) for i in tmp]
            if tmp[1] > tmp[0]:
                tmp[1] = tmp[0]
            timewindow = [tmp[0], tmp[1]]
        else:
            tmp = float(request[-1])
            timewindow = [float(tmp), float(tmp)]

        message['parameters'] = request[:-1]
        message['timewindow'] = timewindow

    elif message['type'] == 'command':
        message['command'] = request

    # print(message)
    return message


# -------------------------------------------------------------------------------
# Replies
# -------------------------------------------------------------------------------
def midas_send_reply(socket, address, data):
    """ Send a list of data arrays using JSON

    Args:
       socket: the ZMQ socket to use
       data: a dictionary with the data vectors
    """

    socket.send(address, zmq.SNDMORE)
    socket.send(b"", zmq.SNDMORE)
    socket.send_string(json.dumps(data, sort_keys = True, indent = 4, separators = (',',':')))

# -------------------------------------------------------------------------------

def midas_receive_reply(socket,deserialize=False):
    result = socket.recv_string()

    if deserialize:
        return json.loads(result)
    else:
        return result

# -------------------------------------------------------------------------------

def parse_metric(x):
    """ Split a semicolon-separated string, split it
        a list.
    Args:
       x: a string with each component separated by semicolons

    Returns:
       a list
    """

    if ';' in x:
        return x.split(';')
    else:
        return [x]

# -------------------------------------------------------------------------------

def get_ip():
    """ Return the current IP address."""

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))

    return s.getsockname()[0]

# -------------------------------------------------------------------------------

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
            print('Error! Multiple sections in the INI file. Provide section name.')
            return None
        else:
            tmp = dict(cfg.items(cfg.sections()[0]))

    if len(args) == 3:
        if cfg.has_section(args[2]):
            tmp = dict(cfg.items(args[2]))
        else:
            print('Error! Section not found in INI file.')
            return None
            
    # Create the node
    return nodeclass(tmp)
    

# -------------------------------------------------------------------------------

class DataState(object):
    """ Thread-safe boolean that can, e.g., be used to keep track of whether
        there is new data or not
    """

    def __init__(self, initial_state = 0):
        self.state = Value('i', initial_state)
        self.lock = Lock()

    def setstate(self, val):
        with self.lock:
            self.state.value = val

    def flipstate(self):
        with self.lock:
            if self.state.value == 0:
                self.state.value = 1
            elif self.state.value == 1:
                self.state.value = 0

    def getstate(self):
        return(self.state.value)

# -------------------------------------------------------------------------------
def python_version():
    """ Return the major Python version (2 or 3) """

    return(float(sys.version[0]))

# -------------------------------------------------------------------------------

def make_url(ip, port, protocol = 'tcp'):
    """ Return a URL """

    return protocol + '://' + ip + ':' + str(port)

# -------------------------------------------------------------------------------

def str2bool(x):
    """ Convert a string to a boolean. """

    return x.lower() in ("true", "1")

# -------------------------------------------------------------------------------

def get_channel_index(channel_list, channel_name):
    """ Return the index of one or more channels in the channel index list. 

    Args:
         channel_list  : a list of all channels (string[])
         channel_names : a string or list of strings with a channel name,
                         the index of which in the channel_list one wants to get
    """

    if isinstance(channel_name, str):
        channel_index = channel_list.index(channel_name)
    elif isinstance(channel_name, list):
        channel_index = []
        for cn in channel_name:
            channel_index.append(channel_list.index(cn))

    return channel_index


# -------------------------------------------------------------------------------

def get_channeL_data(channel_data, channel_list, channel_name):
    """ Return channel data.
        Return the data corresponding to the strings in channel_name, indexed
        according to channel_list.

    Args:
         channel_data  : array of arrays containing the channel data
         channel_list  : a list of all channels (string[])
         channel_name  : a string or list of strings with a channel name,
                         the index of which in the channel_list one wants to get
    """

    channel_index = get_channel_index(channel_list, channel_name)

    if isinstance(channel_index, int):
        data = channel_data[channel_index]
    elif isinstance(channel_index, list):
        data = [0] * len(channel_index)
        for i, index in enumerate(channel_index):
            data[i] = channel_data[index]

    return data
    


# -------------------------------------------------------------------------------

def get_index_vector(N, buffer_full, writepointer):
    """ Create an index vector that can be used to extract data in
        the correct order from a circular buffer.

    Args:
        N            : the size of the buffer
        buffer_full  : is the buffer full, i.e., are elements being overwritten (Boolean)
        writepointer : the index where the next element will be written
    """

    if (0 == buffer_full):
        iv = range(writepointer)
    else:
        iv = [0] * N
        for i in range(N):
            iv[i] = (writepointer + i) % N

    return iv

# -------------------------------------------------------------------------------

def find_range(array,win):
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

    for idx,val in enumerate(a):
        if i0 is None and win[0] >= val:
            i0 = idx
        if i1 is None and win[1] >= val:
            i1 = idx

    return i0,i1


# -------------------------------------------------------------------------------

def resolve_lsl_stream_name_type(stream_name, stream_type):
    """ Resolve an LSL stream by name and signal type.

    Args:
        stream_name: <string> the name of the stream
        stream_type: <string> the signal type of the stream
    """

    print('Resolving stream ...')
    streams = lsl.resolve_streams()
    for s in streams:
        if (s.name() == stream_name) and (s.type() == stream_type):
            print('\tDone')
            return(s)

# -------------------------------------------------------------------------------

def create_lsl_inlet(stream, buffer_length):
    """ Create an LSL stream inlet from an LSL stream object.

    Args:
        stream: an LSL stream object (e.g. from resolve_lsl_stream_name_type)
        buffer_length: the size of the buffering used for the LSL stream

    """
    print('Trying to connect to the stream ...')
    inlet = lsl.StreamInlet(stream, max_buflen = buffer_length)
    print('\tDone')
    return(inlet)

# -------------------------------------------------------------------------------

def LRU_queue_broker(url_frontend, url_backend, NBR_WORKERS, run_state):
    """ Least-recently used queue broker.

    Args:
        url_backend: the router url used for backend (workers)
        url_frontend: the router url used for frontend (clients)
        NBR_workers: the number of workers (worker processes / threads)
        run_state: <integer> boolean "poison pill" to signal termination to the process

    This code is slightly modified from http://zguide.zeromq.org/py:lruqueue originally
    written by Guillaume Aubert (gaubert) <guillaume(dot)aubert(at)gmail(dot)com>.

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

# -------------------------------------------------------------------------------
# Do nothing if we run this module
# -------------------------------------------------------------------------------

def main():
    pass

if __name__ == '__main__':
    main()
    
# -------------------------------------------------------------------------------
