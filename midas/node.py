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
import json
import inspect
import multiprocessing as mp
from . import utilities as mu
import pylsl as lsl


class BaseNode(object):

    """ Simple MIDAS base node class. """

    def __init__(self,
                 config=None,
                 nodename="basenode",
                 nodetype='',
                 nodeid="00",
                 nodedesc="base node",
                 primary_node=True,
                 ip=None,
                 port_frontend=5001,
                 port_backend=5002,
                 port_publisher='',
                 n_workers=5,
                 lsl_stream_name=None,
                 n_channels=None,
                 channel_names=[],
                 channel_descriptions=None,
                 sampling_rate=None,
                 buffer_size_s=30,
                 run_publisher=False,
                 secondary_data=False,
                 n_channels_secondary=0,
                 buffer_size_secondary=0,
                 channel_names_secondary=[],
                 channel_descriptions_secondary=None,
                 default_channel=''):
        """ Initializes a basic MIDAS node class. Arguments can be passed either
            as config dict or specified spearately. If argumets are passed via
            both methods the ini-file will overwrite manually specified
            arguments.
        """

        # Parse information from a dictionary (from an ini-file), if provided
        if config:
            # Settings for general node properties
            if 'nodename' in config:
                nodename = config['nodename']

            if 'nodetype' in config:
                nodetype = config['nodetype']

            if 'nodeid' in config:
                nodeid = config['nodeid']

            if 'nodedesc' in config:
                nodedesc = config['nodedesc']

            if 'ip' in config:
                ip = config['ip'].lower().strip()

            if 'primary_node' in config:
                primary_node = mu.str2bool(config['primary_node'])

            if 'port_frontend' in config:
                port_frontend = int(config['port_frontend'])

            if 'port_backend' in config:
                port_backend = int(config['port_backend'])

            if 'port_publisher' in config:
                port_publisher = int(config['port_publisher'])

            if 'run_publisher' in config:
                run_publisher = mu.str2bool(config['run_publisher'])

            if 'n_workers' in config:
                n_workers = int(config['n_workers'])

            # Settings for data stream properties
            if 'lsl_stream_name' in config:
                lsl_stream_name = config['lsl_stream_name']

            if 'n_channels' in config:
                n_channels = int(config['n_channels'])

            if 'channel_names' in config:
                channel_names = mu.listify(config, 'channel_names')

            if 'channel_descriptions' in config:
                channel_descriptions = mu.listify(
                    config,
                    'channel_descriptions')

            if 'sampling_rate' in config:
                sampling_rate = int(config['sampling_rate'])

            if 'buffer_size_s' in config:
                buffer_size_s = float(config['buffer_size_s'])

            # Settings for secondary channels
            if 'secondary_data' in config:
                secondary_data = config['secondary_data']

            if 'default_channel' in config:
                default_channel = config['default_channel']

            if 'n_channels_secondary' in config:
                n_channels_secondary = int(config['n_channels_secondary'])

            if 'buffer_size_secondary' in config:
                buffer_size_secondary = int(config['buffer_size_secondary'])

            if 'channel_names_secondary' in config:
                channel_names_secondary = mu.listify(
                    config,
                    'channel_names_secondary')

            if 'channel_descriptions_secondary' in config:
                channel_descriptions_secondary = mu.listify(
                    config,
                    'channel_descriptions_secondary')

        # general node properties
        self.nodename = nodename
        self.nodetype = nodetype
        self.nodeid = nodeid
        self.nodedesc = nodedesc
        self.primary_node = primary_node
        self.port_frontend = port_frontend
        self.port_backend = port_backend
        self.port_publisher = port_publisher
        self.run_publisher = run_publisher
        self.n_workers = n_workers

        # Automatically determine the IP of the node unless set in the node
        # configuration
        if (ip is None) or (ip == 'auto'):
            ip = mu.get_ip()
        elif ip is 'localhost':
            ip = '127.0.0.1'
        self.ip = ip

        self.url_frontend = mu.make_url(self.ip, self.port_frontend)
        self.url_backend = mu.make_url('127.0.0.1', self.port_backend)

        # publisher settings
        self.topic_list = {}

        if self.run_publisher:
            self.url_publisher = "tcp://" + \
                str(self.ip) + ":" + str(self.port_publisher)
            self.message_queue = mp.Queue(10)
        else:
            self.url_publisher = ''

        # primary channels and data stream properties
        if self.primary_node:
            self.lsl_stream_name = lsl_stream_name
            self.n_channels = n_channels
            if channel_names:
                self.channel_names = channel_names
            else:
                self.channel_names = [str(c) for c in range(self.n_channels)]
            self.channel_descriptions = channel_descriptions
            self.sampling_rate = sampling_rate
            self.buffer_size_s = buffer_size_s
            if self.sampling_rate > 0:
                self.buffer_size = int(self.buffer_size_s * self.sampling_rate)
            else:
                self.buffer_size = int(self.buffer_size_s)

            if self.channel_descriptions is None:
                self.channel_descriptions = [''] * self.n_channels

        else:
            self.lsl_stream_name = ['']
            self.n_channels = 0
            self.channel_names = ['']
            self.channel_descriptions = ['']
            self.sampling_rate = 0
            self.buffer_size_s = 0
            self.buffer_size = 0

        # secondary channels
        self.secondary_data = secondary_data
        self.default_channel = default_channel
        self.n_channels_secondary = n_channels_secondary
        self.buffer_size_secondary = [
            buffer_size_secondary] * self.n_channels_secondary
        self.channel_names_secondary = channel_names_secondary
        self.channel_descriptions_secondary = channel_descriptions_secondary

        if (self.n_channels_secondary > 0) & (self.channel_names_secondary is None):
            self.channel_names_secondary = [
                'ch_s_' +
                str(i) for i in range(
                    self.n_channels_secondary)]

        if self.channel_descriptions is None:
            self.channel_descriptions = [''] * self.n_channels

        if self.channel_descriptions_secondary is None:
            self.channel_descriptions_secondary = [
                ''] * self.n_channels_secondary

        # ------------------------------
        # State variables:
        #    run_state      : poison pill to control processes
        #    wptr   : the current index being written to in the
        #                     circular buffer (channel_data)
        #    buffer_full    : has the circular buffer been full or not
        #    lock_primary   : lock for channel_data (primary data)
        #    lock_secondary : lock for the secondary channel_data
        # ------------------------------
        self.run_state = mp.Value('i', 0)

        self.wptr = mp.Value('i', 0)
        self.buffer_full = mp.Value('i', 0)

        self.lock_primary = mp.Lock()
        self.lock_secondary = []
        for i in range(self.n_channels_secondary):
            self.lock_secondary.append(mp.Lock())

        # ------------------------------
        # Data containers
        # ------------------------------
        # Preallocate primary buffers
        if self.primary_node:
            self.channel_data = [0] * self.n_channels

            for i in range(self.n_channels):
                self.channel_data[i] = mp.Array('d', [0] * self.buffer_size)

            self.time_array = mp.Array('d', [0] * self.buffer_size)
            self.last_time = mp.Array('d', [0])
        else:
            self.channel_data = []
            self.time_array = []
            self.last_time = []

        # Preallocate secondary buffers
        if self.secondary_data:
            self.channel_data_secondary = [0] * self.n_channels_secondary
            self.time_array_secondary = [0] * self.n_channels_secondary
            self.last_time_secondary = mp.Array(
                'd',
                [0] *
                self.n_channels_secondary)

            for i in range(self.n_channels_secondary):
                self.channel_data_secondary[i] = mp.Array(
                    'd',
                    [0] *
                    self.buffer_size_secondary[i])
                self.time_array_secondary[i] = mp.Array(
                    'd',
                    [0] *
                    self.buffer_size_secondary[i])

            self.wptr_secondary = mp.Array('i', [0] * self.n_channels_secondary)
            self.buffer_full_secondary = mp.Array(
                'i',
                [0] *
                self.n_channels_secondary)

        # ------------------------------
        # Empty containers for functions
        # ------------------------------
        self.metric_names = []
        self.metric_descriptions = []
        self.metric_pointers = []

        # ------------------------------
        # Empty container for processes
        # ------------------------------
        self.process_list = []

        # ------------------------------
        # Empty container for metric functions
        # ------------------------------
        self.metric_functions = []

    # -------------------------------------------------------------------------
    # Receiver (receives data and stores the data in a circular buffer)
    # -------------------------------------------------------------------------
    def receiver(self):
        """ Receive data from an LSL stream and store it in a circular
            buffer.
        """

        streams = []

        while not streams:
            print("Trying to connect to the stream: " + self.lsl_stream_name)
            streams = lsl.resolve_byprop(
                'name',
                self.lsl_stream_name,
                timeout=10)
            if not streams:
                print("\tStream not found, re-trying...")

        inlet = lsl.StreamInlet(streams[0], max_buflen=1)
        print("\tDone")

        i = 0
        self.last_time.value = 0  # init the last_time value
        while self.run_state.value:
            x, t = inlet.pull_sample()

            self.lock_primary.acquire()  # LOCK-ON

            for k in range(self.n_channels):
                self.channel_data[k][self.wptr.value] = x[k]

            if t is None:
                t = self.last_time.value + self.sampling_rate

            self.time_array[self.wptr.value] = t
            self.last_time.value = t

            i += 1
            self.wptr.value = i % self.buffer_size
            self.lock_primary.release()  # LOCK-OFF

            # is the buffer full
            if (0 == self.buffer_full.value) and (i >= self.buffer_size):
                self.buffer_full.value = 1

    # -------------------------------------------------------------------------
    # Publish messages that are placed in the message queue
    # -------------------------------------------------------------------------
    def publisher(self):
        """ Publish data using ZeroMQ.

            A message to be published is placed in the node's message queue
            (self.message_queue), from which this functions gets() the next
            message and publishes it using the node's publisher.
        """

        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.connect(self.url_publisher)

        while self.run_state.value:
            if not self.message_queue.empty():
                socket.send_string(
                    "%s;%s" %
                    (self.nodename, self.message_queue.get()))
            time.sleep(0.0001)

    # -------------------------------------------------------------------------
    # Respond to queries over ZeroMQ
    # -------------------------------------------------------------------------
    def responder(self, responder_id):
        """ Respond to queries over ZeroMQ.

            The responder listens to messages over ZeroMQ and handles messages
            following the MIDAS Messaging Protocol. The messages can be queries
            of metrics, data, or commands regarding, e.g., the state of the
            node.
        """

        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self.url_backend)
        socket.send(b"READY")

        print('Started new responder.\tID: ' + str(responder_id))

        while self.run_state.value:
            try:
                address, req_type, request = mu.midas_recv(socket)
                recv_time = time.time()

                if req_type == 'metric':
                    return_value = self.handle_metric(request)

                elif req_type == 'data':
                    return_value = self.handle_data(request)

                elif req_type == 'command':
                    return_value = self.handle_command(request)

                elif req_type == 'ping':
                    return_value = str(time.time() - recv_time)

                else:
                    return_value = {"error": "not recognized"}

                mu.midas_send(socket, 'reply', return_value, address)

            except zmq.ContextTerminated:
                return
    # -------------------------------------------------------------------------

    def unwrap_channel(self, channel_name):
        """ Gives the unwrapping vector for the specified channel

        Args:
            channel_name <string>: name of the channel
        Returns:
            idx <list>: unwrapping vector
        """
        if channel_name in self.channel_names:
            if self.buffer_full.value:
                idx = [0] * self.buffer_size
                for i in range(self.buffer_size):
                    idx[i] = (self.wptr.value + i) % self.buffer_size
            else:
                idx = range(self.wptr.value)

        elif channel_name in self.channel_names_secondary:
            ch_idx = self.channel_names_secondary.index(channel_name)
            if self.buffer_full_secondary[ch_idx]:
                idx = [0] * self.buffer_size_secondary[ch_idx]
                for i in range(self.buffer_size_secondary[ch_idx]):
                    idx[i] = ((self.wptr_secondary[ch_idx] + i) %
                              self.buffer_size_secondary[ch_idx])
            else:
                idx = range(self.wptr_secondary[ch_idx])

        return idx

    def push_sample_secondary(self, ch, timep, value, use_lock=True):
        """ Push a new sample into a secondary data buffer.

        Args:
               ch: <int>    secondary data channel index
            timep: <float>  time stamp of new sample
            value: <float>  value of new sample
        """
        if use_lock:
            self.lock_secondary[ch].acquire()

        self.channel_data_secondary[ch][self.wptr_secondary[ch]] = value
        self.time_array_secondary[ch][self.wptr_secondary[ch]] = timep
        self.wptr_secondary[ch] += 1

        if ((0 == self.buffer_full_secondary[ch]) and
                (self.wptr_secondary[ch] >= self.buffer_size_secondary[ch])):
            self.buffer_full_secondary[ch] = 1

        self.wptr_secondary[ch] = (self.wptr_secondary[ch] %
                                   self.buffer_size_secondary[ch])
        if use_lock:
            self.lock_secondary[ch].release()

    def push_chunk_secondary(self, ch, timeps, values):
        """ Push a chunk of new samples into a secondary data buffer.

        Args:
                ch: <int>   secondary data channel index
            timeps: <list>  list of time stamps for new values
            values: <list>  list of new values
        """

        self.lock_secondary[ch].acquire()
        for t, v in zip(timeps, values):
            self.push_sample_secondary(ch, t, v, use_lock=False)
        self.lock_secondary[ch].release()

    def is_valid_request(self, request):
        """ Asserts that the given request is valid.

        Args:
           request: dict containing the unpacked JSON request

        Returns:
            True if request is valid, otherwise False
        """
        # Start by assuming the request is valid
        metric_ok = True
        arguments_ok = True
        channels_ok = True
        time_ok = True

        if 'type' in request:
            metric_ok = request['type'] in self.metric_names

        if 'arguments' in request:
            try:
                n = inspect.ismethod(self.metric_pointers[request['type']]) + 1
                fun = inspect.getargspec(self.metric_pointers[request['type']])
                arguments_ok = len(request['arguments']) <= len(fun.args) - n
            except:
                arguments_ok = False

        if 'channels' in request:
            try:
                channels_ok = set(
                    self.channel_names +
                    self.channel_names_secondary).issuperset(
                    request['channels'])
            except:
                channels_ok = False

        if 'time_window' in request:
            try:
                time_ok = all(
                    [isinstance
                     (t, (float, int))
                     for t in request['time_window']])
            except:
                time_ok = False

        return metric_ok and arguments_ok and channels_ok and time_ok

    def get_channel_list(self, requests):
        """ Returns an intersection of requested channels and existing channels.

        Args:
            requests <list>: list of requests
        Returns:
            channels <list>: list of requested channels
        """
        channels = []
        for request in requests:
            if 'channels' in request:
                channels.extend(request['channels'])
        return list(set.intersection(set(channels),
                                     set(self.channel_names +
                                         self.channel_names_secondary)))

    def get_data_from_channel(self, channel_name):
        """ Copy and unwrap data from specified channel

        Args:
            channel_name <string>: name of the channel
        Returns:
            data <list> array of samples
            times <list> array of timestamps
        """
        if channel_name in self.channel_names:
            time_array = self.time_array[:]
            data = self.channel_data[self.channel_names.index(channel_name)][:]

        elif channel_name in self.channel_names_secondary:
            idx = self.channel_names_secondary.index(channel_name)
            time_array = self.time_array_secondary[idx][:]
            data = self.channel_data_secondary[idx][:]

        unwrap_idx = self.unwrap_channel(channel_name)

        time_array = [time_array[i] for i in unwrap_idx]
        time_array = [abs(i - time_array[-1]) for i in time_array]
        data = [data[i] for i in unwrap_idx]

        return data, time_array

    def lock_all_secondary(self):
        """ Locks all channels of the secondary buffer. """
        [lock.acquire() for lock in self.lock_secondary]

    def release_all_secondary(self):
        """ Releases all channels of the secondary buffer. """
        [lock.release() for lock in self.lock_secondary]

    def snapshot_data(self, channels):
        """ Copies specified data channels.

        Args:
            channels <list>: list of channels
        Returns:
            snapshot <dict>: data and times for each channel
        """
        self.lock_primary.acquire()
        self.lock_all_secondary()
        snapshot = {}
        for channel in channels:
            data, time = self.get_data_from_channel(channel)
            snapshot[channel] = (data, time)
        self.lock_primary.release()
        self.release_all_secondary()
        return snapshot

    def unpack_snapshot(self, snapshot, channels, time_window):
        """ Extracts speciefied channels and time-windows from a snapshot

        Args:
            snapshot <dict> a snapshot of data
            channels <list> list of channels
            time_window <list> two-element list specifying the time-window
        """
        times = []
        data = []
        if time_window:
            if len(time_window) == 1:
                time_window = [time_window[0], time_window[0]]
            time_window[1] = time_window[0] - time_window[1]
        for channel in map(snapshot.get, channels):
            if time_window:
                start, stop = mu.find_range(channel[1], time_window)
                data.append(channel[0][start:stop])
                times.append(channel[1][start:stop])
            else:
                data.append(channel[0])
                times.append(channel[1])
        return data, times

    def handle_metric(self, requests):
        """ Function for processing incoming metric requests

        Args:
            requests: JSON-formatted request or a list of multiple metric
                      requests

        Returns:
            JSON-formatted result string
        """
        try:
            requests = json.loads(requests)
        except ValueError:
            return json.dumps({'Error': "Can't unpack request(s)!"})

        # Wrap singular request into a list
        if isinstance(requests, dict):
            requests = [requests]

        channels = self.get_channel_list(requests)
        snapshot = self.snapshot_data(channels)

        results = []
        for request in requests:
            if 'type' in request and self.is_valid_request(request):

                if 'time_window' in request:
                    time_window = request['time_window']
                else:
                    time_window = None

                if 'channels' in request:
                    data, times = self.unpack_snapshot(snapshot,
                                                       request['channels'],
                                                       time_window)
                else:
                    data = []
                    times = []

                if 'arguments' in request:
                    arguments = request['arguments']
                else:
                    arguments = []

                data = {'data': data, 'time': times}
                result = self.metric_pointers[request['type']](data, *arguments)
                request['return'] = result
            else:
                request['return'] = "Malformed request!"

            results.append(request)

        return json.dumps(results)

    def handle_data(self, requests):
        """ Processes incoming data request

        Args:
            requests: JSON-formatted request or a list of multiple data requests
        Returns:
            JSON-formatted result string
        """
        try:
            requests = json.loads(requests)
        except ValueError:
            return json.dumps({'Error': "Can't unpack request(s)!"})

        if isinstance(requests, dict):
            requests = [requests]

        channels = self.get_channel_list(requests)
        snapshot = self.snapshot_data(channels)

        results = []
        for request in requests:
            if self.is_valid_request(request):
                if 'channels' in request:
                    channels = request['channels']

                if 'time_window' in request:
                    time_window = request['time_window']
                else:
                    time_window = None

                data, times = self.unpack_snapshot(snapshot, channels,
                                                   time_window)
                this_data = {}
                for idx, ch in enumerate(channels):
                    this_data[ch] = {'data': data[idx], 'time': times[idx]}
                request['return'] = this_data
            else:
                request['return'] = "Malformed request!"
            results.append(request)

        return json.dumps(results)

    def handle_command(self, command):
        """ Handling function for commands

            Args:
                command: a command (currently some very bugged format)
            Returns:
                return_value: return value of the command
        """

        if command == "get_metric_list":
            return_value = self.get_metric_list()
        elif command == "get_nodeinfo":
            return_value = self.get_nodeinfo()
        elif command == "get_publisher":
            return_value = self.get_publisher_url()
        elif command == "get_data_list":
            return_value = self.get_data_list()
        elif command == "get_topic_list":
            return_value = self.get_topic_list()
        else:
            return_value = "unknown command"

        return json.dumps(return_value)

    # -------------------------------------------------------------------------
    # Start the node
    # -------------------------------------------------------------------------
    def start(self):
        """ Start the node. """
        self.run_state.value = 1

        # Add user-defined metrics to the metric list
        self.generate_metric_lists()

        # Create and configure beacon
        self.beacon = mu.Beacon(
            name=self.nodename,
            type=self.nodetype,
            id=self.nodeid,
            interval=2)
        self.beacon.ip = self.ip
        self.beacon.port = self.port_frontend

        # Start the load-balancing broker
        self.proc_broker = mp.Process(
            target=mu.LRU_queue_broker,
            args=(self.url_frontend,
                  self.url_backend,
                  self.n_workers,
                  self.run_state))
        self.proc_broker.start()

        # Start the publisher if it is configured
        if self.run_publisher:
            self.proc_publisher = mp.Process(target=self.publisher)
            self.proc_publisher.start()

        # If the node is a primary node, start the receiver
        if self.primary_node:
            self.proc_receiver = mp.Process(target=self.receiver)
            self.proc_receiver.start()

        # Start responders
        self.proc_responder_list = [0] * self.n_workers

        for i in range(self.n_workers):
            self.proc_responder_list[i] = mp.Process(target=self.responder,
                                                     args=(i,))
            self.proc_responder_list[i].start()

        # Start user-defined processes, if there are any
        self.proc_user_list = [0] * len(self.process_list)

        for i, fn in enumerate(self.process_list):
            self.proc_user_list[i] = mp.Process(target=fn)
            self.proc_user_list[i].start()

        # Set the beacon online
        self.beacon.set_status('online')
        self.beacon.start()

        time.sleep(5)
        print("Node '%s' now online." % self.nodename)

    # -------------------------------------------------------------------------
    # Stop the node
    # -------------------------------------------------------------------------
    def stop(self):
        """ Terminates the node. """
        if self.run_state.value:

            print("Node '%s' shutting down ..." % self.nodename)

            self.beacon.set_status('offline')
            self.run_state.value = 0

            # Terminate responders
            for i in self.proc_responder_list:
                i.terminate()

            # Terminate user-defined processes, if there are any
            for i in self.proc_user_list:
                i.terminate()

            # Terminate broker
            self.proc_broker.join()

            # Stop receiver if it is running
            if self.primary_node:
                self.proc_receiver.join()

            # Stop the publisher if it is running
            if self.run_publisher:
                self.proc_publisher.join()

            # Stop the beacon
            self.beacon.stop()

        else:
            print("Node '%s' is not running." % self.nodename)

        print("Node '%s' is now offline." % self.nodename)

    # -------------------------------------------------------------------------
    # Minimalist user interface for the node
    # -------------------------------------------------------------------------
    def show_ui(self):
        """ Show a minimal user interface. """
        while True:
            tmp = input(" > ")
            if tmp == "q":
                self.stop()
                sys.exit(0)

    # -------------------------------------------------------------------------
    # Generate metric list
    # -------------------------------------------------------------------------
    def generate_metric_lists(self):
        """ Generate metric lists for the node.

            metric_functions    : pointers to functions used to calculate
                                  metrics (array)
            metric_names        : the names of the metrics (array)
            metric_descriptions : dict with function names as key and
                                  description as value
            metric_pointers     : dict with function names as key and function
                                  pointer as value
        """
        self.metric_names = [m.__name__ for m in self.metric_functions]
        self.metric_descriptions = dict(
            zip(self.metric_names,
                [m.__doc__.split('\n')[0].strip()
                 for m in self.metric_functions]))
        self.metric_pointers = dict(
            zip(self.metric_names, self.metric_functions))

    def generate_nodeinfo(self):
        """ Stores all node information in a dict """
        self.nodeinfo = {}
        self.nodeinfo['name'] = self.nodename
        self.nodeinfo['desc'] = self.nodedesc
        self.nodeinfo['primary_node'] = self.primary_node
        self.nodeinfo['channel_count'] = self.n_channels
        self.nodeinfo['channel_names'] = ",".join(self.channel_names)
        self.nodeinfo['channel_descriptions'] = ",".join(
            self.channel_descriptions)
        self.nodeinfo['sampling_rate'] = self.sampling_rate
        self.nodeinfo['buffer_size'] = self.buffer_size_s
        self.nodeinfo['buffer_full'] = self.buffer_full.value

    # -------------------------------------------------------------------------
    # Return descriptions of the metrics
    # -------------------------------------------------------------------------
    def get_metric_list(self):
        """ Returns the metrics list of the node as a dictionary where the name
            of the metric is the key and the description is the value.
        """
        return self.metric_descriptions
    # -------------------------------------------------------------------------

    def get_topic_list(self):
        """ Return topics that are published by the node. """
        return self.topic_list

    # -------------------------------------------------------------------------

    def get_nodeinfo(self):
        """ Return information about the node. """
        self.generate_nodeinfo()
        return self.nodeinfo
    # -------------------------------------------------------------------------

    def get_publisher_url(self):
        """ Return the URL of the publisher socket in the node. """
        return self.url_publisher
    # -------------------------------------------------------------------------

    def get_data_list(self):
        """ Returns the data list of the node as a dictionary where the name of
            the data is the key and the description is the value.
        """
        cn = self.channel_names + self.channel_names_secondary
        cd = self.channel_descriptions + self.channel_descriptions_secondary
        return dict(zip(cn, cd))
    # -------------------------------------------------------------------------
