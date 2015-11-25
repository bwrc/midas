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
                 node_name="basenode",
                 node_type='',
                 node_id="00",
                 node_description="base node",
                 primary_node=True,
                 ip=None,
                 port_frontend=5001,
                 port_backend=5002,
                 port_publisher='',
                 n_responders=5,
                 lsl_stream_name=None,
                 primary_n_channels=None,
                 primary_channel_names=[],
                 primary_channel_descriptions=None,
                 primary_sampling_rate=None,
                 primary_buffer_size_s=30,
                 run_publisher=False,
                 secondary_node=False,
                 secondary_n_channels=0,
                 secondary_buffer_size=0,
                 secondary_channel_names=[],
                 secondary_channel_descriptions=None,
                 default_channel=''):
        """ Initializes a basic MIDAS node class. Arguments can be passed either
            as config dict or specified spearately. If argumets are passed via
            both methods the ini-file will overwrite manually specified
            arguments.
        """

        # Parse information from a dictionary (from an ini-file), if provided
        if config:
            # Settings for general node properties
            if 'node_name' in config:
                node_name = config['node_name']

            if 'node_type' in config:
                node_type = config['node_type']

            if 'node_id' in config:
                node_id = config['node_id']

            if 'node_description' in config:
                node_description = config['node_description']

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

            if 'n_responders' in config:
                n_responders = int(config['n_responders'])

            # Settings for data stream properties
            if 'lsl_stream_name' in config:
                lsl_stream_name = config['lsl_stream_name']

            if 'primary_n_channels' in config:
                primary_n_channels = int(config['primary_n_channels'])

            if 'primary_channel_names' in config:
                primary_channel_names = mu.listify(config, 'primary_channel_names')

            if 'primary_channel_descriptions' in config:
                primary_channel_descriptions = mu.listify(config, 'primary_channel_descriptions')

            if 'primary_sampling_rate' in config:
                primary_sampling_rate = int(config['primary_sampling_rate'])

            if 'primary_buffer_size_s' in config:
                primary_buffer_size_s = float(config['primary_buffer_size_s'])

            # Settings for secondary channels
            if 'secondary_node' in config:
                secondary_node = config['secondary_node']

            if 'secondary_n_channels' in config:
                secondary_n_channels = int(config['secondary_n_channels'])

            if 'secondary_buffer_size' in config:
                secondary_buffer_size = int(config['secondary_buffer_size'])

            if 'secondary_channel_names' in config:
                secondary_channel_names = mu.listify(config, 'secondary_channel_names')

            if 'secondary_channel_descriptions' in config:
                secondary_channel_descriptions = mu.listify(config, 'secondary_channel_descriptions')

        # general node properties
        self.node_name = node_name
        self.node_type = node_type
        self.node_id = node_id
        self.node_description = node_description
        self.primary_node = primary_node
        self.secondary_node = secondary_node
        self.port_frontend = port_frontend
        self.port_backend = port_backend
        self.port_publisher = port_publisher
        self.run_publisher = run_publisher
        self.n_responders = n_responders

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
            self.url_publisher = 'tcp://{}:{}'.format(self.ip, self.port_publisher)
            self.message_queue = mp.Queue(10)
        else:
            self.url_publisher = ''

        # primary channels and data stream properties
        if self.primary_node:
            self.initialize_primary(lsl_stream_name,
                                    primary_n_channels,
                                    primary_channel_names,
                                    primary_buffer_size_s,
                                    primary_sampling_rate,
                                    primary_channel_descriptions)
        else:
            self.primary_n_channels = 0
            self.primary_buffer_size = 0
            self.primary_channel_names = []
            self.primary_channel_descriptions = []

        # secondary channels and properties
        if self.secondary_node:
            self.initialize_secondary(secondary_n_channels,
                                      secondary_buffer_size,
                                      secondary_channel_names,
                                      secondary_channel_descriptions)
        else:
            self.secondary_n_channels = 0
            self.secondary_buffer_size = 0
            self.secondary_channel_names = []
            self.secondary_channel_descriptions = []

        # ------------------------------
        # State variables:
        #    run_state      : poison pill to control processes
        #    primary_lock   : lock for channel_data (primary data)
        #    secondary_lock : lock for the secondary channel_data
        # ------------------------------
        self.run_state = mp.Value('i', 0)

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

    def initialize_primary(self, lsl_stream_name, primary_n_channels,
                           primary_channel_names, primary_buffer_size_s,
                           primary_sampling_rate, primary_channel_descriptions):
        """ Initialize primary LSL stream properties and allocate memory for
            storing the data.
        """

        # Initialize stream properties
        self.lsl_stream_name = lsl_stream_name
        self.primary_n_channels = primary_n_channels
        if primary_channel_names:
            self.primary_channel_names = primary_channel_names
        else:
            self.primary_channel_names = [str(c) for c in range(self.primary_n_channels)]

        self.primary_sampling_rate = primary_sampling_rate
        self.primary_buffer_size_s = primary_buffer_size_s
        self.primary_channel_descriptions = primary_channel_descriptions
        if self.primary_sampling_rate > 0:
            self.primary_buffer_size = int(self.primary_buffer_size_s * self.primary_sampling_rate)
        else:
            self.primary_buffer_size = self.primary_buffer_size_s

        if not self.primary_channel_descriptions:
            self.primary_channel_descriptions = [''] * self.primary_n_channels
        else:
            self.primary_channel_descriptions = primary_channel_descriptions

        self.primary_last_sample_received = mp.Value('d', time.time())

        # Preallocate primary buffers
        self.primary_channel_data = [0] * self.primary_n_channels
        for i in range(self.primary_n_channels):
            self.primary_channel_data[i] = mp.Array('d', [0] * self.primary_buffer_size)
        self.primary_time_array = mp.Array('d', [0] * self.primary_buffer_size)
        self.primary_last_time = mp.Array('d', [0])

        self.primary_wptr = mp.Value('i', 0)
        self.primary_buffer_full = mp.Value('i', 0)

        self.primary_lock = mp.Lock()

    def initialize_secondary(self, n_channels, buffer_size,
                             channel_names, channel_descriptions):
        """ Initialize secondary data properties and allocate memory for
            storing the data.
        """

        # Initialize data properties
        self.secondary_n_channels = n_channels
        self.secondary_buffer_size = [buffer_size] * n_channels
        self.secondary_channel_names = channel_names
        self.secondary_channel_descriptions = channel_descriptions

        if self.secondary_n_channels > 0 and not self.secondary_channel_names:
            new_names = []
            for idx in range(self.secondary_n_channels):
                new_names.append('ch_s_{}'.format(idx))
            self.secondary_channel_names = new_names

        if not self.secondary_channel_descriptions:
            self.secondary_channel_descriptions = [''] * self.secondary_n_channels

        # Preallocate secondary buffers
        self.secondary_channel_data = [0] * self.secondary_n_channels
        self.secondary_time_array = [0] * self.secondary_n_channels
        self.last_time_secondary = mp.Array('d', [0] * self.secondary_n_channels)

        for idx, size in enumerate(self.secondary_buffer_size):
            self.secondary_channel_data[idx] = mp.Array('d', [0] * size)
            self.secondary_time_array[idx] = mp.Array('d', [0] * size)

        self.secondary_wptr = mp.Array('i', [0] * self.secondary_n_channels)
        self.secondary_buffer_full = mp.Array('i', [0] * self.secondary_n_channels)

        self.secondary_lock = []

        for i in range(self.secondary_n_channels):
            self.secondary_lock.append(mp.Lock())

    def receiver(self):
        """ Receive data from an LSL stream and store it in a circular
            buffer.
        """

        streams = []

        while not streams:
            print("Trying to connect to the stream: " + self.lsl_stream_name)
            streams = lsl.resolve_byprop('name', self.lsl_stream_name, timeout=10)
            if not streams:
                print("\tStream not found, re-trying...")

        inlet = lsl.StreamInlet(streams[0], max_buflen=1)
        print("\tDone")

        i = 0
        self.last_time.value = 0  # init the last_time value
        while self.run_state.value:
            x, t = inlet.pull_sample()
            self.primary_last_sample_received.value = time.time()

            self.primary_lock.acquire()  # LOCK-ON

            for k in range(self.primary_n_channels):
                self.primary_channel_data[k][self.primary_wptr.value] = x[k]

            if t is None:
                t = self.last_time.value + self.primary_sampling_rate

            self.primary_time_array[self.primary_wptr.value] = t
            self.last_time.value = t

            i += 1
            self.primary_wptr.value = i % self.primary_buffer_size
            self.primary_lock.release()  # LOCK-OFF

            # is the buffer full
            if (0 == self.primary_buffer_full.value) and (i >= self.primary_buffer_size):
                self.primary_buffer_full.value = 1
        # Ending run, clear inlet
        inlet.close_stream()

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
                socket.send_string('{};{}'.format(self.node_name, self.message_queue.get()))
            time.sleep(0.0001)

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

    def unwrap_channel(self, channel_name):
        """ Gives the unwrapping vector for the specified channel

        Args:
            channel_name <string>: name of the channel
        Returns:
            idx <list>: unwrapping vector
        """
        if channel_name in self.primary_channel_names:
            if self.primary_buffer_full.value:
                idx = [0] * self.primary_buffer_size
                for i in range(self.primary_buffer_size):
                    idx[i] = (self.primary_wptr.value + i) % self.primary_buffer_size
            else:
                idx = range(self.primary_wptr.value)

        elif channel_name in self.secondary_channel_names:
            ch_idx = self.secondary_channel_names.index(channel_name)
            if self.secondary_buffer_full[ch_idx]:
                idx = [0] * self.secondary_buffer_size[ch_idx]
                for i in range(self.secondary_buffer_size[ch_idx]):
                    idx[i] = (self.secondary_wptr[ch_idx] + i) % self.secondary_buffer_size[ch_idx]
            else:
                idx = range(self.secondary_wptr[ch_idx])

        return idx

    def push_sample_secondary(self, ch, timep, value, use_lock=True):
        """ Push a new sample into a secondary data buffer.

        Args:
               ch: <int>    secondary data channel index
            timep: <float>  time stamp of new sample
            value: <float>  value of new sample
        """
        if use_lock:
            self.secondary_lock[ch].acquire()

        self.secondary_channel_data[ch][self.secondary_wptr[ch]] = value
        self.secondary_time_array[ch][self.secondary_wptr[ch]] = timep
        self.secondary_wptr[ch] += 1

        if 0 == self.secondary_buffer_full[ch] and self.secondary_wptr[ch] >= self.secondary_buffer_size[ch]:
            self.secondary_buffer_full[ch] = 1

        self.secondary_wptr[ch] = self.secondary_wptr[ch] % self.secondary_buffer_size[ch]
        if use_lock:
            self.secondary_lock[ch].release()

    def push_chunk_secondary(self, ch, timeps, values):
        """ Push a chunk of new samples into a secondary data buffer.

        Args:
                ch: <int>   secondary data channel index
            timeps: <list>  list of time stamps for new values
            values: <list>  list of new values
        """

        self.secondary_lock[ch].acquire()
        for t, v in zip(timeps, values):
            self.push_sample_secondary(ch, t, v, use_lock=False)
        self.secondary_lock[ch].release()

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
                channels_ok = set(self.primary_channel_names + self.secondary_channel_names).issuperset(request['channels'])
            except:
                channels_ok = False

        if 'time_window' in request:
            try:
                time_ok = all([isinstance(t, (float, int)) for t in request['time_window']])
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
        return list(set.intersection(set(channels), set(self.primary_channel_names + self.secondary_channel_names)))

    def get_data_from_channel(self, channel_name):
        """ Copy and unwrap data from specified channel

        Args:
            channel_name <string>: name of the channel
        Returns:
            data <list> array of samples
            times <list> array of timestamps
        """
        if channel_name in self.primary_channel_names:
            time_array = self.primary_time_array[:]
            data = self.primary_channel_data[self.primary_channel_names.index(channel_name)][:]

        elif channel_name in self.secondary_channel_names:
            idx = self.secondary_channel_names.index(channel_name)
            time_array = self.secondary_time_array[idx][:]
            data = self.secondary_channel_data[idx][:]

        unwrap_idx = self.unwrap_channel(channel_name)

        time_array = [time_array[i] for i in unwrap_idx]
        time_array = [abs(i - time_array[-1]) for i in time_array]
        data = [data[i] for i in unwrap_idx]

        return data, time_array

    def lock_all_secondary(self):
        """ Locks all channels of the secondary buffer. """
        [lock.acquire() for lock in self.secondary_lock]

    def release_all_secondary(self):
        """ Releases all channels of the secondary buffer. """
        [lock.release() for lock in self.secondary_lock]

    def snapshot_data(self, channels):
        """ Copies specified data channels.

        Args:
            channels <list>: list of channels
        Returns:
            snapshot <dict>: data and times for each channel
        """
        self.primary_lock.acquire()
        self.lock_all_secondary()
        snapshot = {}
        for channel in channels:
            data, time = self.get_data_from_channel(channel)
            snapshot[channel] = (data, time)
        self.primary_lock.release()
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
                    # TODO: Consider moving this to unpack_snapshot
                    if self.primary_node:
                        last_sample = time.time() - self.primary_last_sample_received.value
                        request['primary_last_sample_received'] = last_sample

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

                if self.primary_node:
                    last_sample = time.time() - self.primary_last_sample_received.value
                    request['primary_last_sample_received'] = last_sample

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

    def start(self):
        """ Start the node. """
        self.run_state.value = 1

        # Add user-defined metrics to the metric list
        self.generate_metric_lists()

        # Create and configure beacon
        # TODO: Change argument names in utilities.py as well
        self.beacon = mu.Beacon(name=self.node_name,
                                node_type=self.node_type,
                                node_id=self.node_id,
                                interval=2)
        self.beacon.ip = self.ip
        self.beacon.port = self.port_frontend

        # Start the load-balancing broker
        self.proc_broker = mp.Process(target=mu.LRU_queue_broker,
                                      args=(self.url_frontend,
                                            self.url_backend,
                                            self.n_responders,
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
        self.proc_responder_list = [0] * self.n_responders

        for i in range(self.n_responders):
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
        print("Node '%s' now online." % self.node_name)

    def stop(self):
        """ Terminates the node. """
        if self.run_state.value:

            print("Node '%s' shutting down ..." % self.node_name)

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
            print("Node '%s' is not running." % self.node_name)

        print("Node '%s' is now offline." % self.node_name)

    def show_ui(self):
        """ Show a minimal user interface. """
        while True:
            tmp = input(" > ")
            if tmp == "q":
                self.stop()
                sys.exit(0)

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

        def check_num_args(fun_handle):
            n_args = len(inspect.getargspec(fun_handle).args)
            if inspect.ismethod(fun_handle) and n_args >= 2:
                return True
            elif not inspect.ismethod(fun_handle) and n_args >= 1:
                return True
            else:
                return False

        self.metric_names = [func.__name__ for func in self.metric_functions]
        docs = [func.__doc__ for func in self.metric_functions]
        self.metric_descriptions = dict(zip(self.metric_names, docs))
        self.metric_pointers = dict(zip(self.metric_names,
                                        self.metric_functions))
        # Finally check if each metric function has at least one argument
        for metric in self.metric_functions:
            if not check_num_args(metric):
                raise AttributeError('Metric function has no arguments')

    def generate_nodeinfo(self):
        """ Stores all node information in a dict """
        self.nodeinfo = {}
        self.nodeinfo['name'] = self.node_name
        self.nodeinfo['desc'] = self.node_description
        self.nodeinfo['primary_node'] = self.primary_node
        self.nodeinfo['channel_count'] = self.primary_n_channels
        self.nodeinfo['channel_names'] = ",".join(self.primary_channel_names)
        self.nodeinfo['channel_descriptions'] = ",".join(self.primary_channel_descriptions)
        self.nodeinfo['sampling_rate'] = self.primary_sampling_rate
        self.nodeinfo['buffer_size'] = self.primary_buffer_size_s
        self.nodeinfo['buffer_full'] = self.primary_buffer_full.value

    def get_metric_list(self):
        """ Returns the metrics list of the node as a dictionary where the name
            of the metric is the key and the description is the value.
        """
        return self.metric_descriptions

    def get_topic_list(self):
        """ Return topics that are published by the node. """
        return self.topic_list

    def get_nodeinfo(self):
        """ Return information about the node. """
        # TODO: Merge above, generate_nodeinfo is only called here
        self.generate_nodeinfo()
        return self.nodeinfo

    def get_publisher_url(self):
        """ Return the URL of the publisher socket in the node. """
        return self.url_publisher

    def get_data_list(self):
        """ Returns the data list of the node as a dictionary where the name of
            the data is the key and the description is the value.
        """
        names = self.primary_channel_names + self.secondary_channel_names
        descriptions = self.primary_channel_descriptions + self.secondary_channel_descriptions
        return dict(zip(names, descriptions))
