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

import os
import zmq
import sys
import json
import time
import bottle
import random
import waitress
import threading
from . import utilities as mu


class Dispatcher():

    """ Dispatcher for MIDAS.
    """

    def __init__(self,
                 config=None,
                 node_list=None,
                 port=8080,
                 ip=None,
                 n_threads=5,
                 run_pubsub_proxy=False,
                 proxy_port_in=None,
                 proxy_port_out=None):
        """ Initializes a Dispatcher-object.

        Args:
            ini_file: <dict> contents of a configuration file
            node_list: <list> list of node types to discover
            port: <int> port number for the web server
            ip: <str> IP for the web server
            n_threasds: <int> number of threads
        """

        self.node_addresses = {}
        self.node_metrics = {}
        self.node_data = {}
        self.node_indices = {}
        self.node_topics = {}
        self.node_publisher_urls = {}
        self.node_publisher_connected = []

        # server settings
        if config:
            if 'port' in config:
                port = int(config['port'])
            if 'threads' in config:
                n_threads = config['threads']
            if 'node_list' in config:
                node_list = mu.listify(config, 'node_list')
            if 'ip' in config:
                ip = config['ip']
            if 'run_pubsub_proxy' in config:
                run_pubsub_proxy = mu.str2bool(config['run_pubsub_proxy'])
            if 'proxy_port_in' in config:
                proxy_port_in = config['proxy_port_in']
            if 'proxy_port_out' in config:
                proxy_port_out = config['proxy_port_out']

        self.port = port
        self.n_threads = n_threads
        self.node_list = node_list

        self.run_pubsub_proxy = run_pubsub_proxy
        self.proxy_port_in = proxy_port_in
        self.proxy_port_out = proxy_port_out

        # get IP address
        if ip:
            self.ip = ip
        else:
            self.ip = mu.get_ip()

        if self.ip == 'localhost':
            self.ip = '127.0.0.1'

        # set pubsub proxy url
        if self.run_pubsub_proxy:
            self.url_proxy_out = "tcp://" + \
                str(self.ip) + ":" + str(self.proxy_port_out)
        else:
            self.url_proxy_out = ''

        self.context = zmq.Context()

        # create a variable that tracks if there are new publishers
        self.new_publisher = mu.DataState(0)

        # set update interval (in seconds) for node discovery
        self.discovery_interval = 10

        # Initially discover all nodes and metrics
        self.discover_nodes()
        if self.node_addresses:
            self.discover_node_properties()

    def pubsub_proxy(self, context):
        """ Run proxy for publisher-subscriber.

            The nodes are publishing messages on various addresses, and
            the proxy relays all of these through one address.
        """

        # create sockets and bind
        socket_proxy_in = context.socket(zmq.XSUB)
        socket_proxy_out = context.socket(zmq.XPUB)

        socket_proxy_out.bind(self.url_proxy_out)

        self.node_publisher_connected = []
        for nodename in self.node_publisher_urls:
            publisher_url = self.node_publisher_urls[nodename]
            socket_proxy_in.bind(publisher_url)
            self.node_publisher_connected.append(publisher_url)
        self.new_publisher.setstate(0)

        try:
            zmq.proxy(socket_proxy_in, socket_proxy_out)
        except zmq.error.ContextTerminated:
            pass

    def pubsub_proxy_watchdog(self):
        """ A watchdog function that starts the publisher-subscriber proxy and
            monitors if there are new  publishers. In case of new publishers,
            the proxy is restarted.

            In the restarting process some messages may be lost.
        """

        proxy_running = False
        context = None

        while self.run_state:
            if self.new_publisher.getstate() == 1:

                if proxy_running:
                    if context:
                        context.term()
                        proxy_running = False
                else:
                    context = zmq.Context()
                    psproxy = threading.Thread(
                        target=self.pubsub_proxy,
                        args=(
                            context,
                            ))
                    psproxy.start()
                    proxy_running = True

                time.sleep(0.0001)

    def update_nodes(self):
        """ Update nodes and corresponding metrics.
        """

        while self.run_state:
            self.discover_nodes()
            if self.node_addresses:
                self.discover_node_properties()
            time.sleep(self.discovery_interval)

    def discover_nodes(self):
        """ Find all nodes that are online.
        """

        new_nodes = {}
        all_nodes = mu.discover_all_nodes(timeout=5)

        if self.node_list is not None:
            for node in all_nodes:
                if node in self.node_list:
                    new_nodes[node] = all_nodes[node]
            self.node_addresses = new_nodes
        else:
            self.node_addresses = all_nodes

    def discover_node_properties(self):
        """ Discover the properties of nodes in our address book.
        """

        new_metrics = {}
        new_data = {}
        new_indices = {}
        new_topics = {}
        new_publisher_urls = {}

        new_publishers = 0

        for node in self.node_addresses:
            if node:
                socket_tmp = self.context.socket(zmq.REQ)
                socket_tmp.connect(self.node_addresses[node]['address'])

                # Get metric list
                mu.midas_send(socket_tmp, 'command', 'get_metric_list')
                node_metric_list = json.loads(socket_tmp.recv_string())
                new_metrics.update({node: node_metric_list})

                # Get data list
                mu.midas_send(socket_tmp, 'command', 'get_data_list')
                node_data_list = json.loads(socket_tmp.recv_string())
                new_data.update({node: node_data_list})

                # Get topic list
                mu.midas_send(socket_tmp, 'command', 'get_topic_list')
                node_topic_list = json.loads(socket_tmp.recv_string())
                new_topics.update({node: node_topic_list})

                # Get publisher URLs
                mu.midas_send(socket_tmp, 'command', 'get_publisher')
                node_publisher_url = json.loads(socket_tmp.recv_string())
                new_publisher_urls.update({node: node_publisher_url})

                if node_publisher_url not in self.node_publisher_connected:
                    new_publishers += 1

            self.node_metrics = new_metrics
            self.node_data = new_data
            self.node_indices = new_indices
            self.node_topics = new_topics
            self.node_publisher_urls = new_publisher_urls

            # signal if there were new publishers
            if new_publishers > 0:
                self.new_publisher.setstate(1)

    def format_json(self, data):
        """ Utility function to format json-dumps.
        """
        callback_function = bottle.request.GET.get('callback')

        bottle.response.content_type = 'application/json'

        if callback_function:
            result = "%s(%s)" % (callback_function, data)
        else:
            result = json.dumps(
                data,
                sort_keys=True,
                indent=4,
                separators=(
                    ',',
                    ' : '))
        return result

    def pass_json(self, data):
        """ Just pass the data, dont wrap in JSON (because metrics and data
            are already in JSON).
        """
        callback_function = bottle.request.GET.get('callback')

        bottle.response.content_type = 'application/json'

        if callback_function:
            result = "%s(%s)" % (callback_function, data)
        else:
            result = data

        return result

# =============================================================================
# Route definitions
# =============================================================================

    def root(self):
        """ Returns the root.
        """

        return('MIDAS Dispatcher online.')
    # -------------------------------------------------------------------------

    def status_nodes(self):
        """
        @api {get} /status/nodes Available nodes
        @apiVersion 0.3.0
        @apiPermission none
        @apiParam {String} name Name of the User.
        @apiGroup Status
        @apiName GetStatusNodes
        @apiDescription Returns the available nodes in the MIDAS network.


        @apiSuccess {String} address The address of the node
                             (for MIDAS internal use only).
        @apiSuccess {String} description A brief description of the node.
        @apiSuccess {String} id The ID of the node.
        @apiSuccess {String} service A short name indicating the service
                             provided by the node.
        @apiSuccess {String} status_online Node status is either online or
                             offline.
        @apiSuccess {String} status_description A description related to
                             status_online

        @apiExample Request status of all nodes
            http 127.0.0.1:8080/status/nodes

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_a" : {
                "address" : "tcp://192.168.2.226:5011",
                "id" : "01",
                "service" : "example_node_a",
                "status" : "online"
            },
            "example_node_two" : {
                "address" : "tcp://192.168.2.226:5014",
                "id" : "02",
                "service" : "example_node_two",
                "status" : "online"
            }
        }
        """

        bottle.response.content_type = 'application/json'

        return self.format_json(self.node_addresses)

    # -------------------------------------------------------------------------

    def status_metrics(self, node=None):
        """
        @api {get} /:nodename/status/metrics Available metrics
        @apiGroup Status
        @apiName GetStatusMetrics
        @apiDescription Return the metrics (with descriptions) that can be
                        returned by the nodes.
        @apiParam {String} nodename The name of the node. Omit to target all
                           nodes.

        @apiExample Request metrics from all nodes
            http 127.0.0.1:8080/status/metrics

        @apiExample Request metrics from the node named 'example_node_a'
            http 127.0.0.1:8080/example_node_a/status/metrics

        @apiSuccessExample {json} Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_a" : {
                "metric_a" : "Returns the metric A calculated from the data.
                              The function takes the start and stop frequencies
                              as paramters: metric_a(band_start, band_stop).",
                "metric_b" : "Returns the metric B calculated from the data.",
                "metric_c" : "Returns the metric C calculated from the data."
            }
        }

        """

        bottle.response.content_type = 'application/json'

        if node and node in self.node_metrics:
            return self.format_json(self.node_metrics[node])
        else:
            return self.format_json(self.node_metrics)
    # -------------------------------------------------------------------------

    def status_data(self, node=None):
        """
        @api {get} /:nodename/status/data Available data
        @apiGroup Status
        @apiName GetStatusData
        @apiDescription Return the available data that can be returned by the
                        nodes.
        @apiParam {String} nodename The name of the node. Omit to target all
                           nodes.

        @apiExample Request data from all nodes
            http 127.0.0.1:8080/status/data

        @apiExample Request data from the node named 'example_node_a'
            http 127.0.0.1:8080/example_node_a/status/data


        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_a" : {
                "Ch1" : "First channel",
                "Ch2" : "Second channel"
            },
            "example_node_two" : {
                "Ch1" : "First channel",
            }
        }
        """

        bottle.response.content_type = 'application/json'

        if node and node in self.node_data:
            return self.format_json(self.node_data[node])
        else:
            return self.format_json(self.node_data)
    # -------------------------------------------------------------------------

    def status_topics(self, node=None):
        """
        @api {get} /:nodename/status/topics Available topics
        @apiGroup Status
        @apiName GetStatusTopics
        @apiDescription Return the topics (with descriptions) that are published
                        by the nodes.
        @apiParam {String} nodename The name of the node. Omit to target all
                           nodes.

        @apiExample Request topics published from all nodes
            http 127.0.0.1:8080/status/topics

        @apiExample Request topics from the node named 'example_node_a'
            http 127.0.0.1:8080/example_node_a/status/topics

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_a" : {
                "topic_one" : "a test topic",
                "topic_two" : "another topic"
            },
            "example_node_two" : {}
        }
       """

        bottle.response.content_type = 'application/json'

        if node and node in self.node_topics:
            return self.format_json(self.node_topics[node])
        else:
            return self.format_json(self.node_topics)
    # -------------------------------------------------------------------------

    def status_publisher(self):
        """
        @api {get} /status/publisher Publisher URL
        @apiGroup Status
        @apiName GetStatusPublisher
        @apiDescription Return the ZeroMQ URL used by the Midas Dispatcher to
                        relay messages published by the nodes.

        @apiExample Get the dispatcher publisher URL
            http 127.0.0.1:8080/status/publisher

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "url" : "tcp://127.0.0.1:6000"
        }
       """

        bottle.response.content_type = 'application/json'

        if self.run_pubsub_proxy:
            return self.format_json({'url': self.url_proxy_out})
        else:
            return self.format_json({'error': 'proxy not running'})
    # -------------------------------------------------------------------------

    def status_nodeinfo(self, node):
        """
        @api {get} /:nodename/status/info Node information
        @apiGroup Status
        @apiName GetStatusNode
        @apiDescription Return information for a particular node.
        @apiParam {String} nodename The name of the node.

        @apiExample Request information from the node named 'example_node_a'
            http 127.0.0.1:8080/example_node_a/status/info

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "buffer_full" : 1,
            "buffer_size" : 0.01,
            "channel_count" : 2,
            "channel_descriptions" : [
                "First channel",
                "Second channel"
            ],
            "channel_names" : [
                "Ch1",
                "Ch2"
            ],
            "desc" : "An example node",
            "name" : "example_node_a",
            "primary_node" : true,
            "sampling_rate" : 500
        }
        """

        if node in self.node_addresses:
            socket_tmp = self.context.socket(zmq.REQ)
            socket_tmp.connect(self.node_addresses[node]['address'])
            mu.midas_send(socket_tmp, 'command', 'get_nodeinfo')
            results = socket_tmp.recv()
        else:
            results = {'error': 'node not available'}

        return self.pass_json(results)
    # -------------------------------------------------------------------------

    def get_metric(self, node, requests):
        """@api {get} /:nodename/metric/:requests Request metrics
        @apiGroup Metrics
        @apiName GetMetric @apiDescription Request one or more metrics from a
                                           specific node.

        @apiParam {String} nodename The name of the node.

        @apiParam {String} requests Single metric request (or a list of
        multiple requests) for the node to calculate. All requests are JSON
        formatted and multiple simultaneous requests can be placed inside a JSON
        array, e.g. [request, request, ...]

        @apiParam {String} request JSON-formatted metric request. Request
        parameters are defined as key-value pairs. The "type"-key, followed by
        the metric name, defines which metric is used. Channels can be specified
        by "channels"-key, followed by a list of channel names. Time-window of
        the data is specified using the "time_window"-key, followed by two
        element array. The time window is composed of two numbers: the first
        number indicates the starting point of the data in seconds from the
        present time The second number indicates the length of data segment in
        seconds. Extra positional arguments of the target metric functions can
        be given arguments using the "arguments"-key, followed by a list of
        arguments.

        @apiExample {curl} Request metric without any extra arguments
            http 127.0.0.1:8080/example_node_a/metric/'{"type":"metric_a"}'

        @apiExample {curl} Request metric using data from specified channel
            http 127.0.0.1:8080/example_node_a/metric/'{"type":"metric_a", "channels":["Ch1"]}'

        @apiExample Request metric calculated using data from multiple channels
            http 127.0.0.1:8080/example_node_a/metric/'{"type":"metric_a", "channels":["Ch1", "Ch2"]}'

        @apiExample Request one metric with extra parameters
            http 127.0.0.1:8080/example_node_a/metric/'{"type":"metric_a", "arguments":[12, 0.05]}'

        @apiExample Request multiple metrics
            url -i 127.0.0.1:8080/example_node_a/metric/'[{"type":"metric_a"}, {"type":"metric_b"}]'

        @apiExample Request multiple metrics with differing arguments
            http 127.0.0.1:8080/example_node_a/metric/'[{"type":"metric_a", "channels":["Ch1"]}, {"type":"metric_b", "channels":["Ch2"], "arguments":[1.5, 128]}]'

        @apiExample Request metric from specified channel and time-window
            http 127.0.0.1:8080/example_node_a/metric/'{"type":"metric_a", "channels":["Ch1"], "time-window":[10, 5]}'

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        [
            {
                "channels": ["Ch1"],
                "return": 81.5,
                "type": "metric_a"
            }
        ]
        """

        if node in self.node_addresses:
            socket_tmp = self.context.socket(zmq.REQ)
            socket_tmp.connect(self.node_addresses[node]['address'])

            mu.midas_send(socket_tmp, 'metric', requests)
            result = socket_tmp.recv_string()

            return self.pass_json(result)
        else:
            return self.format_json({'error': 'node not available'})
    # -------------------------------------------------------------------------

    def get_data(self, node, requests):
        """
        @api {get} /:nodename/data/:requests Request data
        @apiGroup Data
        @apiName GetData Request
        @apiDescription Return data from the nodes.
        @apiParam {String} nodename The name of the node.

        @apiParam {String} requests Single data requests (or a list of multiple
        requests) to be retrieved from the node. All requests are JSON formatted
        and multiple simultaneous requests can be placed inside a JSON array,
        e.g. [request, request, ...]

        @apiParam {String} request JSON-formatted metric request. Request
        parameters are defined as key-value pairs. Channels can be specified
        by "channels"-key, followed by a list of channel names. Time-window of
        the data is specified using the "time_window"-key, followed by two
        element array. The time window is composed of two numbers: the first
        number indicates the starting point of the data in seconds from the
        present time The second number indicates the length of data segment in
        seconds.

        @apiExample Request the past 3 seconds of data from channel Ch1
            http 127.0.0.1:8080/example_node_a/data/'{"channels":["Ch1"], "time_window":[3, 3]}'

        @apiExample Request all data from channels Ch1 and Ch2
            http 127.0.0.1:8080/example_node_a/data/'{"channels":["Ch1", "Ch2"]}'


        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        [
            {
                "channels": [
                    "Ch1",
                    "Ch2"
                ],
                "return": {
                    "Ch1": {
                        "data": [
                            43.0,
                            44.0
                        ],
                        "time": [
                            2.0009971640010917,
                            1.0003105989999312
                        ]
                    },
                    "Ch2": {
                        "data": [
                            43.0,
                            44.0
                        ],
                        "time": [
                            2.0009971640010917,
                            1.0003105989999312
                        ]
                    }
                },
                "time_window": [
                    3
                ]
            }
        ]

        """

        if node in self.node_addresses:
            socket_tmp = self.context.socket(zmq.REQ)
            socket_tmp.connect(self.node_addresses[node]['address'])

            mu.midas_send(socket_tmp, 'data', requests)
            data = socket_tmp.recv_string()
            return self.pass_json(data)
        else:
            return self.format_json({node: 'not available'})

    def get_test(self):
        """
        @api {get} /test Test Dispatcher
        @apiGroup Status
        @apiName TestDispatcher
        @apiDescription Dispatcher test function, returns a random number.

        @apiExample Test dispatcher
            http 127.0.0.1:8080/test

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "test": 0.09173727416497413
        }
        """
        x = random.uniform(0, 1)
        return self.format_json({'test': x})

    def ping_node(self, node, num):
        """
        @api {get} /:nodename/pings/:n Ping node
        @apiGroup Status
        @apiName PingNode
        @apiDescription Ping the specified node. Returns round-trip-times for
                        each ping.
        @apiParam {String} nodename The name of the node.
        @apiParam {Int} n number of ping repetitions

        @apiExample Ping target node 5 times
            http 127.0.0.1:8080/example_node_a/ping/5

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_a_RTT": [
                0.0009348392486572266,
                0.0008633136749267578,
                0.0005664825439453125,
                0.0006177425384521484,
                0.0005941390991210938
            ]
        }
        """
        if node in self.node_addresses:
            latencies = []
            for _ in range(int(num)):
                time_sent = time.time()
                socket_tmp = self.context.socket(zmq.REQ)
                socket_tmp.connect(self.node_addresses[node]['address'])
                mu.midas_send(socket_tmp, 'ping', "ping")
                ping = socket_tmp.recv_string()
                latencies.append(time.time() - time_sent - float(ping))
            return self.format_json({node + "_RTT": latencies})

        else:
            return self.format_json({node: 'not available'})

    def show_ui(self):
        """ Show a minimal user interface.
        """

        time.sleep(1)

        while self.run_state:
            tmp = input(" > ")
            if tmp == "q":
                break
        self.stop()

    def start(self):
        """ Starts running the dispatcher server. Routes are also configured
            here because they can not be formed until the object has been
            initialized.
        """

        # control variable for threads
        self.run_state = True

        # start thread handling updating of nodes
        self.ud = threading.Thread(target=self.update_nodes)
        self.ud.start()

        # start thread running publisher-subscriber proxy
        if self.run_pubsub_proxy:
            self.psproxy_watchdog = threading.Thread(
                target=self.pubsub_proxy_watchdog)
            self.psproxy_watchdog.start()

        # Connect routes
        bottle.route('/', method="GET")(self.root)
        bottle.route(
            '/<node>/metric/<requests>',
            method="GET")(
            self.get_metric)
        bottle.route(
            '/<node>/data/<requests>',
            method="GET")(
            self.get_data)

        # Status request routes
        bottle.route('/status/nodes', method="GET")(self.status_nodes)
        bottle.route('/status/metrics', method="GET")(self.status_metrics)
        bottle.route('/status/data', method="GET")(self.status_data)
        bottle.route('/status/topics', method="GET")(self.status_topics)
        bottle.route('/status/publisher', method="GET")(self.status_publisher)

        # Node status request routes
        bottle.route(
            '/<node>/status/metrics',
            method="GET")(
            self.status_metrics)
        bottle.route('/<node>/status/data', method="GET")(self.status_data)
        bottle.route('/<node>/status/topics', method="GET")(self.status_topics)

        # 'On-demand' status request routes
        bottle.route('/<node>/status/info', method="GET")(self.status_nodeinfo)

        # Test method
        bottle.route('/test', method="GET")(self.get_test)

        # Ping method
        bottle.route('/<node>/ping/<num>', method="GET")(self.ping_node)

        # Thread for the UI
        self.ui = threading.Thread(target=self.show_ui)
        self.ui.start()

        # Start the web server
        app = bottle.default_app()
        waitress.serve(
            app,
            host=self.ip,
            port=self.port,
            threads=self.n_threads)

    def stop(self):
        print("Stopping dispatcher")
        self.run_state = False

        self.ud.join()

        if self.run_pubsub_proxy:
            self.psproxy_watchdog.join()

        os._exit(1)

# -----------------------------------------------------------------------------
# Run the dispatcher if started from the command line
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    dp = mu.midas_parse_config(Dispatcher, sys.argv)
    if dp is not None:
        dp.start()
# -----------------------------------------------------------------------------
