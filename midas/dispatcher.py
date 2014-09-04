#!/usr/bin/env python3

# This file is part of the MIDAS system.
# Copyright 2014
# Andreas Henelius <andreas.henelius@ttl.fi>, Jari Torniainen <jari.torniainen@ttl.fi>
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
    """ Dispatcher for MIDAS. """
    def __init__(self,
                 config = None,
                 node_list = None,
                 port = 8080,
                 ip = None,
                 n_threads = 5,
                 run_pubsub_proxy = False,
                 proxy_port_in = None,
                 proxy_port_out = None):

        """ Initializes a Dispatcher-object.

        Args:
            ini_file: <dict> contents of a configuration file
            node_list: <list> list of node types to discover
            port: <int> port number for the web server
            ip: <str> IP for the web server
            n_threasds: <int> number of threads
        """

        self.node_addresses            = {}
        self.node_metrics              = {} 
        self.node_data                 = {}
        self.node_indices              = {}
        self.node_topics               = {}
        self.node_publisher_urls       = {}
        self.node_publisher_connected  = []

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
                proxy_port_out= config['proxy_port_out']

        self.port       = port
        self.n_threads  = n_threads
        self.node_list  = node_list

        self.run_pubsub_proxy = run_pubsub_proxy
        self.proxy_port_in    = proxy_port_in
        self.proxy_port_out   = proxy_port_out

        # get IP address
        if ip:
            self.ip = ip
        else:
            self.ip = mu.get_ip()

        if self.ip == 'localhost':
            self.ip = '127.0.0.1'

        # set pubsub proxy url
        if self.run_pubsub_proxy:
            self.url_proxy_out = "tcp://" + str(self.ip) + ":" + str(self.proxy_port_out)
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

# =============================================================================

    def pubsub_proxy(self, context):
        """ Run proxy for publisher-subscriber. 
        
            The nodes are publishing messages on various addresses, and
            the proxy relays all of these through one address.
        """

        # create sockets and bind
        socket_proxy_in  = context.socket(zmq.XSUB)
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
        """ A watchdog function that starts the publisher-subscriber proxy and monitors if
            there are new  publishers. In case of new publishers, the proxy is restarted.

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
                    psproxy = threading.Thread(target = self.pubsub_proxy, args = (context,))
                    psproxy.start()
                    proxy_running = True

                time.sleep(0.0001)

# =============================================================================

    def update_nodes(self):
        """ Update nodes and corresponding metrics. """

        while self.run_state:
            self.discover_nodes()
            if self.node_addresses:
                self.discover_node_properties()
            time.sleep(self.discovery_interval)


    def discover_nodes(self):
        """ Find all nodes that are online. """

        new_nodes = {}
        all_nodes = mu.discover_all_nodes(timeout = 5)

        for node in all_nodes:
            if node in self.node_list:
                new_nodes[node] = all_nodes[node]

        self.node_addresses = new_nodes


    def discover_node_properties(self):
        """ Discover the properties of nodes in our address book. """

        new_metrics        = {}
        new_data           = {}
        new_indices        = {}
        new_topics         = {}
        new_publisher_urls = {}
        
        new_publishers = 0

        for node in self.node_addresses:
            if node:
                socket_tmp = self.context.socket(zmq.REQ)
                socket_tmp.connect(self.node_addresses[node]['address'])

                # Get metric list
                mu.midas_send_message(socket_tmp,'command', ['get_metric_list'])
                node_metric_list = mu.midas_receive_reply(socket_tmp, deserialize = True) 
                new_metrics.update({node:node_metric_list})

                # Get data list
                mu.midas_send_message(socket_tmp,'command', ['get_data_list'])
                node_data_list = mu.midas_receive_reply(socket_tmp, deserialize = True) 
                new_data.update({node:node_data_list})

                # Get topic list
                mu.midas_send_message(socket_tmp,'command', ['get_topic_list'])
                node_topic_list = mu.midas_receive_reply(socket_tmp, deserialize = True) 
                new_topics.update({node:node_topic_list})

                # Get publisher URLs
                mu.midas_send_message(socket_tmp,'command', ['get_publisher'])
                node_publisher_url = mu.midas_receive_reply(socket_tmp, deserialize = True) 
                new_publisher_urls.update({node:node_publisher_url})

                if not node_publisher_url in self.node_publisher_connected:
                    new_publishers += 1
                    
            self.node_metrics        = new_metrics
            self.node_data           = new_data
            self.node_indices        = new_indices
            self.node_topics         = new_topics
            self.node_publisher_urls = new_publisher_urls

            # signal if there were new publishers
            if new_publishers > 0:
                self.new_publisher.setstate(1)


# =============================================================================

    def wrap_list(self,dump):
        """ Utility function to format json-dumps as HTML. """

        return("<pre>" + json.dumps(dump,sort_keys=True,indent=4,separators=(',',':')) +"</pre>")


    def format_json(self, data):
        """ Utility function to format json-dumps. """
        callback_function = bottle.request.GET.get('callback')

        bottle.response.content_type = 'application/json'
      
        if callback_function:
            result = "%s(%s)" % (callback_function, data)
        else:
            result = json.dumps(data, sort_keys = True, indent = 4, separators = (',',' : '))

        return result
        
# =============================================================================
# Route definitions
# =============================================================================

    def root(self):
        """ Returns root. """

        return('MIDAS Dispatcher online.')

    # ------------------------------------------------------------------------------

    def status_nodes(self):
        """
        @api {get} /status/nodes Available nodes
        @apiGroup Status
        @apiName GetStatusNodes
        @apiDescription Returns the available nodes in the MIDAS system.


        @apiSuccess {String} address The address of the node (for MIDAS internal use only).
        @apiSuccess {String} description A brief description of the node.
        @apiSuccess {String} id The ID of the node.
        @apiSuccess {String} service A short name indicating the service provided by the node.
        @apiSuccess {String} status_online Node status is either online or offline.
        @apiSuccess {String} status_description A description related to status_online

        @apiExample Request status of all nodes
                    curl -i http://midas_dispatcher:8080/status/nodes

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_one" : {
                "address" : "tcp://192.168.2.226:5011",
                "id" : "01",
                "service" : "example_node_one",
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
        #return self.node_addresses

    # ------------------------------------------------------------------------------

    def status_metrics(self, node = None):
        """
        @api {get} /:nodename/status/metrics Available metrics
        @apiGroup Status
        @apiName GetStatusMetrics
        @apiDescription Return the metrics (with descriptions) that can be returned by the nodes.
        @apiParam {String} nodename The name of the node. Omit to target all nodes.

        @apiExample Request metrics from all nodes
                    curl -i http://midas_dispatcher:8080/status/metrics

        @apiExample Request metrics from the node named 'example_node_one'
                    curl -i http://midas_dispatcher:8080/example_node_one/status/metrics

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_one" : {
                "metric_a" : "Returns the metric A calculated from the data. The function takes the start and stop frequencies as paramters: metric_a(band_start, band_stop).",
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

    # ------------------------------------------------------------------------------

    def status_data(self, node = None):
        """
        @api {get} /:nodename/status/data Available data
        @apiGroup Status
        @apiName GetStatusData
        @apiDescription Return the available data that can be returned by the nodes.
        @apiParam {String} nodename The name of the node. Omit to target all nodes.

        @apiExample Request data from all nodes
                    curl -i http://midas_dispatcher:8080/status/data

        @apiExample Request data from the node named 'example_node_one'
                    curl -i http://midas_dispatcher:8080/example_node_one/status/data


        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_one" : {
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

    # ------------------------------------------------------------------------------

    def status_topics(self, node = None):
        """
        @api {get} /:nodename/status/topics Available topics
        @apiGroup Status
        @apiName GetStatusTopics
        @apiDescription Return the topics (with descriptions) that are published by the nodes.
        @apiParam {String} nodename The name of the node. Omit to target all nodes.

        @apiExample Request topics published from all nodes
                    curl -i http://midas_dispatcher:8080/status/topics

        @apiExample Request topics from the node named 'example_node_one'
                    curl -i http://midas_dispatcher:8080/example_node_one/status/topics

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "example_node_one" : {
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
    
    # ------------------------------------------------------------------------------

    def status_publisher(self):
        """
        @api {get} /status/publisher Publisher URL
        @apiGroup Status
        @apiName GetStatusPublisher
        @apiDescription Return the ZeroMQ URL used by the Midas Dispatcher to relay messages published by the nodes.

        @apiExample Get the dispatcher publisher URL
                    curl -i http://midas_dispatcher:8080/status/publisher

        @apiSuccessExample Success-Response:
        HTTP/1.1 200 OK
        {
            "url" : "tcp://127.0.0.1:6000"
        }
       """

        bottle.response.content_type = 'application/json'

        if self.run_pubsub_proxy:
            return self.format_json({'url' : self.url_proxy_out})
        else:
            return self.format_json({'error' : 'proxy not running'})

    # ------------------------------------------------------------------------------

    def status_nodeinfo(self, node):
        """
        @api {get} /:nodename/status/info Node information
        @apiGroup Status
        @apiName GetStatusNode
        @apiDescription Return information for a particular node.
        @apiParam {String} nodename The name of the node.

        @apiExample Request information from the node named 'example_node_one'
                    curl -i http://midas_dispatcher:8080/example_node_one/status/info

        @apiSuccessExample Success-Response:
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
            "name" : "example_node_one",
            "primary_node" : true,
            "sampling_rate" : 500
        }
        """

        if node in self.node_addresses:
            socket_tmp = self.context.socket(zmq.REQ)
            socket_tmp.connect(self.node_addresses[node]['address'])
            mu.midas_send_message(socket_tmp,'command', ['get_nodeinfo'])
            results = mu.midas_receive_reply(socket_tmp)      
        else:
            results = {'error' : 'node not available'}

        return self.format_json(results)
 
    # ------------------------------------------------------------------------------

    def get_metric(self, node, metric_names, time):
        """@api {get} /:nodename/metric/:metric_list/:time_window Request metrics
        @apiGroup Metrics
        @apiName GetMetric @apiDescription Request one or more metrics from a specific node.

        @apiParam {String} nodename The name of the node.

        @apiParam {String} metric_names The metrics to be calculated
        by the node. Parameters to a metric are separated by colons
        (:), multiple metrics are separated by semicolons
        (;). Parameters can be passed to the metrics. The channel name
        must always be given as the first argument to the
        metric.Multiple channel names are separated by commas (,) for
        metrics that use data from multiple channel as input. If the
        node only receives a data stream with one channel, the channel
        name can be omitted from the arguments to the metric.
        
        @apiParam {Float} time_window The time window of data to be
        used in the calculation of the metric. All metrics are
        calculated from the same data and are hence comparable. The
        time window is composed of two numbers: the first number
        indicates the starting point of the data in seconds from the
        present time, and the second number indicates the length of
        data to be used in seconds. The parts are separated by a colon
        (:). If the colon and the second number are omitted, all data
        from the past n seconds are used, where n is the number given
        as the time window.

        @apiExample Request one metric without parameters
                    # Request the metric 'metric_e' from the node 'example_node_two'.
                    # using a time window of length 3 seconds, starting 10 seconds ago from the present time.
                    # The metric will be calculated from the channel 'Ch1'
                    
                    curl -i http://midas_dispatcher:8080/example_node_two/metric/metric_e:Ch1/10:3


        @apiExample Request one metric specifying channel name
                    # Request the metric 'metric_b' from the node 'example_node_one'.
                    # using a time window of length 3 seconds, starting 10 seconds ago from the present time.
                    # The metric will be calculated from the channel Ch2
                    
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_b:Ch2/10:3

        @apiExample Request the same metric calculated from different channels
                    # Request the metric 'metric_b' from the node 'example_node_one'.
                    # using a time window of length 3 seconds, starting 10 seconds ago from the present time.
                    # The metric will be calculated from the channels Ch1 and Ch2.
                    
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_b:Ch1;metric_b:Ch2/10:3

        @apiExample Request one metric with extra parameters
                    # Request the metric 'metric_a' from the node 'example_node_one' with parameters 2.5 and 3.7,
                    # using a time window of length 3 seconds, starting 10 seconds ago from the present time.  
                    # The metric will be calculated from the channel Ch1
                    
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_a:Ch2:2.5:3.7/10:3

        @apiExample Request multiple metrics
                    # Request the metrics 'metric_b' and 'metric_c' from the node 'example_node_one' without parameters.
                    # using a time window of length 3 seconds, starting 10 seconds ago from the present time.  
                    
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_b:Ch2;metric_c:Ch1/10:3

        @apiExample Request multiple metrics metrics with and without parameters
                    # Request the metrics 'metric_a' and 'metric_b' from the node 'example_node_one' with parameters
                    # for metric_a and no parameters for metric_b,
                    # using a time window of length 3 seconds, starting 10 seconds ago from the present time.  
                    
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_a:Ch1:2.5:3.7;metric_b:Ch2/10:3

        @apiExample Specifying the time window
                    # Request the metric 'metric_a' from the node 'example_node_one' with parameters 1.3 and 2.9
                    # using all data from 7 seconds ago to the present time
                    
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_a:Ch2:1.3:2.9/7
                    
                    # This call is equivalent
                    curl -i http://midas_dispatcher:8080/example_node_one/metric/metric_a:Ch2:1.3:2.9/7:7




        @apiSuccessExample Success-Response:
        {
            "metric_a_Ch1_2.5_3.7":0.09886096999680793,
            "metric_b_Ch2":0.9123432571384757
        }
        """

        if node in self.node_addresses:
            socket_tmp = self.context.socket(zmq.REQ) 
            socket_tmp.connect(self.node_addresses[node]['address'])

            metric_names = mu.parse_metric(metric_names)
            request = metric_names + [str(time)]

            mu.midas_send_message(socket_tmp, 'metric', request)
            results = mu.midas_receive_reply(socket_tmp)

            return self.format_json(results)
        else:
            return self.format_json({'error' : 'node not available'})


    # ------------------------------------------------------------------------------

    def get_data(self, node, data_names, time):
        """
        @api {get} /:nodename/data/:channel_list/:time_window Request data
        @apiGroup Data
        @apiName GetData Request 
        @apiDescription Return data from the nodes.
        @apiParam {String} nodename The name of the node.

        @apiParam {String} channel_list The data channels to be
        returned. Multiple channels are separate by semicolons (;).

        @apiParam {Float} time_window The time window specifying the
        amount of data to be returned. The time window is composed of
        two numbers: the first number indicates the starting point of
        the data in seconds from the present time, and the second
        number indicates the length of data to be used in seconds. The
        parts are separated by a colon (:). If the colon and the
        second number are omitted, all data from the past n seconds
        are used, where n is the number given as the time window.

        @apiExample Request the past 3 seconds of data from channel Ch1 from the
                    node named 'example_node_one'
              
                    curl -i http://midas_dispatcher:8080/example_node_one/data/Ch1/3

        @apiExample Request data from channels Ch1 and Ch2 from node named
                    'example_node_one'. The data should be returned
                    for a 2-second time window starting 5 seconds ago.
              
                    curl -i http://midas_dispatcher:8080/example_node_one/data/Ch1;Ch2/3:1


        @apiSuccessExample Success-Response:
        {
            "Ch1":{
                "data":[
                    24.0,
                    25.0,
                    26.0,
                    27.0
                ],
                "time":[
                    0.008341878999999608,
                    0.006259979998503695,
                    0.004175565998593811,
                    0.002090279998810729
                ]
            },
            "Ch2":{
                "data":[
                    24.0,
                    25.0,
                    26.0,
                    27.0
                ],
                "time":[
                    0.008341878999999608,
                    0.006259979998503695,
                    0.004175565998593811,
                    0.002090279998810729
                ]
            }
        }
        """

        if node in self.node_addresses:
            socket_tmp = self.context.socket(zmq.REQ)
            socket_tmp.connect(self.node_addresses[node]['address'])

            data_names = mu.parse_metric(data_names)
            request = data_names + [str(time)]
            
            mu.midas_send_message(socket_tmp, 'data', request)
            data = mu.midas_receive_reply(socket_tmp)
            return self.format_json(data)
        else:
            return self.format_json({node: 'not available'})

    # ------------------------------------------------------------------------------
    # Test function
    # ------------------------------------------------------------------------------
    def get_test(self):
        """ Test function. """

        x = random.uniform(0,1)

        return self.format_json({'test' : x})

    # ------------------------------------------------------------------------------
    # Minimalist user interface for the node
    # ------------------------------------------------------------------------------
    def show_ui(self):
        """ Show a minimal user interface. """

        time.sleep(1)

        while self.run_state:
            tmp =  input(" > ")
            if tmp == "q":
                break
        self.stop()
    # ------------------------------------------------------------------------------
    # Start
    # ------------------------------------------------------------------------------
    def start(self):
        """ Starts running the dispatcher server. Routes are also configured
            here because they can not be formed until the object has been 
            initialized.
        """

        # control variable for threads
        self.run_state = True

        # start thread handling updating of nodes
        self.ud = threading.Thread(target = self.update_nodes)
        self.ud.start()

        # start thread running publisher-subscriber proxy
        if self.run_pubsub_proxy:
            self.psproxy_watchdog = threading.Thread(target = self.pubsub_proxy_watchdog)
            self.psproxy_watchdog.start()

        # Connect routes
        bottle.route('/',method = "GET")(self.root)
        bottle.route('/<node>/metric/<metric_names>/<time>', method = "GET")(self.get_metric)
        bottle.route('/<node>/data/<data_names>/<time>', method = "GET")(self.get_data)

        # Status request routes
        bottle.route('/status/nodes', method = "GET")(self.status_nodes) 
        bottle.route('/status/metrics', method = "GET")(self.status_metrics)
        bottle.route('/status/data', method = "GET")(self.status_data)
        bottle.route('/status/topics', method = "GET")(self.status_topics)
        bottle.route('/status/publisher', method = "GET")(self.status_publisher)

        # Node status request routes
        bottle.route('/<node>/status/metrics', method = "GET")(self.status_metrics)
        bottle.route('/<node>/status/data', method = "GET")(self.status_data)
        bottle.route('/<node>/status/topics', method = "GET")(self.status_topics)

        # 'On-demand' status request routes
        bottle.route('/<node>/status/info', method = "GET")(self.status_nodeinfo)

        # Test method
        bottle.route('/test', method = "GET")(self.get_test)

        # Thread for the UI
        self.ui = threading.Thread(target = self.show_ui)
        self.ui.start()

        # Start the web server
        app = bottle.default_app()
        waitress.serve(app,host = self.ip, port = self.port, threads = self.n_threads)

    # ------------------------------------------------------------------------------
    # Stop
    # ------------------------------------------------------------------------------
    def stop(self):
        print("Stopping dispatcher")
        self.run_state = False

        self.ud.join()

        if self.run_pubsub_proxy:
            self.psproxy_watchdog.join()

        os._exit(1)
# ------------------------------------------------------------------------------
# Run the dispatcher if started from the command line
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    dp = mu.midas_parse_config(MidasDispatcher, sys.argv)

    if dp is not None:
        dp.start()

# ------------------------------------------------------------------------------
