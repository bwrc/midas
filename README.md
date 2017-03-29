![MIDAS logo](https://raw.githubusercontent.com/bwrc/midas/master/midas.png)

The MIDAS (Modular Integrated Distributed Analysis System) is a system
for online analysis of streaming signals and allows easy integration
of machine learning frameworks and processing modules. MIDAS aims to 
simplify the process of setting up of ad hoc networks for online 
processing and analysis of multiple signals. This is achieved through a 
distributed structure consisting of nodes and dispatchers. Core features 
of MIDAS include automatic discovery of network components and easy 
extensibility with custom nodes. Possible use cases include 
implementation of real-time signal fusion and machine learning. MIDAS is 
written in Python 3. 

![An example MIDAS network](https://raw.githubusercontent.com/bwrc/midas/master/flowchart.png) 

For more information (background, installation, usage, ...) refer to
the [MIDAS wiki](https://github.com/bwrc/midas/wiki).

This repository only contains the core library used to setup a MIDAS
network. For practical examples of processing nodes please check out the
[MIDAS-nodes](https://github.com/bwrc/midas-nodes/) repository.

Installation 
------------
MIDAS requires Python 3. Python 3 is your default, use the pip command and otherwise pip3 to ensure that Python 3 is used.

      pip install git+https://github.com/bwrc/midas

Quick start guide
-----------------
[Usage example](https://raw.githubusercontent.com/bwrc/midas/master/midas-usage.gif)

You can run the examples found in the [examples](https://github.com/bwrc/midas/tree/master/examples) by following these steps. The example consist of one signal source feeding data to two two processing nodes.
Please note that all commands should be run inside the examples directory.

The signal source produces a two-channel 1 Hz LSL-stream (default sensor-level interface of MIDAS) with linearly increasing values between 0 - 100. In other words the values outputted by the stream increase once every second and resets to zero every 100th second. You can start the stream by opening a terminal and typing:

	python stream_example.py

The two nodes receive data from the signal source and can perform various operations to the incoming data. Results of these operations can be requested through the dispatcher. To start the nodes open two terminals and type:

	python node_example_a.py config.ini node_a

and

	python node_example_b.py config.ini node_b

in separate terminals. 

In order to access the nodes you still need a dispatcher. Dispatcher is a simple webserver that handles commnucation between the user and nodes. The example dispatcher uses localhost and port 8080. To start the dispatcher open up a terminal and type:

	midas-dispatcher config.ini dispatcher

The example MIDAS-network should now be operational. You can test it by requesting the list of nodes

https://127.0.0.1:8080/status/nodes

To see the available metrics from each node use

https://127.0.0.1:8080/status/metrics

License information
-------------------
MIDAS is released under the MIT license, but depends on some components 
released under different licenses. Please refer to the file "LICENSE" for 
license information for this software, and for terms and conditions for
usage, and a disclaimer of
all warranties.

MIDAS also depends on other software projects with different
licenses. These are listed on the [Licensing
page](https://github.com/bwrc/midas/wiki/Licensing) in the MIDAS wiki.
