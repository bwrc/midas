MIDAS
=====

The MIDAS (Modular Integrated Distributed Analysis System) is a system
for online analysis of streaming signals and allows easy integration
of such into machine learning frameworks.

For more information (background, installation, usage, ...) refer to
the [MIDAS wiki](https://github.com/bwrc/midas/wiki).

This repository only contains the core library used to setup a MIDAS
network. For practical examples please check out the
[MIDAS-nodes](https://github.com/bwrc/midas-nodes/) repository.

Installation (pip3 if your system uses Python 2.7 by default)
------------
	pip install bottle PyZMQ Waitress pylsl
	pip install git+https://github.com/bwrc/midas

License information
-------------------
Please refer to the file "LICENSE" for license information for this
software, and for terms and conditions for usage, and a disclaimer of
all warranties.

MIDAS also depends on other software projects with different
licenses. These are listed on the [Licensing
page](https://github.com/bwrc/midas/wiki/Licensing) in the MIDAS wiki.
