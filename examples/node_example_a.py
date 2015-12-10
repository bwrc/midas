#!/usr/bin/env python3

import sys
import time
import numpy as np
from midas.node import BaseNode, lsl
from midas import utilities as mu


# ------------------------------------------------------------------------------
# Create an Example Node A based on the Base Node
# ------------------------------------------------------------------------------
class NodeExampleA(BaseNode):
    """ MIDAS example node A. """

    def __init__(self, *args):
        """ Initialize example node. """
        super().__init__(*args)
        # Specify all metric-functions by adding them to the
        # metric_functions-list. This makes them visible to dispatcher.
        self.metric_functions.append(self.metric_a)
        self.metric_functions.append(self.test)
        self.metric_functions.append(metric_b)
        self.process_list.append(self.process_x)

    # Metric function can be defined as class methods so that they can
    # access the class attributes. This enables some additional functionality.
    def metric_a(self, x):
        """ Returns the mean of the input vector calculated from the data. """
        a = np.mean(x['data'][0])
        return a

    def test(self, x, p1=0, p2=0):
        """ Testing function that echoes inputs it gets"""
        print('>>>>>>>>>>>')
        print("\tNumber of channels=%d" % len(x['data']))
        for idx, ch in enumerate(x['data']):
            print("\t\tCh%d: %d samples" % (idx, len(ch)))
        print("\tArguments:")
        print("\t\targ1=%s" % p1)
        print("\t\targ2=%s" % p2)
        print("\tData:")
        for d in x['data']:
            print("\t\t" + str(d))
        print("\tTime:")
        for t in x['time']:
            print("\t\t" + str(t))
        return 1

    # Processes are class methods that loop while the node is running. A process
    # can be used to calculate and push new values into secondary data channels.
    def process_x(self):
        """ Automatically calculates values for two secondary channels. """
        # Process loops while the node is running. Variable run_state.value is
        # the poison-pill of the node.
        channel_name = self.primary_channel_names[0]
        while self.run_state.value:
            # Snapshot and unpack 10 seconds of 'primary' data from channel 0
            snapshot = self.snapshot_data([channel_name])
            data, times = self.unpack_snapshot(snapshot,
                                               [channel_name],
                                               [10, 10])
            # Calculate mean and standard deviation of this 10 second chunk from
            # channel 0
            if data[0]:
                new_value1 = np.mean(data[0])
                new_value2 = np.std(data[0])
            else:
                new_value1 = 0.0
                new_value2 = 0.0
            # Obtain a time-stamp for new values
            time_stamp = lsl.local_clock()
            # Push mean to 1st secondary data channel
            self.push_sample_secondary(0, time_stamp, new_value1)
            # Push standard deviation to 2nd secondary data channel
            self.push_sample_secondary(1, time_stamp, new_value2)
            # Sleep until next sample (note: for accurate timing you don't want
            # to use time.sleep)
            time.sleep(1.0)


# Metric functions can also exist outside the class as long as they are added
# to the metric_functions-list in node __init__. These "outside" functions have,
# however, no access to class attributes. Note that it is also possible to
# include metric functions from a completely separate module.
def metric_b(x, arg1=0, arg2=0):
    """ Returns 'metric b'. Takes two additional arguments."""
    b1 = np.max(x['data'][0]) - arg1
    b2 = np.min(x['data'][0]) - arg2
    b = (b1, b2)
    return b


# ------------------------------------------------------------------------------
# Run the node if started from the command line
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    node = mu.midas_parse_config(NodeExampleA, sys.argv)
    if node is not None:
        node.start()
        node.show_ui()
# ------------------------------------------------------------------------------
# EOF
# ------------------------------------------------------------------------------
