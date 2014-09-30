#!/usr/bin/env python3

import sys
import inspect
import random
import time

from midas.node import BaseNode
from midas import pylsl_python3 as lsl
from midas import utilities as mu


# ------------------------------------------------------------------------------
# Create an Example Node based on the Base Node
# ------------------------------------------------------------------------------
class MidasNodeExample(BaseNode):

    def __init__(self, *args):
        """ Initialize example node. """
        super().__init__(*args)

        # Generate dict for metric descriptions and function pointers
        # these are saved to metric_list and metric_functions dicts
        self.metric_functions.append(metric_a)
        self.metric_functions.append(self.metric_b)
        self.metric_functions.append(metric_c)
        
        self.process_list.append(self.process_x)

        # generate topic list
        self.topic_list = {'topic_one' : 'a test topic',
                           'topic_two' : 'another topic'}

    # Define an analysis function as a class method so that it also
    # can access the attributes of the class, which is needed in order
    # to send publish messages
    def metric_b(self, x):
        """ Returns the metric B calculated from the data. """

        x = random.random()

        print('sending message')
        self.message_queue.put('A calculating metric_b')

        return(x)

    def process_x(self):
        """ Automatically calculates values for two secondary channels. """
        
        # Run while node is running
        while self.run_state.value:
            # Push sample to 1st secondary data channel
            self.push_sample_secondary(0,lsl.local_clock(),random.random())
            # Push sample to 2nd secondary data channel
            self.push_sample_secondary(1,lsl.local_clock(),random.random())
            time.sleep(5)

# ------------------------------------------------------------------------------
# Define some analysis functions. These could be in a separate module.
# ------------------------------------------------------------------------------

def metric_a(x, bstart, bstop):
    """ Returns the metric A calculated from the data. The function takes the start and stop frequencies as parameters: metric_a(band_start, band_stop)."""
    x = random.random()
    return(x)

def metric_c(x):
    """ Returns the metric C calculated from the data. """
    x = random.random()
    return(x)

# ------------------------------------------------------------------------------
# Run the node if started from the command line
# ------------------------------------------------------------------------------
if __name__ == '__main__':
    node = mu.midas_parse_config(MidasNodeExample, sys.argv)

    if node is not None:
        node.start()
        node.show_ui()
# ------------------------------------------------------------------------------
# EOF
# ------------------------------------------------------------------------------
