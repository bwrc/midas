#!/usr/bin/env python3
import sys
import inspect
import random

from midas.node import BaseNode
from midas import pylsl_python3 as lsl
from midas import utilities as mu

  
# ------------------------------------------------------------------------------
# Create an Example Node based on the Base Node
# ------------------------------------------------------------------------------
class MidasNodeExample(BaseNode):
    def __init__(self, *args):
        super().__init__(*args)

        # Generate dict for metric descriptions and function pointers
        # these are saved to metric_list and metric_functions dicts
        self.metric_functions = []

        self.metric_functions.append(metric_d)
        self.metric_functions.append(self.metric_e)
        self.generate_metric_lists()

    # Define an analysis function as a class method so that it also
    # can access the attributes of the class, which is needed in order
    # to send publish messages
    def metric_f(self, x):
        """ Returns the metric F calculated from the data. """

        x = random.random()

        print('sending message')
        self.message_queue.put('B calculating metric_e')
        
        return(x)

# ------------------------------------------------------------------------------
# Define some analysis functions. These could be in a separate module.
# ------------------------------------------------------------------------------
        
def metric_d(x, bstart, bstop):
    """ Returns the metric D calculated from the data. The function takes the start and stop frequencies as paramters: metric_a(band_start, band_stop)."""
    x = random.random()
    return(x)

def metric_e(x):
    """ Returns the metric E calculated from the data. """
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
