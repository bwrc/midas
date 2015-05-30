#!/usr/bin/env python3

import sys
import numpy as np
import scipy.signal
from midas.node import BaseNode
from midas import utilities as mu


# -----------------------------------------------------------------------------
# Create an Example Node B based on the Base Node
# -----------------------------------------------------------------------------
class NodeExampleB(BaseNode):
    """ MIDAS example node B. """

    def __init__(self, *args):
        """ Initialize example node B. """
        super().__init__(*args)
        # Specify all metric-functions by adding them to the
        # metric_functions-list. This makes them visible to dispatcher.
        self.metric_functions.append(self.metric_c)
        self.metric_functions.append(metric_d)

    # A more complex example of a metric function. This metric takes data and
    # calculates the average power on frequencies between f_lim1 and f_lim2.
    def metric_c(self, x, f_lim1=10, f_lim2=20):
        """ Calculate spectral average of input signal in band f_lim1 - f_lim2.
        Args:
            x <dict>: input data
            f_lim1 <float>: lower frequency band limit
            f_lim2 <float>: upper frequency band limit
        Returns:
            c <float>: average power in frequency band f_lim1 - f_lim2
        """
        f, P = scipy.signal.welch(x['data'][0], fs=self.sampling_rate)
        c = np.mean(P[np.bitwise_and(f >= f_lim1, f <= f_lim2)])
        return c


# Metrics can handle multiple channels as well. If multiple channels are passed
# to the metric they appear as a list under 'data' key in the input dict. This
# function iterates through all the channels in the input dict.
def metric_d(x):
    """ Normalizes the last sample of each channel given as input."""
    d = []
    for ch in x['data']:
        d.append(ch[-1] / np.max(ch))
    return d


# -----------------------------------------------------------------------------
# Run the node if started from the command line
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    node = mu.midas_parse_config(NodeExampleB, sys.argv)
    if node is not None:
        node.start()
        node.show_ui()
# -----------------------------------------------------------------------------
# EOF
# -----------------------------------------------------------------------------
