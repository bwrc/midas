#!/usr/bin/env python3

import sys

from midas import utilities as mu
from midas.dispatcher import Dispatcher

# ------------------------------------------------------------------------------
# Run the dispatcher if started from the command line
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    dp = mu.midas_parse_config(Dispatcher, sys.argv)
    if dp is not None:
        dp.start()
# ------------------------------------------------------------------------------
# EOF
# ------------------------------------------------------------------------------
