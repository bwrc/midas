#!/usr/bin/env python3

import time
from midas.node import lsl

# Starts up a two channel "dummy" LSL stream that claims to be 500 Hz sampled
# EEG.

# Set sampling rate and sample interval
fs = 1.0
ts = 1.0 / fs

# Create a LSL stream named 'Dummy' with 2 channels of random data
N = 2
stream_eeg_info = lsl.StreamInfo('Dummy', 'EEG', N, fs, 'float32', 'uid001')
stream_eeg_outlet = lsl.StreamOutlet(stream_eeg_info, max_buffered=1)

# Start streaming
print('Streaming random data ...')
i = 0
while True:
    data = [(i % 100)] * N
    stream_eeg_outlet.push_sample(data)
    time.sleep(ts)
    i += 1
