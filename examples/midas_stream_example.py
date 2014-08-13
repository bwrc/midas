#!/usr/bin/env python3
import time
import random

from midas import pylsl_python3 as lsl

# Set sampling rate
fs = 500.0
ts = 1.0 / fs

# Create a stream with 2 channels of random data
N = 2
stream_eeg_info = lsl.StreamInfo('Dummy', 'EEG', N, fs, 'float32', 'uid001')
stream_eeg_outlet = lsl.StreamOutlet(stream_eeg_info, max_buffered = 1)

print('Streaming random data ...')

i = 0

while True:
    data = [(i % 100)] * N
    stream_eeg_outlet.push_sample(data)

    time.sleep(ts)
    i += 1
