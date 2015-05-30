1.0.4 (30.05.2015)
==================
* API for metric and data requests changed to JSON-format
* Automatic discovery of nodes optimized
* Configuration requirements of nodes relaxed
* Metrics from secondary nodes can now be requested without specifying data

1.0.3 (30.09.2014)
==================
* Removed generate_metric_list-call from nodes and moved it to BaseNode
* Improved current examples
* Added metric_functions initialization to BaseNode
* Added secondary data to midas_node_example_A
* Added separate locks and buffer sizes for secondary channels.
* Added handling functions for secondary data. Both single samples and chunks can now be pushed into secondary data channels.
* Fixed a bug where metrics and data replies were JSON.dumped twice, resulting in malformed return values.

1.0.2 (22.09.2014)
==================
* Fixed bug that crashed responders with badly formatted HTML-queries
* Non-existing channels can no longer be requested
* Fixed the syntax of example code

1.0.1 (12.09.2014)
==================
* Fixed bug that prevented data from multiple channels to be used.
* Changed the input format of data to metric functions. The data is now a dictionary, that contains the keys "data" (an array of arrays, one for each channel) and "names" (an array containing the channel names).

1.0.0 (13.08.2014)
==================
* First release
