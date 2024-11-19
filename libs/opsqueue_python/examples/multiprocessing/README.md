This example shows how to start a cluster of Python programs using `multiprocessing`, with each of them running their own consumer client.

There are situations in which it is useful to run Python workloads this way, but besides this it is a very easy way to start a large number of consumer processes locally to see how the queue behaves and do load testing when there is a large consumer cluster.
