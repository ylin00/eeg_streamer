EEG Streamer
------------
Streaming EEG raw signal to remote Kafka server.

Author:
    Yanxian Lin, Insight Health Data Science Fellow, Boston 2020


Prerequisites
-------------

make sure you have started zookeeper server and kafka broker server
Step 1 and Step 2 in https://kafka.apache.org/quickstart

0. download kafka from apache.org, unzip using ```tar -xvf XXX```
1. run ```bin/zookeeper-server-start.sh config/zookeeper.properties```
2. run ```bin/kafka-server-start.sh config/server.properties```


Usage
-----
```Python3 EEGStreamer.py```
