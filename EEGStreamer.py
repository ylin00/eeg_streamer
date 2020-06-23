# -*- coding: utf-8 -*-
"""
EEG Streamer
------------
Streaming EEG raw signal to remote Kafka server.

Author:
    Yanxian Lin, Insight Health Data Science Fellow, Boston 2020
"""

import argparse
import ast
from termcolor import colored

from confluent_kafka import Producer, Consumer, KafkaError
from time import time, sleep
from datetime import datetime
import numpy as np
import configparser

VERSION = '1.0.1'


class EEGStreamer:
    """EEG Streamer

    Streaming EEG signal to Kafka.

    """
    def __init__(self):
        # parse arguments
        args = self.parse_args()
        self.__verbose = args.verbose
        self.__infile = args.infile
        # parse config
        config = configparser.ConfigParser()
        config.read(args.config)
        KALFK_BROKER_ADDRESS = config['DEFAULT']['KALFK_BROKER_ADDRESS']
        STREAMER_TOPIC = config['DEFAULT']['STREAMER_TOPIC']
        CONSUMER_TOPIC = config['DEFAULT']['CONSUMER_TOPIC']

        if args.id is not None:
            self.streamer_id = args.id.replace(' ', '_')
        else:
            self.streamer_id = config['STREAMER']['STREAMER_ID'].replace(' ', '_')
            """ID of the streamer"""

        # Initialize a Kafka Producer and a consumer
        self.producer = Producer({'bootstrap.servers': KALFK_BROKER_ADDRESS})
        """producer that produce stream of EEG signal"""

        self.producer_topic = STREAMER_TOPIC
        """producer_topic of streaming"""

        self.consumer = Consumer({
            'bootstrap.servers': KALFK_BROKER_ADDRESS,
            'auto.offset.reset': 'earliest',
            'group.id': self.streamer_id,
            'client.id': 'client',
            'enable.auto.commit': True,
            'session.timeout.ms': 6000
        })
        """consumer that subscribe the predicted results. Detailed configs 
        can be found https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1
        """

        self.consumer_topic = CONSUMER_TOPIC
        """topic for consumer"""

        # Streaming related configs
        self.max_stream_duration = 10000
        """streaming duration in second"""
        self.streaming_rate = 256
        """Streaming rate in Hz"""
        self.delay_refresh_intv = 1.0  # time in second
        """refresh interval in seconds"""
        self.flush_interval = 1
        """Flush interval for Producer. In second"""

        # Listening related configs
        self.listen_interval = 1
        """Interval in second of listening"""

        # Data related configs
        self.sampling_rate = 256
        """samping rate in Hz"""
        self.nchannel = 8
        """number of channels"""
        self.montage = '1020'
        """standard 10-20 montage"""

    def start(self):
        """Start streaming.
        """
        # Print a table header
        print('Time \t \t Patient \t Event ')

        self.consumer.subscribe([self.consumer_topic])

        # read in txt files, fake a stream data
        ds = np.tile(np.loadtxt(self.__infile, delimiter=','), [1, 10])
        montage = self.montage

        start_time = time()
        heart_beat = time()
        stream_delay = 0.8 / self.streaming_rate
        stream_count = 1

        for istream in range(0, ds.shape[1]):

            # Produce signal as stream
            joint_str = ','.join(['%.6f' % float(ds[ich, istream]) for ich in
                                  range(0, np.shape(ds)[0])])
            timestamp = time()
            value = "{'t':%.6f,'v':[" % float(timestamp) + joint_str + "]}"
            self.producer.produce(self.producer_topic, key=montage, value=value)

            # Flush after given interval
            intv = int(self.flush_interval * self.streaming_rate)
            if stream_count % max(intv, 1) == 0:
                self.producer.flush(1)

            # Listen after given interval
            intv = int(self.listen_interval * self.streaming_rate)
            if (stream_count) % max(intv, 1) == 0:
                self.listen()

            stream_delay, stream_count, heart_beat = self.sleep_and_sync(
                stream_delay, stream_count, heart_beat)

            # too long, shut down
            if time() - start_time > self.max_stream_duration:
                break

    def listen(self):
        """Listen to remote prediction result

        Returns:
            int: 1=alert. 0=background. 2=not sync.
        """
        # if listen == 1:
        #     print(timestamp, "!!!!!!SEIZURE IS COMING!!!!!!")
        # elif listen == 2:
        #     print(timestamp, "not sync'ed. check network connection.")
        # else:
        #     print(timestamp, "all good")
        msg = self.consumer.poll(0.1)
        if msg is None:
            pass
        elif not msg.error():
            print(f"Received message: key={msg.key().decode('utf-8')} val={msg.value()}") if self.__verbose else None
            if msg.key().decode('utf-8') != 'key':
                return None
            # TODO: if msg.key() == XXX
            t, v = self.decode(msg.key(), msg.value())
            t = datetime.fromtimestamp(int(t)).time().strftime('%I:%M:%S %p')
            # TODO: Check time stamp
            # print(msg.key().decode('utf-8'))
            if v[0] == 'pres':
                print(t, "\t", self.streamer_id, "\t", colored("!!seizure in 10~15 min!!", 'red'))
            elif v[0] == 'bckg':
                print(t, "\t", self.streamer_id, "\t", "looks all good")
            else:
                print(t, f'UNKNOWN: {v[0]} not recognized')
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print('End of partition reached {0}/{1}' \
                  .format(msg.topic(), msg.partition())) if self.__verbose else None
        else:
            print('Error occured: {0}'.format(msg.error().str())) if self.__verbose else None

    def stop(self):
        # consume the remaining msg and stop the consumer
        self.consumer.consume(num_messages=10)
        self.consumer.close()
        pass

    def parse_args(self):
        parser = argparse.ArgumentParser(description=self.__doc__ + '\n' + VERSION)
        parser.add_argument("config", help="config file")
        parser.add_argument("infile", help="data file to stream from")
        parser.add_argument("-id", "--id", help="Streamer identifier")
        parser.add_argument("-V", "--version", help="show program version",
                            action="version", version=VERSION)
        parser.add_argument("-v", "--verbose",
                            help="enable verbose mode",
                            action="store_true")
        # parser.set_defaults(verbose=False,
        #                     config='./config.ini',
        #                     infile='./data/svdemo-3bkg-3pre-3bkg-3pre.txt')
        return parser.parse_args()

    @staticmethod
    def decode(key, value):
        """decode a message key and value and return list"""
        # TODO: if key is invalid then return None
        mydata = ast.literal_eval(value.decode("UTF-8"))
        return mydata['t'], mydata['v']

    def sleep_and_sync(self, sampling_delay, sampling_count, heart_beat):
        """Sleep and adjust the sampling delay

        Returns: tuple(float, int, float)
            sampling_delay, sampling_count, heart_beat
        """
        # Adhere to sampling frequency
        sleep(sampling_delay)
        sampling_count += 1

        # Adjust the sleeping interval every refresh_delay_interval seconds
        if sampling_count == (self.delay_refresh_intv * self.sampling_rate):

            new_heartbeat = time()
            duration = new_heartbeat - heart_beat
            deviation = (self.delay_refresh_intv - duration) * 1000

            try:
                sampling_delay = sampling_delay + deviation / (
                        self.delay_refresh_intv * 1000) / self.sampling_rate * 0.5
                # 0.5 = dampening factor
                if sampling_delay < 0:
                    raise ValueError
            except ValueError:
                sampling_delay = 0
                print(
                    "WARNING: NEW DELAY TIME INTERVAL WAS A NEGATIVE NUMBER. Setting to 0..")
            print(f"Deviation: {deviation:.2f} ms, new delay:"
                  f" {sampling_delay * 1000:.2f} ms.") if self.__verbose else None
            sampling_count = 0
            heart_beat = new_heartbeat

        return sampling_delay, sampling_count, heart_beat


if __name__ == '__main__':
    eegstreamer = EEGStreamer()
    eegstreamer.start()
    eegstreamer.stop()
