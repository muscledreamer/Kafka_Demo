# -*- coding: utf-8 -*-
# @Email: jqian_bo@163.com
# @Author: JingQian Bo
# @Create Time: 2018/9/27-下午3:46
from pykafka import KafkaClient
from exceptions import *


class KafkaTask(object):
    """ A Function encapsulation client by pykafka """

    def __init__(self, server: str):
        self.server = server
        self.client = KafkaClient(hosts=server)

    def init_producer(self, topic: bytes):
        """Init Kafka Producer

            Args:
                topic: Type --> bytes `Produce msg what topic You want`.
            Raises:
                ParameterError: When type of topic isn't bytes.

            """
        if not isinstance(topic, bytes):
            raise ParameterError(ParameterError._ParameterError_Topic)
        topic = self.client.topics[topic]
        self.producer = topic.get_producer(sync=True)

    def init_consumer_pykafka(self, topic: bytes, group_id: str, offset_type: str):
        """Init Kafka Consumer by pykafka

            Args:
                topic: Type --> bytes `Consumer msg what topic You want`.

                group_id: Type --> str `Consumer msg what group_id You want,
                You can receive duplicate data using different group_id in same topic`.

                offset_type: Type --> str `LATEST can receive the latest data,
                EARLIEST can receive earliest data in topic`
            Raises:
                ParameterError: When use error Parameter to init init_consumer.

            """
        if not isinstance(topic, bytes) or not isinstance(group_id, str) \
                or not isinstance(offset_type, str) or offset_type not in ["LATEST", "EARLIEST"]:
            raise ParameterError(ParameterError._ParameterError_pykafka_Consumer)
        from pykafka.simpleconsumer import OffsetType
        _OffsetType = {"LATEST": OffsetType.LATEST, "EARLIEST": OffsetType.EARLIEST}
        topic = self.client.topics[topic]
        self.consumer = topic.get_simple_consumer(auto_commit_enable=True, auto_commit_interval_ms=1,
                                                  consumer_id=group_id, auto_offset_reset=_OffsetType[offset_type],
                                                  reset_offset_on_start=True)

    def init_consumer_kafka(self, topic: str, group_id: str, offset_type: str):
        """Init Kafka Consumer by kafka-python

            Args:
                topic: Type --> str `Consumer msg what topic You want`.

                group_id: Type --> str `Consumer msg what group_id You want,
                You can receive duplicate data using different group_id in same topic`.

                offset_type: Type --> str `LATEST can receive the latest data,
                EARLIEST can receive earliest data in topic`
            Raises:
                ParameterError: When use error Parameter to init init_consumer.

            """
        from kafka import KafkaConsumer
        if not isinstance(topic, str) or not isinstance(group_id, str) \
                or not isinstance(offset_type, str) or offset_type not in ["LATEST", "EARLIEST"]:
            raise ParameterError(ParameterError._ParameterError_kafka_Consumer)

        self.consumer = KafkaConsumer(bootstrap_servers=self.server,
                                      auto_offset_reset=offset_type,
                                      group_id=group_id,
                                      )
        self.consumer.subscribe(topics=topic.split(','))

    def pull(self):
        """Get info from Kafka Consumer by kafka-python

            Poll Parameters:
                timeout_ms: Type --> int `Interval between each piece of data`.

                max_records: Type --> int `The amount of data per batch of data`

        """

        return self.consumer.poll(timeout_ms=0, max_records=1)

    def send_message(self, msg, *, key: bytes = ""):
        """ Send MSG by this func
            Args:
                msg: Type --> bytes or str `Message to broker`.

                key: Type --> bytes `Producer msg what key You want,
                You can see the key when you receiving data`.
            Raises:
                ParameterError: when error type for key.

        """
        if not isinstance(key, bytes):
            raise ParameterError(ParameterError._ParameterError_Key)
        produce_msg = msg.encode() if isinstance(msg, str) else msg
        self.producer.produce(produce_msg, partition_key=key)
