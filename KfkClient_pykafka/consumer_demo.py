# -*- coding: utf-8 -*-
# @Email: jqian_bo@163.com
# @Author: JingQian Bo
# @Create Time: 2018/9/27-下午3:46
import time
from exceptions import KafkaTaskException
from kafka_client import KafkaTask
from conf import demo_Sever, demo_Topic, demo_GroupID


# pykafka接收数据
def get_data_by_pykafka():
    kafka_client = KafkaTask(server=demo_Sever)
    kafka_client.init_consumer_pykafka(topic=demo_Topic, group_id=demo_GroupID,
                                       offset_type="EARLIEST")  # 'offset_type' must be 'LATEST' or 'EARLIEST'
    for msg in kafka_client.consumer:
        print("Offset:{}, Partition-Key:{}, Copy:{}".format(msg.offset, msg.partition_key, msg.value))


# kafka-python接收数据
def get_data_by_kafka():
    kafka_client = KafkaTask(server=demo_Sever)
    kafka_client.init_consumer_kafka(topic=demo_Topic.decode(), group_id=demo_GroupID,
                                     offset_type="EARLIEST")  # 'offset_type' must be 'LATEST' or 'EARLIEST'
    while True:
        try:
            msgs = kafka_client.pull()
        except KafkaTaskException as e:
            print(e)
            kafka_client.init_consumer_pykafka(topic=demo_Topic.decode(), group_id=demo_GroupID,
                                               offset_type="EARLIEST")  # 'offset_type' must be 'LATEST' or 'EARLIEST'
            time.sleep(1)  # 客户端容错
            continue
        if msgs is None or msgs == {}:  # 接收数据容错
            time.sleep(0.01)
            continue
        for k in msgs:
            msg = msgs[k][0]
            print(msg.topic, msg.partition, msg.offset, msg.value.decode())
            time.sleep(0.2)


if __name__ == '__main__':
    # get_data_by_pykafka()
    get_data_by_kafka()
