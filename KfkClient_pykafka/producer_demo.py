# -*- coding: utf-8 -*-
# @Email: jqian_bo@163.com
# @Author: JingQian Bo
# @Create Time: 2018/9/27-下午3:46
import time
from kafka_client import KafkaTask
from conf import demo_Sever, demo_Topic, demo_Key

# pykafka发送数据
kafka_client = KafkaTask(server=demo_Sever)
kafka_producer = kafka_client.init_producer(topic=demo_Topic)
for i in range(10):
    msg = 'msg-->' + str(i)
    kafka_client.send_message(msg, key=demo_Key)
    print("producer-key:{} ,发送值:{}".format(demo_Key, msg))
    time.sleep(1)
