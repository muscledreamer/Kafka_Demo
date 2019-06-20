# -*- coding: utf-8 -*-
# @Email: jqian_bo@163.com
# @Author: JingQian Bo
# @Create Time: 2019/6/20-9:22 PM


class KafkaTaskException(Exception):
    """ Base Exception for KafkaTask
    _ParameterError_Pull: You can see this Friendly Tips when get msg error.
    """

    _ParameterError_Pull = "Get kafka msg Error"

    pass


class ParameterError(KafkaTaskException):
    """ Error for Parameter Exception
    _ParameterError_Key: You can see this Friendly Tips when error type for key.
    _ParameterError_Topic: You can see this Friendly Tips when error type for topic.
    _ParameterError_Consumer: You can see this Friendly Tips when use error Parameter to init init_consumer.
    """
    _ParameterError_Key = "ParameterError- type(s) for 'Key': must be bytes"
    _ParameterError_Topic = "ParameterError- type(s) for 'topic': must be bytes"
    _ParameterError_pykafka_Consumer = "ParameterError- type(s) for 'topic': must be bytes; " \
                                       "type(s) for 'group_id': must be str; 'offset_type' " \
                                       "must be 'LATEST' or 'EARLIEST'"
    _ParameterError_kafka_Consumer = "ParameterError- type(s) for 'topic': must be str; " \
                                     "type(s) for 'group_id': must be str; 'offset_type' " \
                                     "must be 'LATEST' or 'EARLIEST'"

    pass
