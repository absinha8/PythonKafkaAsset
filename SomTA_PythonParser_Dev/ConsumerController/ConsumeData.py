from kafka import KafkaConsumer
import json
import ConfigController
import traceback
import logging
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

# For JsonLogMsg
import LoggingController
import inspect
from inspect import currentframe, getframeinfo

class ConsumeData:

    def __init__(self):
        
        """ This routine is intended to
            Two variables bootstrap_servers & topic
            defined in YML config file will be stored in
            btSer & topic variable
        """
        
        #print("welcome to Consumer class")
        objConfig = ConfigController.Configuration()
        self.bootSer = objConfig.getConfiguration("Consumer|bootstrap_servers")
        self.group_id = objConfig.getConfiguration("Consumer|group_id")
        #SSL Entry added
        self.SSL_Autho = objConfig.getConfiguration("SSL|SSL_Authorization")
        self.ssl_cafile = str(objConfig.getConfiguration("SSL|ssl_cafile"))
        self.ssl_certfile = str(objConfig.getConfiguration("SSL|ssl_certfile"))
        self.ssl_keyfile = str(objConfig.getConfiguration("SSL|ssl_keyfile"))

        # For JsonLogMsg
        self.objLog = LoggingController.LogObject()
        self.logger = self.objLog.get_logger()

    def tryconvert(self, msg):
        json_msg = ''
        try:
            json_msg = json.loads(msg.decode('ascii'), strict=False)

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.
        return json_msg

    def fn_consume(self, topic_for_consume):
        
        """This routine is intended to
            Create the consumer object
            :param btSer: bootstrap_servers value
            :param topic: topic name
            :return: Consumer object
        """
        try:
            #print(self.btSer)
            #boot_ser = "['" + self.btSer + "']"
            topic = topic_for_consume
            #groupId = topic_for_consume + '_group'
            groupId = self.group_id
            mylist = self.bootSer.split(",")
            #print(type(mylist))
            #print(groupId)
            #print(self.bootSer)
            #print(topic)

            #print(self.SSL_Autho)
            #print(self.ssl_cafile)
            #print(self.ssl_certfile)
            #print(self.ssl_keyfile)

            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            filename = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Consumer started polling from Topic = " + topic
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, clas))

            if self.SSL_Autho != 'Y':
                print("SSL Disabled")
                print(topic)
                consumer = KafkaConsumer(topic,
                                         bootstrap_servers=mylist,
                                         group_id=groupId,
                                         enable_auto_commit=True,
                                         auto_offset_reset='earliest',
                                         max_poll_records=20,
                                         session_timeout_ms=100000,
                                         request_timeout_ms=150000,
                                         value_deserializer=lambda m: self.tryconvert(m))
                # dummy poll
                consumer.poll()

                # For JsonLogMsg
                log_message = "Consumer polled the message from Topic = " + topic
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, clas))

                # go to end of the stream
                # consumer.seek_to_end()
                # start iterate
                return consumer
            else:
                print("SSL Enabled")
                print(topic)
                consumer = KafkaConsumer(topic,
                                         bootstrap_servers=mylist,
                                         group_id=groupId,
                                         enable_auto_commit=True,
                                         auto_offset_reset='earliest',
                                         max_poll_records=20,
                                         session_timeout_ms=100000,
                                         request_timeout_ms=150000,
                                         security_protocol='SSL',
                                         ssl_check_hostname=True,
                                         ssl_cafile=self.ssl_cafile,
                                         ssl_certfile=self.ssl_certfile,
                                         ssl_keyfile=self.ssl_keyfile,
                                         value_deserializer=lambda m: self.tryconvert(m))

                # dummy poll
                consumer.poll()

                # For JsonLogMsg
                log_message = "Consumer polled the message from Topic = " + topic
                self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, clas))

                # go to end of the stream
                #consumer.seek_to_end()
                # start iterate
                return consumer

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.


    def fn_CommitMessage(self,consumer,message):

        """This routine is intended to
            Commit the consumer message from kafka
            :param consumer: Consumer object
            :param message: Message object of the topic
            :return: None
        """
        try:

            meta = consumer.partitions_for_topic(message.topic)
            partition = TopicPartition(message.topic, message.partition)
            offsets = OffsetAndMetadata(message.offset + 1, meta)
            options = {partition: offsets}
            print(options)
            consumer.commit(offsets=options)

            # For JsonLogMsg
            cf = currentframe()
            module = cf.f_code.co_name
            filename = cf.f_code.co_filename
            clas = self.__class__
            log_message = "Consumer committed the message for " + str(options)
            self.logger.info(self.objLog.fn_ProduceJsonLogMessage(None, None, log_message, module, cf.f_lineno, filename, clas))

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

        return None

