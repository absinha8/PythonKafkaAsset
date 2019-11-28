from kafka import KafkaProducer
import json
import ConfigController
from datetime import datetime, timedelta
import traceback
import logging

class ProducerDS:
    def __init__(self):
        print("welcome to Producer class")
        objConfig = ConfigController.Configuration()
        self.btSer = objConfig.getConfiguration("Producer|bootstrap_servers")
        self.topic = objConfig.getConfiguration("Producer|topic")

        # SSL Entry added
        self.SSL_Autho = objConfig.getConfiguration("SSL|SSL_Authorization")
        self.ssl_cafile = str(objConfig.getConfiguration("SSL|ssl_cafile"))
        self.ssl_certfile = str(objConfig.getConfiguration("SSL|ssl_certfile"))
        self.ssl_keyfile = str(objConfig.getConfiguration("SSL|ssl_keyfile"))

    def fn_producer(self, obj, topic_for_produce):
        
        """ 
        This routine is intended to process the json object
        and send to the Kafka producer for the subsequent proessing in the data flow
        of the system.
        :param obj: the json object given for processing
        :return: None
        """ 
        try:

            D_timestamp = datetime(1970, 1, 1, 00)
            s_eventTimestamp = int(str(obj["eventTimestamp"]))
            delta_sec = timedelta(milliseconds=s_eventTimestamp)
            #print(delta_sec)
            nw_eventTimestamp= D_timestamp+delta_sec
            #print(nw_eventTimestamp)
            obj["eventTimestamp"] = str(nw_eventTimestamp)
            #topic = "exptopic1"



            finaljson = obj
            del finaljson["payload"]
            mylist = self.btSer.split(",")
            #topic = "demo_fund_event"
            #boot_ser = "[" + self.BT_Ser + "]"
            #print(self.BT_Ser)
            #print(self.topic)

            #print(self.SSL_Autho)
            #print(self.ssl_cafile)
            #print(self.ssl_certfile)
            #print(self.ssl_keyfile)

            if self.SSL_Autho != 'Y':
                print("SSL Disabled")
                #producer = KafkaProducer(bootstrap_servers=['virtualserver02.Scott-Neil-s-Account.cloud:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer = KafkaProducer(bootstrap_servers=mylist, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send(topic_for_produce, finaljson)
                producer.flush()
                producer.close(timeout=60)
                return 1
            else:
                print("SSL Enabled")
                producer = KafkaProducer(bootstrap_servers=mylist,
                                         security_protocol='SSL',
                                         ssl_check_hostname=True,
                                         ssl_cafile=self.ssl_cafile,
                                         ssl_certfile=self.ssl_certfile,
                                         ssl_keyfile=self.ssl_keyfile,
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send(topic_for_produce, finaljson)
                producer.flush()
                producer.close(timeout=60)
                return 1

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

            return 2

    def fn_producer_sup(self, obj, topic_for_sup):

        """
        This routine is intended to publish the json object into the suspension topic
        for the subsequent proessing in the data flow
        of the system.
        suspension is subject to consideration whether RI validation process is failed and raised the flag
        :param obj: the json object given for processing
        :return: None
        """
        try:

            # topic = "exptopic1"
            supjson = obj
            mylist = self.btSer.split(",")
            # topic = "demo_fund_event"
            # boot_ser = "[" + self.BT_Ser + "]"
            # print(self.BT_Ser)
            # print(self.topic)
            if self.SSL_Autho != 'Y':
                print("SSL Disabled")
                # producer = KafkaProducer(bootstrap_servers=['virtualserver02.Scott-Neil-s-Account.cloud:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer = KafkaProducer(bootstrap_servers=mylist, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send(topic_for_sup, supjson)
                producer.flush()
                producer.close(timeout=60)

                return 1
            else:
                print("SSL Enabled")
                producer = KafkaProducer(bootstrap_servers=mylist,
                                         security_protocol='SSL',
                                         ssl_check_hostname=True,
                                         ssl_cafile=self.ssl_cafile,
                                         ssl_certfile=self.ssl_certfile,
                                         ssl_keyfile=self.ssl_keyfile,
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send(topic_for_sup, supjson)
                producer.flush()
                producer.close(timeout=60)

                return 1

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

            return 2
    def fn_producer_excep(self, obj, topic_for_excep):

        """
        This routine is intended to publish the json object into the suspension topic
        for the subsequent proessing in the data flow
        of the system.
        suspension is subject to consideration whether RI validation process is failed and raised the flag
        :param obj: the json object given for processing
        :return: None
        """
        try:

            # topic = "exptopic1"
            excepjson = obj
            mylist = self.btSer.split(",")
            # topic = "demo_fund_event"
            # boot_ser = "[" + self.BT_Ser + "]"
            # print(self.BT_Ser)
            # print(self.topic)
            if self.SSL_Autho != 'Y':
                print("SSL Disabled")
                #producer = KafkaProducer(bootstrap_servers=['virtualserver02.Scott-Neil-s-Account.cloud:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer = KafkaProducer(bootstrap_servers=mylist, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send(topic_for_excep, excepjson)
                producer.flush()
                producer.close(timeout=60)

                return 1
            else:
                print("SSL Enabled")
                producer = KafkaProducer(bootstrap_servers=mylist,
                                         security_protocol='SSL',
                                         ssl_check_hostname=True,
                                         ssl_cafile=self.ssl_cafile,
                                         ssl_certfile=self.ssl_certfile,
                                         ssl_keyfile=self.ssl_keyfile,
                                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))
                producer.send(topic_for_excep, excepjson)
                producer.flush()
                producer.close(timeout=60)

                return 1

        except Exception as e:
            print(e)
            logging.error(traceback.format_exc())
            # Logs the error appropriately.

            return 2
