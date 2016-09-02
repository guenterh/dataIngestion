

__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2016, swissbib project"
__credits__ = []
__license__ = "??"
__version__ = "0.1"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """

                    """
from kafka import KafkaProducer

class BaseProcessor:

    def __init__(self,appConfig=None):
        self.appConfig = appConfig
        self.producer = KafkaProducer(bootstrap_servers=self.appConfig.getConfig()['Kafka']['host'])


    def collectItems(self):
        #standard baase method whithout implementation
        raise Exception("no implementation in default collectItems method")

    def getAppConfig(self):
        return self.appConfig

    def produceKafkaMessage(self, messageValue,key=None, eventTime=None):
        #todo
        #improved implementation for keys and partitions
        #how to use the event time?
        self.producer.send(self.appConfig.getConfig()['Kafka']['topicToUse'],value= str.encode(messageValue),
                           key=str.enco de(key))
