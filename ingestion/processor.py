

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
from config.appConfig import AppConfig

class BaseProcessor:

    def __init__(self,appConfig : AppConfig =None):
        self.appConfig = appConfig
        self.producer = KafkaProducer(bootstrap_servers=self.appConfig.getConfig()['Kafka']['host'])



    # =====================================
    # initialization work can be done
    # =====================================
    def initialize(self):
        pass

    # =====================================
    # sometimes we have to lookup for new data
    # the mechanisms may vary from data source to data source
    # =====================================
    def lookUpData(self):
        pass


    # =====================================
    # data could be preprocessed
    # in times of using en event hub this could be deprecated
    # =====================================
    def preProcessData(self):
        pass


    # =====================================
    # now process the data
    # =====================================
    def process(self):
        pass

    # =====================================
    # post processing work could be done
    # =====================================
    def postProcessData(self):
        pass


    def getAppConfig(self):
        return self.appConfig

    def produceKafkaMessage(self, messageValue,key=None, eventTime=None):
        #todo
        #improved implementation for keys and partitions
        #how to use the event time?
        self.producer.send(self.appConfig.getConfig()['Kafka']['topicToUse'],value= str.encode(messageValue),
                           key=str.encode(key))
