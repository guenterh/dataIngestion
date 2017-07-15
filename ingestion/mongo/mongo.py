from ingestion.processor import BaseProcessor
from config.appConfig import AppConfig
import re, zlib, sys
from pymongo import MongoClient

__author__ = 'swissbib - UB Basel, Switzerland, Guenter Hipler'
__copyright__ = "Copyright 2016, swissbib project"
__credits__ = []
__license__ = "??"
__version__ = "0.1"
__maintainer__ = "Guenter Hipler"
__email__ = "guenter.hipler@unibas.ch"
__status__ = "in development"
__description__ = """
    kafka producer to load data from Mongo Collections (Kappa - procedure)
"""



class MongoSource(BaseProcessor):

    def __init__(self, appConfig : AppConfig = None):
        BaseProcessor.__init__(self,appConfig)


    def initialize(self):
        self.mongoClient = MongoClientWrapper(self.appConfig)
        self.currentRecordField = self.appConfig.getConfig()['DB']['collection']['docfield']

        self.regexRecordBody = re.compile(self.appConfig.getConfig()['Processing']['recordBodyRegEx'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.regexIdentifier = re.compile(self.appConfig.getConfig()['Processing']['identifierRegEx'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.regexEventTime = re.compile(self.appConfig.getConfig()['Processing']['eventTimeRegEx'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)

        self.regExMFBeingReplaced = re.compile(self.appConfig.getConfig()['Processing']['mfCompatibleBeingReplaced'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)

        self.regExMarcStartTag = re.compile('marc:',
                   re.UNICODE | re.DOTALL | re.IGNORECASE)

        #self.replacement = '<marc:record type="Bibliographic" xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">'
        self.replacement = '<marc:record type="Bibliographic">'

    def process(self):
        if not self.mongoClient is None:
            for doc in  self.mongoClient.getAllDocsInCollection():
                status = doc['status']
                if not status == "deleted" and not status == "newdeleted":
                    textDoc = zlib.decompress(doc[self.currentRecordField]).decode("utf-8")

                    sIdentifier = self.regexIdentifier.search(textDoc)
                    sEventtime = self.regexEventTime.search(textDoc)
                    sBody = self.regexRecordBody.search(textDoc)
                    if sBody and sIdentifier and sEventtime:

                        body = ' '.join(sBody.group(1).splitlines())
                        identifier = sIdentifier.group(1)
                        eventTime = sEventtime.group(1)
                        contentSingleRecord = re.sub("<marc:record.*?>", self.replacement,body )
                        #tLine = re.sub(pattern=self.regExMFBeingReplaced, repl=self.replacement,string=body)
                        tLine = re.sub('marc:', repl='',string=contentSingleRecord)
                        self.produceKafkaMessage(messageValue=tLine,
                                                 key=identifier,
                                                 eventTime=eventTime)
                    else:
                        #todo: implement decent logging framework
                        print("record \n {RECORD} \n does not match regex".format(RECORD=textDoc))



    def postProcessData(self):
        if not self.mongoClient is None:
            self.mongoClient.closeConnection()


class MongoClientWrapper:
    def __init__(self, appConfig : AppConfig = None):
        self.config = appConfig.getConfig()

        if not self.config['HOST']['user'] is None:
            uri = 'mongodb://{USER}:{PASSWORD}@{SERVER}:{PORT}/{DB}'.format(
                USER=self.config['HOST']['user'],
                PASSWORD=self.config['HOST']['password'],
                SERVER=self.config['HOST']['server'],
                PORT=self.config['HOST']['port'],
                DB=self.config['HOST']['authDB']
            )
        else:

            uri = 'mongodb://{SERVER}:{PORT}'.format(
                SERVER=self.config['HOST']['server'],
                PORT=self.config['HOST']['port'],
            )

        self.client = MongoClient( uri)
        self.database = self.client[self.config['DB']['dbname']]
        self.collection = self.database[self.config['DB']['collection']['name']]

    def getAllDocsInCollection(self):

        if not self.collection is None:
            for doc in  self.collection.find():
                yield doc

    def closeConnection(self):
        if not self.client is None:
            self.client.close()


