from sickle import Sickle
from ingestion.processor import BaseProcessor
from sickle.oaiexceptions import OAIError
import re

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


class OAI(BaseProcessor):

    def __init__(self, appConfig=None):
        BaseProcessor.__init__(self,appConfig)
        self.recordBodyRegEx = re.compile(self.appConfig.getConfig()['Processing']['recordBodyRegEx'],
                                                    re.UNICODE | re.DOTALL | re.IGNORECASE)


    def collectItems(self):

        sickle = Sickle(self.appConfig.getConfig()['OAI']['url'])
        dic = {
            'metadataPrefix': self.appConfig.getConfig()['OAI']['metadataPrefix'],
            'setSpec': self.appConfig.getConfig()['OAI']['setSpec']
        }

        if not self.appConfig.getConfig()['OAI']['from'] is None:
            dic['from'] = '{:%Y-%m-%dT%H:%M:%SZ}'.format(self.appConfig.getConfig()['OAI']['from'])

        if not self.appConfig.getConfig()['OAI']['until'] is None:
            dic['until'] = '{:%Y-%m-%dT%H:%M:%SZ}'.format(self.appConfig.getConfig()['OAI']['until'])


        try:

            recordsIt = sickle.ListRecords(
                **dic
            )

            for record in recordsIt:
                #print(record.header.identifier)
                #print(record.header.datestamp)
                if record.header.deleted:
                    #at the moment in time I don't want to use deleted items in Kafka
                    continue
                sBody = self.recordBodyRegEx.search(record.raw)
                if sBody:
                    body = sBody.group(1)
                    #todo
                    #key should not contain the ID of the OAI identifier (sysID of library system
                    #otherwise partitions for single networks won't be stable
                    self.produceKafkaMessage(body,
                                             key=record.header.identifier,
                                             eventTime=record.header.datestamp)
                else:
                    raise Exception("we havent't found the body which should not be the case")

        except OAIError as oaiError:
            print(oaiError)
        except Exception as baseException:
            print(baseException)



