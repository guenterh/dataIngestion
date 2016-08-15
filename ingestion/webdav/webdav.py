from ingestion.processor import BaseProcessor

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

class WebDav(BaseProcessor):
    def __init__(self, appConfig=None):
        BaseProcessor.__init__(self,appConfig)


    def collectItems(self):
        BaseProcessor.collectItems(self)
