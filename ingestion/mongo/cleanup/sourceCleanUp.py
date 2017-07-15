import re, zlib

class DefaultCleanUp:
    def __init__(self, config):
        self.config = config
        self.currentRecordField = self.config['DB']['collection']['docfield']

        self.regexRecordBody = re.compile(self.config['Processing']['recordBodyRegEx'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.regexIdentifier = re.compile(self.config['Processing']['identifierRegEx'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)
        self.regexEventTime = re.compile(self.config['Processing']['eventTimeRegEx'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)

        self.regExMFBeingReplaced = re.compile(self.config['Processing']['mfCompatibleBeingReplaced'],
                   re.UNICODE | re.DOTALL | re.IGNORECASE)

        self.regExMarcStartTag = re.compile('marc:',
                   re.UNICODE | re.DOTALL | re.IGNORECASE)

        #self.replacement = '<marc:record type="Bibliographic" xmlns:marc="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">'
        self.replacement = '<marc:record type="Bibliographic">'


    def cleanUp(self, doc):

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
                contentSingleRecord = re.sub("<marc:record.*?>", self.replacement, body)
                tLine = re.sub('marc:', repl='', string=contentSingleRecord)

                return {'doc': tLine, 'key' : identifier, 'eventTime' : eventTime }
            else:
                # todo: implement decent logging framework
                print("record \n {RECORD} \n does not match regex".format(RECORD=textDoc))
        else:
            print("record  {ID} deleted".format(ID=doc['_id']))

class NebisCleanUp(DefaultCleanUp):


    def cleanUp(self, doc):
        status = doc['status']
        if not status == "deleted" and not status == "newdeleted":
            textDoc = zlib.decompress(doc[self.currentRecordField]).decode("utf-8")

            sIdentifier = self.regexIdentifier.search(textDoc)
            sBody = self.regexRecordBody.search(textDoc)
            if sBody and sIdentifier :
                body = ' '.join(sBody.group(1).splitlines())
                identifier = sIdentifier.group(1)
                contentSingleRecord = re.sub("<marc:record.*?>", self.replacement, body)
                tLine = re.sub('marc:', repl='', string=contentSingleRecord)

                return {'doc': tLine, 'key' : identifier, 'eventTime' : None }
            else:
                print("record \n {RECORD} \n does not match regex".format(RECORD=textDoc))
        else:
            print("record  {ID} deleted".format(ID=doc['_id']))
