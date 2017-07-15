# coding: utf-8

"""
Kappa procedure for Mongo sources
bulk load of mongo data into Kafka cluster as basis for stream processing
"""


if __name__ == '__main__':

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
    from argparse import ArgumentParser
    from config.appConfig import MongoConfig
    #client could be one of these types
    from ingestion.oai.oai import OAI
    from ingestion.webdav.webdav import WebDav
    from ingestion.filePush.filepush import FilePush
    from ingestion.mongo.mongo import MongoSource


    oParser = ArgumentParser()
    oParser.add_argument("-c", "--config", dest="confFile")
    args = oParser.parse_args()

    appConfig = MongoConfig(args.confFile)

    client = MongoSource(appConfig)
    client.initialize()
    client.lookUpData()
    client.preProcessData()
    client.process()
    client.postProcessData()
