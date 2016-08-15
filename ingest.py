# coding: utf-8

"""
bootstrap mechanism to start the various ingestion types
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
    from config.appConfig import AppConfig

    from ingestion.oai.oai import OAI
    from ingestion.webdav.webdav import WebDav
    from ingestion.filePush.filepush import FilePush


    oParser = ArgumentParser()
    oParser.add_argument("-c", "--config", dest="confFile")
    args = oParser.parse_args()


    appConfig = AppConfig(args.confFile)
    client = globals()[appConfig.getProcessor()](appConfig)
    client.collectItems()


