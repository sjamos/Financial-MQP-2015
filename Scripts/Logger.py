import logging

def getLogger(name=None):
    logger = logging.getLogger(name)
    handler = logging.FileHandler('../Logs/BoWIDF.log')
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger

def getSecondaryExceptionLogger(name=None):
    if name:
        name += "_se"
    selogger = logging.getLogger(name)
    handler = logging.FileHandler('../Logs/BoWIDF_SE.log')
    formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    selogger.addHandler(handler)
    selogger.setLevel(logging.INFO)
    return selogger