import os
from os.path import join
import logging
import socket

def get_logger(module_name: str, logfile_name:str='log.log') -> logging.Logger:
    """Simple logger writing to a log file.

    Args:
        module_name (str): module name from which is called

    Returns:
        logging.Logger: Logger class instance
    """    
    FILE_LOG_LEVEL = logging.INFO 
    STREAM_LOG_LEVEL = logging.WARNING 

    extra = {'hostname': socket.gethostname(), 'ip': socket.gethostbyname(socket.gethostname())}
    logger = logging.getLogger(module_name)
    logger.setLevel(FILE_LOG_LEVEL)
    fh = logging.FileHandler(join(os.getcwd(), logfile_name))
    fh.setLevel(FILE_LOG_LEVEL)
    ch = logging.StreamHandler()
    ch.setLevel(STREAM_LOG_LEVEL)
    formatter = logging.Formatter('date_time: %(asctime)s # module_name: %(name)s # level_name: %(levelname)s # host_name: %(hostname)s # ip %(ip)s # message: %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger = logging.LoggerAdapter(logger, extra)

    return logger