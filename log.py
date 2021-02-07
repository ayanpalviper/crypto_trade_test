import logging as l
from logging.handlers import TimedRotatingFileHandler


class log:
    def __init__(self):
        self.logger = l.getLogger()
        formatter = l.Formatter('[%(asctime)s] %(levelname)s p%(process)s %(thread)d - %(message)s')
        logname = "./logs/app.log"
        handler = TimedRotatingFileHandler(logname, when="H", interval=1)
        handler.suffix = "%Y%m%d"
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(l.DEBUG)

    def log_debug(self, message):
        self.logger.debug(message)

    def log_info(self, message):
        self.logger.info(message)

    def log_warn(self, message):
        self.logger.warning(message)

    def log_error(self, message):
        self.logger.error(message)

    def log_critical(self, message):
        self.logger.critical(message)

    def log_exception(self, message):
        self.logger.exception(message)
