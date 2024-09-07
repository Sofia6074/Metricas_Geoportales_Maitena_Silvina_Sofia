import logging


class Logger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        logging.basicConfig(level=logging.INFO)

    def get_logger(self):
        return self.logger