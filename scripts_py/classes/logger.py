import logging


class Logger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)
        logging.basicConfig(
            level=logging.INFO,
            format='(%(name)s): %(message)s'
        )

    def get_logger(self):
        return self.logger