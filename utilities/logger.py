# -*- coding: utf-8 -*-

import datetime
import logging
import os
from logging.handlers import TimedRotatingFileHandler

class UserFormater(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, user_timefunction=None):
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)
        self.user_timefunction = user_timefunction

    def formatTime(self, record, datefmt=None):
        if self.user_timefunction is None:
            return super().formatTime(record, datefmt)
        else:
            return self.user_timefunction()


def get_logger(module_name: str, filename: str = '', user_timefunction=None, backtest=False, level='INFO', add_date_and_pid=False):
    """ module_name 应传入 __name__；filename无需包含'.log' """
    logger = logging.getLogger(module_name)  # 用到时再取logger 不要在文件头部获取logger
    logger.setLevel(level)

    if user_timefunction is not None:
        _formatter = UserFormater('%(asctime)s - %(filename)s:%(lineno)d %(module)s %(levelname)s: %(message)s',
                                  user_timefunction=user_timefunction)
    else:
        _formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)d %(module)s %(levelname)s: %(message)s')

    if not os.path.exists('log'):
        os.mkdir('log')
    filename = filename if filename else f'hft-{os.getpid()}'
    if add_date_and_pid:
        filename += f'-{datetime.date.today().strftime("%Y%m%d")}-{os.getpid()}'
    log_file_name = f'log/backtest-{filename}.log' if backtest else f'log/{filename}.log'
    file_handler = TimedRotatingFileHandler(log_file_name, when='D', encoding='utf-8')  # 避免中文乱码
    file_handler.setFormatter(_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_formatter)

    if len(logger.handlers):
        logger.handlers.clear()

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger