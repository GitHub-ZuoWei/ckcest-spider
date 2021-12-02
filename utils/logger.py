# encoding: utf-8
# Author    : Davide<forever.suwei@gmail.com >
# Datetime  : 2021/2/19 5:38 下午
# User      : suwei
# Product   : PyCharm
# Project   :
# File      : logger.py
# explain   : 文件说明

import logging.config
import os

BASE_PATH = os.getcwd()
print(BASE_PATH)
DEBUG = True
log_level = logging.DEBUG if DEBUG else logging.INFO

log_path = os.path.join(BASE_PATH, 'logs')
if not os.path.exists(log_path):
    os.makedirs(log_path)

print('--', BASE_PATH)
# log config here
InfoLogPath = "/logs/info.log"
WarnLogPath = "/logs/warn.log"
ErrorLogPath = "/logs/error.log"
AccessLogPath = "/logs/access.log"
RootLogPath = "/logs/root.log"

log_config_dict = {
    "version": 1,
    'disable_existing_loggers': False,

    'loggers': {
        'log.info': {
            'handlers': ['info', 'console'],  # 列表类型，可以控制打印到文件和控制台
            'level': log_level,
            'propagate': False,  # 是否传递给父记录器
        },
        'log.warn': {
            'handlers': ['warn', 'console'],
            'level': logging.WARN,
            'propagate': False,  # 是否传递给父记录器
        },
        'log.error': {
            'handlers': ['error', 'console'],
            'level': logging.ERROR,
            'propagate': False,  # 是否传递给父记录器
        },
        'log.access': {
            'handlers': ['access', 'console'],
            'level': logging.INFO,
            'propagate': False,  # 是否传递给父记录器
        },
    },

    'handlers': {
        # 输出到控制台
        'console': {
            'level': log_level,
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        },
        # 输出到文件
        'info': {
            'level': log_level,
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'standard',
            'filename': BASE_PATH + InfoLogPath,
            'when': "midnight",  # 切割日志的时间
            'backupCount': 7,  # 备份份数
            'encoding': 'utf-8'
        },
        'warn': {
            'level': logging.WARN,
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'standard',
            'filename': BASE_PATH + WarnLogPath,
            'when': "midnight",  # 切割日志的时间
            'backupCount': 7,  # 备份份数
            'encoding': 'utf-8'
        },
        'error': {
            'level': logging.ERROR,
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'standard',
            'filename': BASE_PATH + ErrorLogPath,
            'when': "midnight",  # 切割日志的时间
            'backupCount': 7,  # 备份份数
            'encoding': 'utf-8',
        },
        'access': {
            'level': logging.INFO,
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'standard',
            'filename': BASE_PATH + AccessLogPath,
            'when': "midnight",  # 切割日志的时间
            'backupCount': 7,  # 备份份数
            'encoding': 'utf-8'
        }
    },

    'filters': {},

    'formatters': {
        # 标准输出格式
        'standard': {
            # 'format': '[%(asctime)s] - %(levelname)s %(module)s:%(funcName)s(%(lineno)d) - %(message)s'
            'format': '%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',
        }
    }
}

logging.config.dictConfig(log_config_dict)

log_info = logging.getLogger("log.info")
log_warn = logging.getLogger("log.warn")
log_error = logging.getLogger("log.error")
log_access = logging.getLogger("log.access")

log_info.info('aaa')
