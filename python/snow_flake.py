#!/usr/bin python3
# -*- coding: utf-8 -*-
# @Time    : 2021/2/2 17:10
# @Author  : luky1833
# @File    : snow_flake.py
# @Software: PyCharm
# @Desc    :
"""

雪花id获取

"""

import time
import logging

# from exceptions import InvalidSystemClock

# 64位ID的划分
WORKER_ID_BITS = 5
DATACENTER_ID_BITS = 5
SEQUENCE_BITS = 12

# 最大取值计算
MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS)  # 2**5-1 0b11111
MAX_DATACENTER_ID = -1 ^ (-1 << DATACENTER_ID_BITS)

# 移位偏移计算
WOKER_ID_SHIFT = SEQUENCE_BITS
DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS

# 序号循环掩码
SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

# 2021-01-01时间戳
TWEPOCH = 1609430400000

# 参数设置
DATACENTER_ID = 0
WORKER_ID = 0
SEQUENCE = 0


class IdWorker(object):
    """
    用于生成IDs
    """

    def __init__(self):
        """
        初始化
        :param DATACENTER_ID: 数据中心（机器区域）ID
        :param WORKER_ID: 机器ID
        :param SEQUENCE: 其实序号
        """
        # sanity check
        if WORKER_ID > MAX_WORKER_ID or WORKER_ID < 0:
            raise ValueError('worker_id值越界')

        if DATACENTER_ID > MAX_DATACENTER_ID or DATACENTER_ID < 0:
            raise ValueError('datacenter_id值越界')

        self.worker_id = WORKER_ID
        self.datacenter_id = DATACENTER_ID
        self.sequence = SEQUENCE

        self.last_timestamp = -1  # 上次计算的时间戳

    def _gen_timestamp(self):
        """
        生成整数时间戳
        :return:int timestamp
        """
        return int(time.time() * 1000)

    def get_id(self):
        """
        获取新ID
        :return:
        """
        timestamp = self._gen_timestamp()

        # 时钟回拨
        if timestamp < self.last_timestamp:
            logging.error('clock is moving backwards. Rejecting requests until {}'.format(self.last_timestamp))
            raise InvalidSystemClock

        if timestamp == self.last_timestamp:
            self.sequence = (self.sequence + 1) & SEQUENCE_MASK
            if self.sequence == 0:
                timestamp = self._til_next_millis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        new_id = ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT) | (self.datacenter_id << DATACENTER_ID_SHIFT) | \
                 (self.worker_id << WOKER_ID_SHIFT) | self.sequence
        return new_id

    def _til_next_millis(self, last_timestamp):
        """
        等到下一毫秒
        """
        timestamp = self._gen_timestamp()
        while timestamp <= last_timestamp:
            timestamp = self._gen_timestamp()
        return timestamp


class InvalidSystemClock(Exception):
    """
    时钟回拨异常
    """
    pass


def get_snowflake_id():
    _worker = IdWorker().get_id()
    return _worker


if __name__ == '__main__':
    snow_id = get_snowflake_id()
    print(snow_id)
