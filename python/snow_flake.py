#!/usr/bin python3
# -*- coding: utf-8 -*-

"""
雪花id获取
"""

import random
import threading
import time

# 64位ID的划分
WORKER_ID_BITS = 5
DATACENTER_ID_BITS = 5
SEQUENCE_BITS = 12

# 最大取值计算
MAX_WORKER_ID = -1 ^ (-1 << WORKER_ID_BITS)
MAX_DATACENTER_ID = -1 ^ (-1 << DATACENTER_ID_BITS)

# 移位偏移计算
WOKER_ID_SHIFT = SEQUENCE_BITS
DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS

# 序号循环掩码
SEQUENCE_MASK = -1 ^ (-1 << SEQUENCE_BITS)

# 2021-01-01时间戳
INITIAL_TIME_STAMP = 1609430400000

# 参数设置
DATACENTER_ID = 0
WORKER_ID = 0

# 锁
ID_LOCK = threading.Lock()
# 单例
SNOW_FLAKE_ID_GENERATOR = None


class SnowFlakeIdGenerator(object):
    sequence = 0
    last_timestamp = -1  # 上次计算的时间戳
    _instance_lock = threading.Lock()

    def __init__(self):
        if WORKER_ID > MAX_WORKER_ID or WORKER_ID < 0:
            raise ValueError('worker_id值越界')

        if DATACENTER_ID > MAX_DATACENTER_ID or DATACENTER_ID < 0:
            raise ValueError('datacenter_id值越界')

        self.worker_id = WORKER_ID
        self.datacenter_id = DATACENTER_ID

    def __new__(cls, *args, **kwargs):
        if not hasattr(SnowFlakeIdGenerator, "_instance"):
            with SnowFlakeIdGenerator._instance_lock:
                if not hasattr(SnowFlakeIdGenerator, "_instance"):
                    SnowFlakeIdGenerator._instance = object.__new__(cls)
        return SnowFlakeIdGenerator._instance

    def next_id(self):
        global ID_LOCK
        with ID_LOCK:
            _timestamp = self._time_gen()
            # 时钟回拨
            if _timestamp < self.last_timestamp:
                _offset = self.last_timestamp - _timestamp
                # 闰秒
                if _offset <= 5:
                    try:
                        time.sleep(0.01)  # 时间偏差大小小于5ms，则等待两倍时间
                        _timestamp = self._time_gen()
                        if _timestamp < self.last_timestamp:
                            raise InvalidSystemClock
                    except Exception:
                        raise InvalidSystemClock
                else:
                    raise InvalidSystemClock
            # 如果是同一时间生成的，则进行毫秒内序列
            if _timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & SEQUENCE_MASK
                if self.sequence == 0:  # 毫秒内序列已经增长到最大值
                    _timestamp = self._til_next_millis(self.last_timestamp)
            else:
                # 时间戳改变，毫秒内序列重置
                # sequence = 0 会产生大量偶数，如果直接应用该ID来做分库分表，则极有可能出现数据不均匀的情况
                # 解决办法：1、自增ID满值后再重置
                # 2、重置时使用范围随机(重置随机为0~9)
                self.sequence = random.randint(0, 9)
                # 3、生成ID的时候把序列号部分尾数用时间戳对应的位置覆盖
                # self.sequence = 127 & timestamp
            # 上次生成ID的时间截
            self.last_timestamp = _timestamp
            # 时间戳部分 | 数据中心部分 | 机器标识部分 | 序列号部分
            return ((_timestamp - INITIAL_TIME_STAMP) << TIMESTAMP_LEFT_SHIFT) | (
                    self.datacenter_id << DATACENTER_ID_SHIFT) | (self.worker_id << WOKER_ID_SHIFT) | self.sequence

    def _til_next_millis(self, _last_timestamp):
        _timestamp = self._time_gen()
        while _timestamp <= _last_timestamp:
            _timestamp = self._time_gen()
        return _timestamp

    def _time_gen(self):
        return int(time.time() * 1000)


def get_snowflake_id():
    global ID_LOCK
    global SNOW_FLAKE_ID_GENERATOR
    if SNOW_FLAKE_ID_GENERATOR is None:
        with ID_LOCK:
            if SNOW_FLAKE_ID_GENERATOR is None:
                SNOW_FLAKE_ID_GENERATOR = SnowFlakeIdGenerator()
    return SNOW_FLAKE_ID_GENERATOR.next_id()


class InvalidSystemClock(Exception):
    """
    时钟回拨异常
    """
    pass


if __name__ == '__main__':
    print(get_snowflake_id())
