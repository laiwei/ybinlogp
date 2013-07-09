#-*-coding:utf8-*-
import sys
sys.path.insert(0, "../ybinlogp/")

import os
import time
from datetime import datetime
import re
from collections import defaultdict
import redis
from ybinlogp import YBinlogP
from config import REDIS_HOST, REDIS_PORT, EXPIRE
redis_conn = redis.Redis(REDIS_HOST, REDIS_PORT)

def f():
    for x in xrange(0, 1000):
        redis_conn.lpush("laiwei", "laiwei")

def f2():
    v = []
    for x in xrange(0, 100):
        v.append(x)
    redis_conn.lpush("laiwei2", *v)

if __name__ == '__main__':
    import timeit
    #print(timeit.timeit("f()", number=1000, setup="from __main__ import f"))
    print(timeit.timeit("f2()", number=1000, setup="from __main__ import f2"))
