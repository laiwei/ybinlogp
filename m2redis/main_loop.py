#-*-coding:utf8-*-
import sys
sys.path.insert(0, "../ybinlogp/")

import os
import time
from datetime import datetime
import re
import redis
from ybinlogp import YBinlogP

from config import REDIS_HOST, REDIS_PORT, EXPIRE
redis_conn = redis.Redis(REDIS_HOST, REDIS_PORT)

pattern = re.compile(r'\(([0-9]+),([0-9]+),([0-9]+),([0-9]+)\)')
bp = YBinlogP(sys.argv[1], always_update=True)
g_current_file = sys.argv[1]

def get_next_filename(current_file):
    #mysql-bin.000016
    prefix, num = current_file.split(".")
    next_num = "%06i" %(int(num) + 1)
    return "%s.%s" %(prefix, next_num)

def next_file_exists(filename):
    return os.path.exists(filename)

while True:
    for i,event in enumerate(bp):
        if event.event_type == "ROTATE_EVENT":
            next_file = event.data.file_name
            g_current_file = next_file
            bp.clean_up()
            bp = YBinlogP(next_file, always_update=True)
            break
        elif event.event_type == "QUERY_EVENT":
            s = event.data.statement
            if s.startswith("insert into history_"):
                rows = pattern.findall(s) or []
                for raw in rows:
                    l = list(raw)
                    itemid = l[0]
                    clock = l[1]
                    value = l[2]

                    day = datetime.fromtimestamp(int(clock)).strftime("%m%d")
                    name = "%s:%s" %(day, itemid)
                    redis_value = "%s:%s" %(clock, value)
                    print name, redis_value
                    if not redis_conn.exists(name):
                        redis_conn.rpush(name, redis_value)
                        redis_conn.expire(name, EXPIRE)
                    else:
                        redis_conn.rpush(name, redis_value)
    else:
        print "Got to end at %r" % (bp.tell(),)
        time.sleep(1)
        if next_file_exists(get_next_filename(g_current_file)):
            print "check next file g_current_file", g_current_file
            next_file = get_next_filename(g_current_file)
            g_current_file = next_file
            bp.clean_up()
            bp = YBinlogP(next_file, always_update=True)
bp.clean_up()

