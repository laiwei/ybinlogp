#-*-coding:utf8-*-
import sys
sys.path.insert(0, "../")

from ybinlogp import YBinlogP
from config import REDIS_HOST, REDIS_PORT
redis_conn = redis.Redis(REDIS_HOST, REDIS_PORT)

bp = YBinlogP(sys.argv[1], always_update=True)
while True:
    for i,event in enumerate(bp):
        if event.event_type == "ROTATE_EVENT":
            next_file = event.data.file_name
            bp.clean_up()
            bp = YBinlogP(next_file, always_update=True)
            break
        elif event.event_type == "QUERY_EVENT":
            s = event.data.statement
            print s
            if s.startswith("insert into history_"):
                pass
    else:
        print "Got to end at %r" % (bp.tell(),)
        break
bp.clean_up()


#update items set lastclock=1371730256,lastns=559475487,prevvalue=lastvalue,lastvalue='3a0f3791d68d5ff0503be3464c340079  /etc/passwd' where itemid=25316
#QUERY_EVENT
#QUERY_EVENT
#update items set lastclock=1371730256,lastns=556143878,prevvalue=lastvalue,lastvalue='19874058240' where itemid=25436
#QUERY_EVENT
#QUERY_EVENT
#update items set lastclock=1371730257,lastns=556110781,prevvalue=lastvalue,lastvalue='78337024' where itemid=25437
#QUERY_EVENT
#QUERY_EVENT
#update items set lastclock=1371730255,lastns=555493008,prevvalue=lastvalue,lastvalue='314855424' where itemid=25495
#QUERY_EVENT
#QUERY_EVENT
#insert into history_uint (itemid,clock,value,ns) values (25495,1371730255,314855424,555493008),(25436,1371730256,19874058240,556143878),(25437,1371730257,78337024,556110781)
