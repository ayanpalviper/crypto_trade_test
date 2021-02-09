from master import master
from time import sleep
from eligible_pairs import capture
from trade import trade

from datetime import datetime

'''
l = log()
l.log_info('***********START***********')

with open('last.txt', 'r') as f:
    s = f.read()
    date_object = date.today()
    if s.strip() == str(date_object):
        l.log_info('has already run today')
        sys.exit()


slack = slack_util()


def connect(host='http://facebook.com'):
    try:
        urllib.request.urlopen(host, timeout=10)
        return True
    except:
        return False


count = 0

while not connect():
    if count >= 10:
        slack.post_message("No Internet. Maximum retry exceeded, please run script manually")
        l.log_error("Maximum retry exceeded, please run script manually")
        sys.exit()
    l.log_warn("No Internet Connection")
    sleep(600)
    count += 1

'''
start_time = datetime.now()

m = master()
m.init_markets_df()

ep = capture(m)
ep.do()

t = trade(m)
t.do()

time_diff = datetime.now() - start_time
m.l.log_info('main completion took -> %s' % time_diff)

'''
sleep(5)

t = trade()
t.do()

date_object = date.today()
with open('last.txt', 'w') as f:
    print(date_object, file=f)

l.log_info('***********END***********')

'''

m.join_threads()

