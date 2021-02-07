from coindcx_api_caller import call_api
from log import log
from slack_util import slack_util
from master import master


class reset:

    def __init__(self, m):
        self.u = m
        self.l = log()
        self.slack = m.slack
        self.call = m.call
        self.l.log_info('starting reset')

    def do(self):
        base_currency_list = ['BTC', 'ETH']
        df = self.call.get_user_balances()
        statement = "delete from balances"
        self.u.execute_sql(statement, True)
        statement = "delete from trade"
        self.u.execute_sql(statement, True)
        for idx in df.index:
            if df['currency'][idx] not in base_currency_list:
                df.drop(idx, inplace=True)
            else:
                currency = df['currency'][idx]
                bal = df['balance'][idx]
                statement = "insert into balances (currency, balance) values (%(c)s, %(b)s)" % {"c": "'" + currency + "'", "b": "'" + str(bal) + "'"}
                self.u.execute_sql(statement, True)


if __name__ == "__main__":
    m = master()
    r = reset(m)
    r.do()
