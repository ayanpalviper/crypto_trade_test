import threading
from datetime import datetime

from coindcx_api_caller import call_api
from log import log
from slack_util import slack_util
from utility import util


class markets:

    def __init__(self):

        self.start_time = datetime.now()

        self.l = log()

        self.l.log_info('begin')

        self.slack = slack_util()

        self.call = call_api()

        self.util = util()





    def capture(self):

        try:

            df = self.call.get_active_market_details()

            base_currency_list = ['BTC', 'ETH']

            user_balance_df = self.call.get_user_balances()

            min_price_threshold = {
                "BTC": 0.0000001,
                "ETH": 0.00001
            }

            override_price = self.get_override_prices()

            user_balance_df = self.override(user_balance_df, override_price)

            for idx in df.index:
                base_currency_short_name = df['base_currency_short_name'][idx]
                if base_currency_short_name not in base_currency_list:
                    df.drop(idx, inplace=True)
                else:
                    drop = False
                    index = user_balance_df[user_balance_df['currency'] == base_currency_short_name].index
                    bal = user_balance_df['balance'][index].values[0]
                    curr = str(user_balance_df['currency'][index].values[0])
                    if float(bal / 5) < float(df['min_notional'][idx]):
                        drop = True
                    if float(df['min_notional'][idx]) < float(min_price_threshold[curr]):
                        drop = True
                    if drop:
                        df.drop(idx, inplace=True, errors="ignore")

            df.to_json("./json/markets_details.json")

            time_diff = datetime.now() - self.start_time

            self.l.log_info('completion took -> %s' % time_diff)

            self.slack.post_message("market details successfully captured")

        except:
            self.slack.post_message("error occurred while generating market details")
            self.l.log_exception("Error Occurred")

    def override(self, balance_df, override_dict):
        for idxx in balance_df.index:
            c = balance_df['currency'][idxx]
            if override_dict.get(c) is not None:
                balance_df.loc[idxx, 'balance'] = override_dict[c]
                self.l.log_debug("Balance of " + c + " overridden to " + str(override_dict[c]))
        return balance_df

    def get_override_prices(self):
        ret_dict = {}
        statement = '''select total_bal.c, sum(total_bal.b) from (select t.currency as c, sum(t.total_price) as b
                        FROM
                        trade t
                        where 
                        t.action = 'BUY'
                        and t.related = -1
                        group by t.currency
                        UNION
                        select currency as c, balance as b from balances) as total_bal
                        group by total_bal.c'''
        results = self.util.execute_sql(statement, False)
        for result in results:
            ret_dict[result[0]] = float(result[1])
        self.l.log_debug("Override Price config fetched")
        return ret_dict
