import concurrent.futures
from datetime import datetime
from os import listdir
from os.path import isfile, join

import pandas as pd

from log import log
from slack_util import slack_util
from utility import util


class trade:

    def __init__(self):
        self.start_time = datetime.now()
        self.u = util()
        self.slack = slack_util()
        self.l = log('trade.py')

    def get_total_balance(self):
        ret = {}
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
        results = self.u.execute_sql(statement)
        for result in results:
            ret[result[0]] = result[1]
        return ret

    def do(self):

        initial_balance_dict = self.get_total_balance()

        onlyfiles = [f for f in listdir("./ticker") if isfile(join("./ticker", f))]

        self.l.log_info('count of files -> ' + str(len(onlyfiles)))

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for file in onlyfiles:
                p = _perform(self.u)
                futures.append(executor.submit(p.analyse, file))
            for future in concurrent.futures.as_completed(futures):
                self.l.log_info(future.result())

        final_balance_dict = self.get_total_balance()

        for key in final_balance_dict:
            if initial_balance_dict.get(key) is not None:
                initial_bal = initial_balance_dict[key]
                final_bal = final_balance_dict[key]
                perc = ((float(final_bal) - float(initial_bal)) / float(initial_bal)) * 100
                self.slack.post_message("estimated profit % for " + key + " -> " + str(perc))

        time_diff = datetime.now() - self.start_time

        self.l.log_info('completion took -> %s' % time_diff)


class _perform:

    def __init__(self, u):
        self.u = u
        self.markets_df = pd.read_json("./json/markets_details.json")

    def computeRSI(self, data, time_window):
        diff = data.diff(1).dropna()
        up_chg = 0 * diff
        down_chg = 0 * diff
        up_chg[diff > 0] = diff[diff > 0]
        down_chg[diff < 0] = diff[diff < 0]
        up_chg_avg = up_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
        down_chg_avg = down_chg.ewm(com=time_window - 1, min_periods=time_window).mean()
        rs = abs(up_chg_avg / down_chg_avg)
        rsi = 100 - 100 / (1 + rs)
        return rsi

    def stochastic(self, data, k_window, d_window, window):
        min_val = data.rolling(window=window, center=False).min()
        max_val = data.rolling(window=window, center=False).max()
        stoch = ((data - min_val) / (max_val - min_val)) * 100
        K = stoch.rolling(window=k_window, center=False).mean()
        D = K.rolling(window=d_window, center=False).mean()
        return K, D

    def buy(self, pair, price, currency, timestamp, status, action, buy_amount):
        units = buy_amount / float(price)
        substitution_dict = {
            "pair": "'" + pair + "'",
            "price": price,
            "currency": "'" + currency + "'",
            "unixtime": timestamp,
            "status": "'" + status + "'",
            "action": "'" + action + "'",
            "units": "'" + str(units) + "'",
            "total_price": "'" + str(buy_amount) + "'"
        }

        statement = "insert into trade (pair, price, units, total_price, currency, unixtime, status, action) values (%(pair)s, %(price)s, %(units)s, %(total_price)s, %(currency)s, %(unixtime)s, " \
                    "%(status)s, %(action)s)" % substitution_dict
        self.u.execute_sql(statement, True)

        statement = "select balance from balances where currency = '" + currency + "'"
        results = self.u.execute_sql(statement)
        for result in results:
            buy_price = float(result[0]) - float(buy_amount)

        statement = "update balances set balance = " + str(buy_price) + " where currency = %(currency)s" % substitution_dict
        self.u.execute_sql(statement, True)

    def sell(self, pair, price, currency, timestamp, status, action, profit_percent, total_sell_price, total_units):
        substitution_dict = {
            "pair": "'" + pair + "'",
            "price": price,
            "currency": "'" + currency + "'",
            "unixtime": timestamp,
            "status": "'" + status + "'",
            "action": "'" + action + "'",
            "profit": "'" + profit_percent + "'",
            "total_sell_price": "'" + str(total_sell_price) + "'",
            "total_units": "'" + str(total_units) + "'"
        }

        statement = "insert into trade (pair, price, units, total_price, currency, unixtime, status, action, profit) values (%(pair)s, %(price)s, %(total_units)s, " \
                    "%(total_sell_price)s, %(currency)s, %(unixtime)s, %(status)s, %(action)s, %(profit)s)" % substitution_dict
        self.u.execute_sql(statement, True)

        statement = "select balance from balances where currency = '" + currency + "'"
        results = self.u.execute_sql(statement)
        for result in results:
            total_sell_price = float(result[0]) + float(total_sell_price)

        statement = "update balances set balance = " + str(total_sell_price) + " where currency = %(currency)s" % substitution_dict
        self.u.execute_sql(statement, True)

        statement = "select id from trade where pair = %(pair)s and action = 'SELL' order by unixtime desc limit 1" % {"pair": "'" + pair + "'"}
        results = self.u.execute_sql(statement, False)
        id = -1
        for result in results:
            id = result[0]
            break

        statement = "select id from trade where pair = %(pair)s and action = 'BUY' and related = -1" % {"pair": "'" + pair + "'"}
        results = self.u.execute_sql(statement, False)
        for result in results:
            substitution_dict = {
                "related_id": id,
                "id": result[0]
            }
            statement = "update trade set related = %(related_id)s where id = %(id)s" % substitution_dict
            self.u.execute_sql(statement, True)

    def calculate_profit(self, sold_at, pair):
        total_buy_price = 0
        total_units = 0

        statement = "select total_price, units from trade where pair = %(pair)s and action = 'BUY' and related = -1" % {"pair": "'" + pair + "'"}
        results = self.u.execute_sql(statement)

        for result in results:
            total_buy_price += float(result[0])
            total_units += float(result[1])
        total_sell_price = total_units * sold_at
        return total_sell_price, total_units, ((total_sell_price - total_buy_price) / total_buy_price) * 100

    def is_already_bought(self, pair):
        count = 0
        statement = "select count(*) from trade where pair = %(pair)s and action = 'BUY' and related = -1" % {"pair": "'" + pair + "'"}
        results = self.u.execute_sql(statement)
        for result in results:
            count = int(result[0])
        return count > 0

    def rsi_constantly_below_bottom_threshold_since_bought(self, df, current_index, last_buy_idx):
        if last_buy_idx is None:
            return False
        sub_df = df.loc[last_buy_idx:current_index]
        for idx in sub_df.index:
            rsi_below_bottom_threshold = (df['RSI'][idx] < 30) or (0 < (((df['RSI'][idx] - 30) / 30) * 100) < 5)
            if rsi_below_bottom_threshold:
                continue
            else:
                return False
        return True

    def cannot_buy(self, currency, price):
        statement = "select balance from balances where currency = '" + currency + "'"
        results = self.u.execute_sql(statement)
        for result in results:
            if price >= (float(result[0]) * 2) / 3:
                return True
        return False

    def get_min_buy_amount(self, pair):
        df = self.markets_df.loc[self.markets_df['pair'] == pair]
        return df['min_notional'].values[0]

    def analyse(self, filepath):
        _l = log(filepath)
        try:
            df = pd.read_json("./ticker/" + filepath)
            df = df.sort_index(ascending=False)
            df = df.reset_index()
            df = df.drop(columns=['index'])
            df['RSI'] = self.computeRSI(df['close'], 14)
            df['K'], df['D'] = self.stochastic(df['RSI'], 3, 3, 14)
            df['MACD'] = df['close'].ewm(span=12).mean() - df['close'].ewm(span=26).mean()
            df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
            df['MACD'] = df['MACD'].multiply(1000000)
            df['MACD_Signal'] = df['MACD_Signal'].multiply(1000000)

            df['Cum_Vol'] = df['volume'].cumsum()
            df['Cum_Vol_Price'] = (df['volume'] * (df['high'] + df['low'] + df['close']) / 3).cumsum()
            df['VWAP'] = df['Cum_Vol_Price'] / df['Cum_Vol']

            df = df.drop(columns=['Cum_Vol', 'Cum_Vol_Price'])
            df = df.tail(287)
            fp = filepath.replace(".json", "")
            min_buy_amount = self.get_min_buy_amount(fp)
            bought = self.is_already_bought(fp)
            _l.log_info('Already Bought -> ' + str(bought))
            last_bought = None

            for idx in df.index:
                rsi_below_bottom_threshold = (df['RSI'][idx] < 30) or (((abs(30 - df['RSI'][idx]) / 30) * 100) < 5)
                s_rsi_below_threshold_and_blue_below_red = (df['K'][idx] < 20) or ((abs(df['K'][idx] - 20) / 20) * 100) < 5
                macd_green_below_red = df['MACD'][idx] < df['MACD_Signal'][idx] and abs(((df['MACD_Signal'][idx] - df['MACD'][idx]) / df['MACD'][idx]) * 100) > 5
                val = datetime.fromtimestamp(int(df['time'][idx] / 1000))
                val = val.strftime("%Y-%m-%d %H:%M:%S")
                if rsi_below_bottom_threshold and s_rsi_below_threshold_and_blue_below_red and macd_green_below_red:

                    bought_for = df['close'][idx]

                    if self.cannot_buy(fp.split("_")[1], min_buy_amount):
                        _l.log_info('insufficient balance at ' + val)
                        continue
                    if self.rsi_constantly_below_bottom_threshold_since_bought(df, idx, last_bought):
                        _l.log_info('rsi_constantly_below_bottom_threshold_since_bought')
                        continue
                    if last_bought is not None and ((df['time'][idx] - df['time'][last_bought]) <= 900000):
                        _l.log_info('Already bought in the last 15 mins')
                        continue

                    _l.log_info('buying at ' + val)
                    bought = True
                    self.buy(fp, bought_for, fp.split("_")[1], df['time'][idx], "DONE", "BUY", min_buy_amount)
                    last_bought = idx
                    continue

                if bought:
                    rsi_above_top_threshold = (df['RSI'][idx] > 70) and ((((df['RSI'][idx] - 70) / 70) * 100) > 5)
                    s_rsi_above_top_threshold = (df['K'][idx] > 80) and (((df['K'][idx] - 80) / 80) * 100) > 5
                    macd_green_above_red = df['MACD'][idx] > df['MACD_Signal'][idx] and abs(((df['MACD_Signal'][idx] - df['MACD'][idx]) / df['MACD'][idx]) * 100) > 5

                    if rsi_above_top_threshold and s_rsi_above_top_threshold and macd_green_above_red:
                        _l.log_info('sold at' + val)
                        sold_for = df['close'][idx]
                        total_sell_price, total_units, profit_percent = self.calculate_profit(sold_for, fp)
                        bought = False
                        self.sell(fp, sold_for, fp.split("_")[1], df['time'][idx], 'DONE', 'SELL', str(profit_percent), total_sell_price, total_units)

            if bought:
                _l.log_info("remains unsold at EOD")
            return "completed " + filepath

        except:
            _l.log_exception('Error Occurred')
