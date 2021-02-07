import io
import os
import sqlite3 as sql
import threading
from datetime import date, datetime, timedelta
from threading import Lock
from time import sleep

import pandas as pd

import coindcx_api_caller as cdx
from env.load_env import load_env
from log import log
from slack_util import slack_util


def get_dataframe_info(df):
    buf = io.StringIO()
    df.info(buf=buf)
    return buf.getvalue()


def get_date_as_ms_string(days_delta):
    x = datetime.today() - timedelta(days=days_delta)
    dt = date(year=x.year, month=x.month, day=x.day)
    epoch = datetime.utcfromtimestamp(0)
    dt = datetime.combine(dt, datetime.min.time())
    dt = (dt - epoch).total_seconds() * 1000
    dt = str(dt)
    return dt[:dt.index('.')]


class master:

    def __init__(self):
        self.l = log()
        self.lock = Lock()
        self.env = load_env()

        self.slack = slack_util(self.l, self.env)

        self.call = cdx.call_api(self.l, self.env)

        self._created_threads = []
        self.markets_df = None
        self.ls_ticker_df = []
        self.l.log_info('MASTER INIT COMPLETE')


    # PRIVATE METHODS - START

    def _store_json(self, df):
        df.to_json(self.env.get_value('BASE_PATH') + self.env.get_value('USABLE_MARKET_DETAILS_PATH'))

    # PRIVATE METHODS - END

    def acquire_lock(self):
        while self.lock.locked():
            self.l.log_debug("locked")
            sleep(0.5)
        self.lock.acquire(blocking=True)

    def release_lock(self):
        if self.lock.locked():
            self.lock.release()

    def join_threads(self):
        for thread in self._created_threads:
            if thread.is_alive():
                thread.join(timeout=5)
                self.l.log_debug('finishing thread ' + thread.name)
            else:
                self.l.log_debug('thread is already dead ' + thread.name)

    def run_thread(self, name, function, ls_args):
        t = threading.Thread(target=function, args=tuple(ls_args), name="Thread-" + name, daemon=True)
        t.start()
        self.l.log_debug('starting thread ' + t.name)
        self._created_threads.append(t)

    def init_markets_df(self):

        path = self.env.get_value('BASE_PATH') + self.env.get_value('MARKET_DETAILS_PATH')

        try:
            mtime = os.path.getmtime(path)
            val = datetime.fromtimestamp(int(mtime))
            last_access_date = val.strftime("%Y-%m-%d")
        except FileNotFoundError as f:
            last_access_date = 'N/A'

        current_date = datetime.today().strftime('%Y-%m-%d')

        if current_date == last_access_date:
            df = pd.read_json(path)
        else:
            df = self.call.get_active_market_details()
            df.to_json(path)

        base_currency_list = self.env.get_value('BASE_CURR_LIST').split(',')

        user_balance_df = self.call.get_user_balances()

        for idx in df.index:
            base_currency_short_name = df['base_currency_short_name'][idx]
            if base_currency_short_name not in base_currency_list:
                df.drop(idx, inplace=True)
            else:
                drop = False
                if base_currency_short_name not in user_balance_df.currency.values:
                    df.drop(idx, inplace=True, errors="ignore")
                    continue
                index = user_balance_df[user_balance_df['currency'] == base_currency_short_name].index
                bal = user_balance_df['balance'][index].values[0] + user_balance_df['locked_balance'][index].values[0]
                min_notional = float(df['min_notional'][idx])
                '''
                if float(bal / 5) < float(df['min_notional'][idx]):
                    drop = True
                '''
                step = df['step'][idx]
                if step / min_notional > 100:
                    drop = True
                if drop:
                    df.drop(idx, inplace=True, errors="ignore")
        self.markets_df = df
        self.run_thread("store_usable_market_details", self._store_json, [df])
        return df

    def execute_sql(self, statement, commit=False, db='./db/buy_sell_test.db'):
        ls_results = []
        retry_count = 0
        self.acquire_lock()
        conn = sql.connect(db, isolation_level='EXCLUSIVE')
        while True:
            try:
                self.l.log_debug(statement)
                results = conn.execute(statement)
                for result in results:
                    ls_results.append(result)
                break
            except sql.OperationalError as e:
                self.l.log_exception('Error Occured while executing statement -> ' + statement)
                if retry_count >= 10:
                    self.l.log_error('maximum retry reached for statement -> ' + statement)
                    raise Exception('maximum retry reached for statement -> ' + statement)
                sleep(0.5)
                retry_count += 1
                continue
        if commit:
            self.l.log_debug('committing')
            conn.commit()
        conn.close()
        self.release_lock()
        return ls_results

    def store_ticker(self, df):
        self.ls_ticker_df.append(df)
