import pandas as pd

import csv
import re
from datetime import datetime, timedelta

from beancount.core import data
from beancount.core.amount import Amount

from beancount.core.number import D
from beancount.core.number import Decimal

from beancount.core import position

from beancount.ingest import importer
import warnings
import uuid

class InvalidFormatError(Exception):
    def __init__(self, msg):
        pass

FIELDS_EN = (
    'date',
    'time',
    'valuta',
    'product',
    'isin',
    'description',
    'FX',
    'c_change',
    'change', # unknown
    'c_balance',
    'balance', # unknown
    'orderid'
)

class DegiroAccount(importer.ImporterProtocol):
    def __init__(self, language, LiquidityAccount, StocksAccount, FeesAccount, PnLAccount, DivIncomeAccount,
                 DepositAccount=None,
                 currency='EUR', file_encoding='utf-8' ):
        self.language = language
        self.currency = currency
        self.file_encoding = file_encoding

        self.liquidityAccount = LiquidityAccount
        self.stocksAccount = StocksAccount
        self.feesAccount = FeesAccount
        self.pnlAccount = PnLAccount
        self.divIncomeAccount = DivIncomeAccount
        self.depositAccount = DepositAccount

        self._date_from = None
        self._date_to = None
        self._balance_amount = None
        self._balance_date = None

        global l
        if self.language == 'de':
            import degiro_de as l

    def name(self):
        return 'Degiro {} importer'.format(self.__class__.__name__)

    def file_account(self, _):
        return self.account

    def file_date(self, file_):
        self.extract(file_)

        return self._date_to

    def identify(self, file_):
        with open(file_.name, encoding=self.file_encoding) as fd:
            line = fd.readline().strip()

        return True #self._expected_header_regex.match(line)


    def format_datetime(self, x):
        try:
            dt = pd.to_datetime(x, format=l.datetime_format)
        except Exception as e:
            # bad row with no date (will be sanitized later)
            return None
        return dt

    def extract(self, file_, existing_entries=None):
        entries = []

        try:
            df = pd.read_csv(file_.name, encoding=self.file_encoding,
                             header=0, names=FIELDS_EN,
                             parse_dates={ 'datetime' : ['date', 'time'] },
                             date_parser = self.format_datetime
                             )
        except Exception as e:
            raise InvalidFormatError(f"Read file "+ file_.name + " failed " + e)

        # some rows are broken into more rows. Sanitize them now

        prev_idx=None
        for idx, row in df.iterrows():
            if pd.isna(row['datetime']):
                if pd.notna(row['product']):
                    df.at[prev_idx, 'product'] += " "+row['product']
                if pd.notna(row['description']):
                    df.at[prev_idx, 'description'] += " "+row['description']
                if pd.notna(row['orderid']):
                    df.at[prev_idx, 'orderid'] += row['orderid']
            prev_idx=idx

        # drop rows with empty datetime
        df=df[pd.notna(df['datetime'])]

        # Drop 'cash sweep transfer' rows. These are transfers between the flatex bank account
        # and Degiro, and have no effect on the balance
        df=df[df['description'].map(lambda d: not re.match(l.cst['re'], d))]

        # Skipping rows with no change
        df=df[df['change'].map(lambda c: pd.notna(c) and D(c) != 0)]

        # Copy orderid as a new column uuid
        df['uuid']=df['orderid']

        #for idx, row in df.iterrows():
        #    if D(row['change']) == 0 and not re.match(l.liquidity_fund['re'], row['description']):
        #        print(f"Null row: {row}")

        # Match postings with no order id

        dfn = df[pd.isna(df['uuid'])]
        # filter out liquidity fund price changes and fees
        dfn=dfn[dfn['description'].map(lambda d: not ( re.match(l.liquidity_fund['re'], d)
                                                       or
                                                       re.match(l.fees['re'], d)
                                                       or
                                                       re.match(l.deposit['re'], d)
                                                       or
                                                       re.match(l.interest['re'], d)
                                                      ) )]

        #

        for idx, row in dfn.iterrows():
            # print (f"No order ID: {row['datetime']} {row['isin']} {row['description']} {row['change']}")
            if re.match(l.dividend['re'], row['description']):
                # Lookup other legs of dividend transaction
                # 1. Dividend tax: ISIN match
                mdfn=dfn[(dfn['isin']==row['isin'])
                         & (dfn['datetime'] > row['datetime']-timedelta(days=31)) & (dfn['datetime'] < row['datetime']+timedelta(days=5) )
                         & (dfn['description'].map(lambda d: re.match(l.dividend_tax['re'], d)))]
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    if pd.notna(df.loc[midx, 'uuid']):
                        print(f"Ambigous generated uuid for line {midx+2}")
                    #print(f"Setting uuid {muuid} for line {midx+2}")
                    df.loc[midx, 'uuid'] = muuid
                if pd.notna(df.loc[idx, 'uuid']):
                    print(f"Ambigous generated uuid for line {idx+2}")
                df.loc[idx, 'uuid'] = muuid
                continue
            if re.match(l.change['re'], row['description']):
                mdfn=dfn[dfn['datetime']==row['datetime']] # FIXME we should match book in <-> book out and check that exactly 2 rows are found
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    df.loc[midx, 'uuid'] = muuid
                continue
            if re.match(l.split['re'], row['description']):
                mdfn=dfn[(dfn['datetime']==row['datetime']) & (dfn['isin']==row['isin'])]
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    df.loc[midx, 'uuid'] = muuid


        postings = []

        it=df.iterrows()

        row=None

        def handle_lf_and_fees(vals, row, amount ):
            return [data.Posting(self.feesAccount, -amount, None, None, None, None )]

        def handle_deposit(vals, row, amount):
            if self.depositAccount is None:
                return []
            return [data.Posting(self.depositAccount, -amount, None, None, None, None )]

        def handle_buy(vals, row, amount):
            cost = position.CostSpec(
                number_per=vals['price'],
                number_total=None,
                currency=vals['currency'],
                date=row['datetime'].date(),
                label=None,
                merge=False)
            stockamount = Amount(vals['quantity'],row['isin'])

            return [data.Posting(self.stocksAccount, stockamount, cost, None, None, None )]

        def handle_sell(vals, row, amount):

            stockamount = Amount(-vals['quantity'], row['isin'])

            cost=position.CostSpec(
                number_per=None,
                number_total=None,
                currency=None,
                date=None,
                label=None,
                merge=False)

            sellPrice=Amount(vals['price'], vals['currency'])

            return [data.Posting(self.stocksAccount, stockamount, cost, sellPrice, None, None),
                    data.Posting(self.pnlAccount,           None, None,      None, None, None)]

#        def handle_change(vals, row, amount):
#

        trtypes = [
            { 'd': 'Liquidity Fund Price Change', 'r': l.liquidity_fund, 'h': handle_lf_and_fees },
            { 'd': 'Fees',                        'r': l.fees,           'h': handle_lf_and_fees },
            { 'd': 'Deposit',                     'r': l.deposit,        'h': handle_deposit },
            { 'd': 'Buy',                         'r': l.buy,            'h': handle_buy },
            { 'd': 'Sell',                        'r': l.sell,           'h': handle_sell },
            { 'd': 'Interest',                    'r': l.interest,       'h': handle_lf_and_fees }
#            { 'd': 'Change',                      'r': l.change,         'h': handle_change }

        ]

        while True:

            prev_row = row
            idx, row = next(it, [-1, 0])

            if idx == -1 or ( prev_row is not None and (pd.isna(prev_row['uuid']) or row['uuid'] != prev_row['uuid'])):
                # previous transaction completed
                if postings:

                    payee=prev_row['isin']
                    if pd.isna(payee):
                        payee="NO PAYEE"

                    uuid_meta={}
                    if pd.notna(prev_row['uuid']):
                        uuid_meta = {'uuid':prev_row['uuid']}
                    meta=data.new_metadata(__file__,0, uuid_meta)
                    entries.append(data.Transaction(meta, # meta
                                                    prev_row['datetime'].date(),
                                                    self.FLAG,
                                                    payee, # prev_row['isin'], # payee
                                                    prev_row['description'],
                                                    data.EMPTY_SET, # tags
                                                    data.EMPTY_SET, # links
                                                    postings
                                                    ))
                    postings = []


            if idx == -1:
                # prev_row was the last
                break

            if re.match(l.deposit['re'], row['description']) and self.depositAccount is None:
                continue

            amount = Amount(l.fmt_number(row['change']),row['c_change'])

            cost=None
            if pd.notna(row['FX']):
                cost=position.CostSpec(
                    number_per=round(1/l.fmt_number(row['FX']), 6),
                    number_total=None,
                    currency=self.currency,
                    date=None,
                    label=None,
                    merge=False)

            postings.append(data.Posting(self.liquidityAccount+':'+row['c_change'], amount, cost, None, None, None ))

            match = False
            for t in trtypes:
                m=re.match(t['r']['re'], row['description'])
                if m:
                    mv = None
                    if 'vals' in t['r']:
                        mv = t['r']['vals'](m)
                    postings += t['h'](mv, row, amount)
                    match = True
                    break

            if not match and pd.isna(row['uuid']):
                print(f"Line {idx+2} Unepected description: {row['uuid']} {row['description']}")

        return entries

