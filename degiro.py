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
import degiro_de

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

        self._conversion_tolerance = 0.01
        if self.language == 'de':
            self.l = degiro_de

    def name(self):
        return f'{self.__class__.__name__} importer'

    def identify(self, file_):
        # Check header line only
        with open(file_.name, encoding=self.file_encoding) as fd:
            return bool(re.match(','.join(self.l.FIELDS), fd.readline()))

    def file_account(self, _):
        return self.account

    def file_date(self, file_):
        # unfortunately, no simple way to determine file date from content.
        # fall back to file creation date.
        return None

    def format_datetime(self, x):
        try:
            dt = pd.to_datetime(x, format=self.l.datetime_format)
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

        # drop rows with empty datetime or empty change
        df.dropna(subset=['datetime', 'change'], inplace=True)

        # Drop 'cash sweep transfer' rows. These are transfers between the flatex bank account
        # and Degiro, and have no effect on the balance
        df=df[df['description'].map(lambda d: not re.match(self.l.cst['re'], d))]

        # convert numbers in columns 'change' and 'balance'
        for ncol in ('change', 'balance', 'FX'):
            df[ncol] = df[ncol].map( lambda n: self.l.fmt_number(n))

        # Skipping rows with no change
        df=df[df['change'] != 0]

        # Copy orderid as a new column uuid
        df['uuid']=df['orderid']

        #for idx, row in df.iterrows():
        #    if D(row['change']) == 0 and not re.match(l.liquidity_fund['re'], row['description']):
        #        print(f"Null row: {row}")

        # Match currency exchanges and provide uuid if none
        exchanges = df[df['description'].map(lambda d: bool(re.match(self.l.change['re'], d)))]
        # ci1 cr1 ci1 cr2 are indices and rows of matching currency exchanges
        # we assume that 2 consecutive exchange lines belong to each other
        (ci1, cr1) = (None, None)
        for ci2, cr2 in exchanges.iterrows():
            if ci1 is None:
                (ci1, cr1) = (ci2, cr2)
                continue
            # Assume first row is base, second row is foreign
            (bi, b, fi, f) = (ci1, cr1, ci2, cr2)
            if b['c_change'] != self.currency:
                # False assumption, swap
                (bi, b, fi, f) = (fi, f, bi, b)
            if pd.isna(f['FX']):
                print(f'No FX for foreign exchange {fi+2}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            if f['datetime'] != b['datetime']:
                print(f'Conversion date mismatch in lines {bi+2} and {fi+2}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue
            conversion = b['change'] * f['FX']
            result = f['change']
            if abs(conversion+result)/b['change'] > self._conversion_tolerance:
                print(f'Bad conversion in lines {bi+2} and {fi+2}: {conversion} vs {-result}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            # check if uuid match
            if pd.isna(b['uuid']) != pd.isna(f['uuid']) or ( pd.notna(f['uuid']) and f['uuid'] != b['uuid']):
                print(f'Conversion orderid mismatch in lines {bi+2} and {fi+2}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue
            elif pd.isna(f['uuid']):
                # Generate uuid to match conversion later
                muuid=str(uuid.uuid1())
                df.loc[bi, 'uuid'] = muuid
                df.loc[fi, 'uuid'] = muuid

            df.loc[fi, '__price'] = Amount(-b['change'], b['c_change'])

            ci1 = None
            cr1 = None

        if ci1 is not None:
            print(f'Unmatched conversion at line {ci1+2}')

        # Match postings with no order id

        dfn = df[pd.isna(df['uuid'])]

        idx_change = None
        # Generate uuid for transactions without orderid
        for idx, row in dfn.iterrows():
            # liquidity fund price changes and fees: single line pro transaction
            d=row['description']
            if (re.match(self.l.liquidity_fund['re'], d)
                or
                re.match(self.l.fees['re'], d)
                or
                re.match(self.l.payout['re'], d)
                or
                re.match(self.l.interest['re'], d)
                or
                re.match(self.l.deposit['re'], d) ):
                df.loc[idx, 'uuid'] = str(uuid.uuid1())
                continue

            # print (f"No order ID: {row['datetime']} {row['isin']} {row['description']} {row['change']}")
            if re.match(self.l.dividend['re'], row['description']):
                # Lookup other legs of dividend transaction
                # 1. Dividend tax: ISIN match
                mdfn=dfn[(dfn['isin']==row['isin'])
                         & (dfn['datetime'] > row['datetime']-timedelta(days=31)) & (dfn['datetime'] < row['datetime']+timedelta(days=5) )
                         & (dfn['description'].map(lambda d: bool(re.match(self.l.dividend_tax['re'], d))))]
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
            if re.match(self.l.split['re'], row['description']):
                mdfn=dfn[(dfn['datetime']==row['datetime']) & (dfn['isin']==row['isin'])]
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    df.loc[midx, 'uuid'] = muuid
                continue
            if re.match(self.l.buy['re'], row['description']):
                # transition between exchanges: buy and sell the same amount for the same price
                mdfn=dfn[(dfn['datetime']==row['datetime']) & (dfn['isin']==row['isin'])
                         & (dfn['change']==-row['change']) & (dfn['c_change']==row['c_change'])]
                if 1 != len(mdfn.index):
                    print(f"Erroneous transfer match for {mdfn.index+2}")
                    continue

                # No affect for booking. Drop these rows.
                df.drop(index=idx, inplace=True)
                df.drop(index=mdfn.index, inplace=True)

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


        trtypes = [
            { 'd': 'Liquidity Fund Price Change', 'r': self.l.liquidity_fund, 'h': handle_lf_and_fees },
            { 'd': 'Fees',                        'r': self.l.fees,           'h': handle_lf_and_fees },
            { 'd': 'Deposit',                     'r': self.l.deposit,        'h': handle_deposit },
            { 'd': 'Buy',                         'r': self.l.buy,            'h': handle_buy },
            { 'd': 'Sell',                        'r': self.l.sell,           'h': handle_sell },
            { 'd': 'Interest',                    'r': self.l.interest,       'h': handle_lf_and_fees }
        ]


        postings = []

        it=df.iterrows()

        row=None

        while True:

            prev_row = row
            idx, row = next(it, [None, None])

            if row is not None and pd.isna(row['uuid']):
                print(f"unset uuid line={idx+2}")

            if idx is None or ( prev_row is not None and (pd.isna(prev_row['uuid']) or row['uuid'] != prev_row['uuid'])):
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


            if idx is None:
                # prev_row was the last
                break

            if re.match(self.l.deposit['re'], row['description']) and self.depositAccount is None:
                continue

            amount = Amount(row['change'],row['c_change'])

            price = row['__price'] if pd.notna(row['__price']) else None

            postings.append(data.Posting(self.liquidityAccount+':'+row['c_change'], amount, None, price, None, None ))

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

