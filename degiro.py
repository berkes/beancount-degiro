import pandas as pd

import csv
import re
from datetime import datetime, timedelta

from beancount.core import data
from beancount.core.amount import Amount

from beancount.core.number import D

from beancount.core import position

from beancount.ingest import importer
import warnings
import uuid
from collections import namedtuple

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
    def __init__(self, language, LiquidityAccount, StocksAccount, FeesAccount, InterestAccount,
                 PnLAccount, DivIncomeAccount, WhtAccount,
                 ExchangeRoundingErrorAccount,
                 DepositAccount=None,
                 currency='EUR', file_encoding='utf-8' ):
        self.language = language
        self.currency = currency
        self.file_encoding = file_encoding

        self.liquidityAccount = LiquidityAccount
        self.stocksAccount = StocksAccount
        self.feesAccount = FeesAccount
        self.interestAccount = InterestAccount
        self.pnlAccount = PnLAccount
        self.divIncomeAccount = DivIncomeAccount
        self.whtAccount = WhtAccount
        self.depositAccount = DepositAccount
        self.exchangeRoundingErrorAccount = ExchangeRoundingErrorAccount

        self._date_from = None
        self._date_to = None
        self._balance_amount = None
        self._balance_date = None

        self._fx_match_tolerance_percent = 2.0
        if self.language == 'de':
            self.l = degiro_de

    def name(self):
        return f'{self.__class__.__name__} importer'

    def identify(self, file_):
        # Check header line only
        with open(file_.name, encoding=self.file_encoding) as fd:
            return bool(re.match(','.join(self.l.FIELDS), fd.readline()))

    def file_account(self, _):
        return self.liquidityAccount.format(currency=self.currency)

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

    def extract(self, _file, existing_entries=None):
        entries = []

        try:
            df = pd.read_csv(_file.name, encoding=self.file_encoding,
                             header=0, names=FIELDS_EN,
                             parse_dates={ 'datetime' : ['date', 'time'] },
                             date_parser = self.format_datetime,
                             converters = {
                                 'change':  self.l.fmt_number,
                                 'FX':      self.l.fmt_number,
                                 'balance': self.l.fmt_number
                             }
                             )
        except Exception as e:
            raise InvalidFormatError(f"Read file "+ _file.name + " failed " + e)

        # some rows are broken into more rows. Sanitize them now

        # map index to line number
        def i2l(i:int):
            return i+2

        prev_idx=None
        for idx, row in df.iterrows():
            if pd.isna(row['datetime']):
                if prev_idx is None:
                    # First row is already broken
                    continue
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
        df=df[df['description'].map(lambda d: not re.match(self.l.cst.re, d))]


        # Skipping rows with no change
        df=df[df['change'] != 0]

        # Copy orderid as a new column uuid
        df['uuid']=df['orderid']

        #for idx, row in df.iterrows():
        #    if D(row['change']) == 0 and not re.match(l.liquidity_fund['re'], row['description']):
        #        print(f"Null row: {row}")

        # Match currency exchanges and provide uuid if none
        exchanges = df[df['description'].map(lambda d: bool(re.match(self.l.change.re, d)))]
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
                print(f'No FX for foreign exchange {i2l(fi)}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            if f['datetime'] != b['datetime']:
                print(f'Conversion date mismatch in lines {i2l(bi)} and {i2l(fi)}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            # One of calculated and actual is negative; the sum should balance
            calculated = b['change'] * f['FX']
            actual = f['change']
            fx_error=calculated+actual  # expected to be 0.00 f['c_change']
            fx_error_percent=abs(fx_error/b['change']) * D(100.0)
            if fx_error_percent > self._fx_match_tolerance_percent:
                print(f'{_file} Currency exchange match failed in lines {i2l(bi)} and {i2l(fi)}:\n'
                      f"{abs(b['change'])} {b['c_change']} * {f['FX']} {f['c_change']}/{b['c_change']} != {abs(actual)} {f['c_change']}\n"
                      f'fx error: {fx_error_percent:.2f} %\n'
                      f'conversion tolerance: {self._fx_match_tolerance_percent:.2f} %')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            # check if uuid match
            if pd.isna(b['uuid']) != pd.isna(f['uuid']) or ( pd.notna(f['uuid']) and f['uuid'] != b['uuid']):
                print(f'Conversion orderid mismatch in lines {i2l(bi)} and {i2l(fi)}')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue
            elif pd.isna(f['uuid']):
                # Generate uuid to match conversion later
                muuid=str(uuid.uuid1())
                df.loc[bi, 'uuid'] = muuid
                df.loc[fi, 'uuid'] = muuid

            df.loc[bi, '__FX'] = Amount(f['FX'], f['c_change'])
            if abs(fx_error) >= 0.01:
                # error at least 1 cent in whatever currency:
                # apply a correction posting to avoid balance error
                df.loc[fi, '__FX_corr'] = -fx_error

            ci1 = None
            cr1 = None

        if ci1 is not None:
            print(f'Unmatched conversion at line {i2l(ci1)}')

        # Match postings with no order id

        dfn = df[pd.isna(df['uuid'])]

        idx_change = None
        # Generate uuid for transactions without orderid
        for idx, row in dfn.iterrows():
            # liquidity fund price changes and fees: single line pro transaction
            d=row['description']
            if (re.match(self.l.liquidity_fund.re, d)
                or
                re.match(self.l.fees.re, d)
                or
                re.match(self.l.payout.re, d)
                or
                re.match(self.l.interest.re, d)
                or
                re.match(self.l.deposit.re, d) ):
                df.loc[idx, 'uuid'] = str(uuid.uuid1())
                continue

            # print (f"No order ID: {row['datetime']} {row['isin']} {row['description']} {row['change']}")
            if re.match(self.l.dividend.re, row['description']):
                # Lookup other legs of dividend transaction
                # 1. Dividend tax: ISIN match
                mdfn=dfn[(dfn['isin']==row['isin'])
                         & (dfn['datetime'] > row['datetime']-timedelta(days=31)) & (dfn['datetime'] < row['datetime']+timedelta(days=5) )
                         & (dfn['description'].map(lambda d: bool(re.match(self.l.dividend_tax.re, d))))]
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    if pd.notna(df.loc[midx, 'uuid']):
                        print(f"Ambigous generated uuid for line {i2l(midx)}")
                    #print(f"Setting uuid {muuid} for line {i2l(midx)}")
                    df.loc[midx, 'uuid'] = muuid
                if pd.notna(df.loc[idx, 'uuid']):
                    print(f"Ambigous generated uuid for line {i2l(idx)}")
                df.loc[idx, 'uuid'] = muuid
                continue
            if re.match(self.l.split.re, row['description']):
                mdfn=dfn[(dfn['datetime']==row['datetime']) & (dfn['isin']==row['isin'])]
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    df.loc[midx, 'uuid'] = muuid
                continue
            if re.match(self.l.buy.re, row['description']):
                # transition between exchanges: buy and sell the same amount for the same price
                mdfn=dfn[(dfn['datetime']==row['datetime']) & (dfn['isin']==row['isin'])
                         & (dfn['change']==-row['change']) & (dfn['c_change']==row['c_change'])]
                if 1 != len(mdfn.index):
                    print(f"Erroneous transfer match for {i2l(mdfn.index)}")
                    continue

                # No affect for booking. Drop these rows.
                df.drop(index=idx, inplace=True)
                df.drop(index=mdfn.index, inplace=True)

        def handle_fees(vals, row, amount ):
            return 2, "Degiro", f"Fee: {row['description']}", \
                [data.Posting(self.feesAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_liquidity_fund(vals, row, amount ):
            return 2, "Degiro", "Liquidity fund price change", \
                [data.Posting(self.interestAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_interest(vals, row, amount ):
            return 2, "Degiro", f"Interest: {row['description']}", \
                [data.Posting(self.interestAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_deposit(vals, row, amount):
            if self.depositAccount is None:
                # shall not happen anyway
                return PRIO_LAST, "", []
            return 2, "You", "Deposit/Withdrawal",  \
                [data.Posting(self.depositAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_dividend(vals, row, amount ):
            return 1, row['isin'], f"Dividend {row['isin']}", \
                [data.Posting(self.divIncomeAccount.format(currency=amount.currency, isin=row['isin']), -amount, None, None, None, None )]

        def handle_dividend_tax(vals, row, amount ):
            return 2, row['isin'], f"Dividend tax {row['isin']}", \
                [data.Posting(self.whtAccount.format(currency=amount.currency, isin=row['isin']), -amount, None, None, None, None )]

        def handle_change(vals, row, amount ):
            # No extra posting; currency exchange has already two legs in the cvs
            # Just make a pretty description
            return 2, "Degiro", f"Currency exchange", []

        def handle_buy(vals, row, amount):
            cost = position.CostSpec(
                number_per=vals['price'],
                number_total=None,
                currency=vals['currency'],
                date=row['datetime'].date(),
                label=None,
                merge=False)
            stockamount = Amount(vals['quantity'],row['isin'])

            return 1, row['isin'], f"SELL {stockamount.number} @ {stockamount.currency} @ {vals['price']} {vals['currency']}", \
                [data.Posting(self.stocksAccount.format(isin=row['isin']), stockamount, cost, None, None, None )]

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

            return 1, row['isin'], f"SELL {stockamount.number} {row['isin']} @ {vals['price']} {vals['currency']}", \
                [
                    data.Posting(self.stocksAccount.format(isin=row['isin']),
                                 stockamount, cost, sellPrice, None, None),
                    data.Posting(self.pnlAccount.format(currency=row['c_change'], isin=row['isin']),
                                 None,        None,      None, None, None)
                ]

        TT = namedtuple('TT', ['doc', 'descriptor', 'handler'])

        trtypes = [
            TT('Liquidity Fund Price Change', self.l.liquidity_fund, handle_liquidity_fund),
            TT('Fees',                        self.l.fees,           handle_fees),
            TT('Deposit',                     self.l.deposit,        handle_deposit),
            TT('Buy',                         self.l.buy,            handle_buy),
            TT('Sell',                        self.l.sell,           handle_sell),
            TT('Interest',                    self.l.interest,       handle_interest),
            TT('Dividend',                    self.l.dividend,       handle_dividend),
            TT('Dividend tax',                self.l.dividend_tax,   handle_dividend_tax),
            TT('Currency exchange',           self.l.change,         handle_change),
        ]


        postings = []

        it=df.iterrows()

        row=None
        idx=None

        PRIO_LAST=99
        NO_DESCRIPTION="<no description>"
        NO_PAYEE="<no payee>"
        prio=PRIO_LAST
        description=NO_DESCRIPTION
        payee=NO_PAYEE

        balances={}

        while True:

            prev_row = row
            prev_idx = idx
            idx, row = next(it, [None, None])

            if row is not None and not row['c_balance'] in balances:
                balances[row['c_balance']]={'line': i2l(idx), 'balance': row['balance'], 'date': row['datetime'].date()}

            if row is not None and pd.isna(row['uuid']):
                print(f"unset uuid line={i2l(idx)}")

            if idx is None or ( prev_row is not None and (pd.isna(prev_row['uuid']) or row['uuid'] != prev_row['uuid'])):
                # previous transaction completed
                if postings:

                    uuid_meta={}
                    if pd.notna(prev_row['uuid']):
                        uuid_meta = {'uuid':prev_row['uuid']}
                    entries.append(data.Transaction(data.new_metadata(_file.name,i2l(prev_idx), uuid_meta),
                                                    prev_row['datetime'].date(),
                                                    self.FLAG,
                                                    payee,
                                                    description,
                                                    data.EMPTY_SET, # tags
                                                    data.EMPTY_SET, # links
                                                    postings
                                                    ))
                    postings = []
                    prio = PRIO_LAST
                    description=NO_DESCRIPTION
                    payee=NO_PAYEE

            if idx is None:
                # prev_row was the last
                break

            if re.match(self.l.deposit.re, row['description']) and self.depositAccount is None:
                continue

            amount = Amount(row['change'],row['c_change'])

            fxprice = row['__FX'] if pd.notna(row['__FX']) else None

            postings.append(data.Posting(self.liquidityAccount.format(currency=row['c_change']), amount, None, fxprice, None, None ))

            if pd.notna(row['__FX_corr']):
                postings.append(
                    data.Posting(
                        self.exchangeRoundingErrorAccount.format(currency=row['c_change']),
                        Amount(row['__FX_corr'], row['c_change']), None, None, None, None
                    )
                )

            match = False
            for t in trtypes:
                m=re.match(t.descriptor.re, row['description'])
                if m:
                    mv = None
                    if t.descriptor.vals:
                        mv = t.descriptor.vals(m)
                    (np, npay, nd, npostings) = t.handler(mv, row, amount)
                    postings += npostings
                    if np < prio:
                        payee=npay
                        description=nd
                        prio=np
                    match = True
                    break

            if pd.isna(row['uuid']):
                print(f"Line {i2l(idx)} Unexpected description: {row['uuid']} {row['description']}")

        for bc in balances:
            b=balances[bc]
            entries.append(
                data.Balance(
                    data.new_metadata(_file.name, b['line']),
                    b['date'] + timedelta(days=1),
                    self.liquidityAccount.format(currency=bc),
                    Amount(b['balance'], bc),
                    None,
                    None,
                )
            )

        return entries

