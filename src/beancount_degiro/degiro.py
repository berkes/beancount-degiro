import pandas as pd

import logging
import sys
import re
from datetime import datetime, timedelta
from io import StringIO

from beancount.core import data
from beancount.core.amount import Amount
from beancount.core.number import D
from beancount.core import position
from beancount.ingest import importer

import uuid
from collections import namedtuple
from .stockutil import StockSearch
from .degiro_lang import DegiroLangInterface

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
    def __init__(self, language, LiquidityAccount, StocksAccount, SplitsAccount,
                 FeesAccount, InterestAccount,
                 PnLAccount, DivIncomeAccount, WhtAccount,
                 RoundingErrorAccount,
                 DepositAccount=None,
                 TickerCacheFile=None,
                 currency='EUR', file_encoding='utf-8' ):

        root=logging.getLogger()
        root.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
        root.addHandler(handler)

        self.l = None
        if language:
            self.l = language()
        if not self.l or not isinstance(self.l, DegiroLangInterface):
            logging.log(logging.ERROR, f'Unsupported or unset language {self.l}')

        self.currency = currency
        self.file_encoding = file_encoding

        self.liquidityAccount = LiquidityAccount
        self.stocksAccount = StocksAccount
        self.splitsAccount = SplitsAccount
        self.feesAccount = FeesAccount
        self.interestAccount = InterestAccount
        self.pnlAccount = PnLAccount
        self.divIncomeAccount = DivIncomeAccount
        self.whtAccount = WhtAccount
        self.depositAccount = DepositAccount
        self.roundingErrorAccount = RoundingErrorAccount
        self.tickerCacheFile = TickerCacheFile
        self._date_from = None
        self._date_to = None
        self._balance_amount = None
        self._balance_date = None

        self._fx_match_tolerance_percent = 2.0

    def name(self):
        return f'{self.__class__.__name__} importer'

    def identify(self, file_):
        # Check header line only
        with open(file_.name, encoding=self.file_encoding) as fd:
            return bool(re.match(','.join(self.l.fields), fd.readline()))

    def file_account(self, _):
        return self.liquidityAccount.format(currency=self.currency)

    def file_date(self, file_):
        # unfortunately, no simple way to determine file date from content.
        # fall back to file creation date.
        return None

    def extract(self, _file, existing_entries=None):

        def format_datetime(x):
            try:
                return pd.to_datetime(x, format=self.l.datetime_format)
            except Exception as e:
                # bad row with no date (will be sanitized later)
                return None

        with open(_file.name) as f:
            lines=f.readlines()

        if len(lines) < 1:
            logging.log(logging.ERROR, f'Empty input')
            return []
        header=lines[0]
        del lines[0]
        lines = [header] + list(reversed(lines))
        linecount = len(lines)

        # map index to line number
        def i2l(i:int):
            return linecount-i

        try:
            df = pd.read_csv(StringIO(''.join(lines)), encoding=self.file_encoding,
                             header=0, names=FIELDS_EN,
                             parse_dates={ 'datetime' : ['date', 'time'] },
                             date_parser = format_datetime,
                             converters = {
                                 'change':  self.l.fmt_number,
                                 'FX':      self.l.fmt_number,
                                 'balance': self.l.fmt_number
                             }
                             )
        except Exception as e:
            raise InvalidFormatError(f"Read file "+ _file.name + " failed " + e)

        # some rows are broken into more rows. Sanitize them now

        # put empty string if nan in these columns to ease sanitization below
        df.fillna(value={'orderid':'', 'product':'', 'description':''}, inplace=True)
        fraction=None
        for idx, row in df.iterrows():
            if pd.isna(row['datetime']):
                if fraction is not None:
                    logging.log(logging.WARNING, f'line={i2l(fi)} too many broken lines')
                fraction=row
                continue

            if fraction is not None:
                if pd.notna(fraction['product']):
                    df.at[idx, 'product'] += " "+fraction['product']
                if pd.notna(fraction['description']):
                    df.at[idx, 'description'] += " "+fraction['description']
                if pd.notna(fraction['orderid']):
                    df.at[idx, 'orderid'] += fraction['orderid']
                fraction=None

        # drop rows with empty datetime or empty change
        df.dropna(subset=['datetime', 'change'], inplace=True)

        # Drop 'cash sweep transfer' rows. These are transfers between the flatex bank account
        # and Degiro, and have no effect on the balance
        df=df[df['description'].map(lambda d: not self.l.cst(d))]

        # Copy orderid as a new column uuid
        df['uuid']=df['orderid']

        # Match currency exchanges and provide uuid if none
        exchanges = df[df['description'].map(lambda d: bool(self.l.change(d)))]
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
                logging.log(logging.WARNING, f'line={i2l(fi)} no FX for foreign exchange')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            if f['datetime'] != b['datetime']:
                logging.log(logging.WARNING, f'line={i2l(bi)} line={i2l(fi)} conversion date mismatch')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            # One of calculated and actual is negative; the sum should balance
            calculated = b['change'] * f['FX']
            actual = f['change']
            fx_error=calculated+actual  # expected to be 0.00 f['c_change']
            fx_error_percent=abs(fx_error/b['change']) * D(100.0)
            if fx_error_percent > self._fx_match_tolerance_percent:
                logging.log(logging.WARNING,
                    f'line={i2l(bi)} line={i2l(fi)} currency exchange match failed:\n'
                    f"  {abs(b['change'])} {b['c_change']} * {f['FX']} {f['c_change']}/{b['c_change']} != {abs(actual)} {f['c_change']} "
                    f'fx error: {fx_error_percent:.2f}% '
                    f'conversion tolerance: {self._fx_match_tolerance_percent:.2f}%')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue

            # check if uuid match
            if f['uuid'] != b['uuid']:
                logging.log(logging.WARNING, f'line={i2l(bi)} line={i2l(fi)} conversion orderid mismatch')
                # skip first row; continue with second
                (ci1, cr1) = (ci2, cr2)
                continue
            elif f['uuid'] == '':
                # Generate uuid to match conversion later
                muuid=str(uuid.uuid1())
                df.loc[bi, 'uuid'] = muuid
                df.loc[fi, 'uuid'] = muuid

            df.loc[bi, '__FX'] = Amount(f['FX'], f['c_change'])
            df.loc[fi, '__FX_corr'] = -fx_error

            ci1 = None
            cr1 = None

        if ci1 is not None:
            logging.log(logging.WARNING, f'line={i2l(ci1)} unmatched conversion')

        # Match postings with no order id

        dfn = df[df['uuid']=='']

        idx_split = None # Consecutive stock split rows are matched
        idx_isin_change = None # Consecutive ISIN change rows are matched
        # Generate uuid for transactions without orderid
        for idx, row in dfn.iterrows():
            # liquidity fund price changes and fees: single line pro transaction
            d=row['description']
            if (self.l.liquidity_fund(d)
                or
                self.l.fees(d)
                or
                self.l.payout(d)
                or
                self.l.interest(d)
                or
                self.l.deposit(d)):
                df.loc[idx, 'uuid'] = str(uuid.uuid1())
                continue

            if self.l.dividend(row['description']):
                # Lookup other legs of dividend transaction
                # 1. Dividend tax: ISIN match
                mdfn=dfn[(dfn['isin']==row['isin'])
                         & (dfn['datetime'] > row['datetime']-timedelta(days=31)) & (dfn['datetime'] < row['datetime']+timedelta(days=5) )
                         & (dfn['description'].map(lambda d: bool(self.l.dividend_tax(d))))]
                muuid=str(uuid.uuid1())
                for midx, mrow in mdfn.iterrows():
                    if df.loc[midx, 'uuid'] != '':
                        logging.log(logging.WARNING, f"line={i2l(midx)} ambigous generated uuid")
                    df.loc[midx, 'uuid'] = muuid
                if df.loc[idx, 'uuid'] != '':
                    logging.log(logging.WARNING, f"line={i2l(idx)} ambigous generated uuid")
                df.loc[idx, 'uuid'] = muuid
                continue
            if self.l.split(row['description']):
                if idx_split is None:
                    idx_split = idx
                    continue
                other_row=dfn.loc[idx_split]
                if other_row['datetime'] != row['datetime']:
                    logging.log(logging.WARNING, f"line={i2l(idx_split)} line={i2l(idx)} split matching failed")
                    idx_split=idx  # retry matching this row with following split row
                    continue
                muuid=str(uuid.uuid1())
                logging.log(logging.DEBUG, f"line={i2l(idx_split)} line={i2l(idx)} marking split uuid={muuid}")
                df.loc[idx_split, 'uuid'] = df.loc[idx, 'uuid'] = muuid
                idx_split=None
                continue
            if self.l.isin_change(row['description']):
                # ISIN Change of fonds: buy and sell the same amount for the same price
                if idx_isin_change is None:
                    idx_isin_change = idx
                    continue
                other_row=dfn.loc[idx_isin_change]
                if other_row['datetime'] != row['datetime'] or other_row['change'] != -row['change'] or other_row['c_change'] != row['c_change']:
                    logging.log(logging.WARNING, f"line={i2l(idx_isin_change)} line={i2l(idx)} ISIN change matching failed")
                    idx_isin_change=idx  # retry matching this row with following ISIN change row
                    continue
                muuid=str(uuid.uuid1())
                logging.log(logging.DEBUG, f"line={i2l(idx_isin_change)} line={i2l(idx)} marking ISIN change uuid={muuid}")
                df.loc[idx_isin_change, 'uuid'] = df.loc[idx, 'uuid'] = muuid
                idx_isin_change=None
                continue
            if self.l.buy(row['description']):
                # transition between exchanges: buy and sell the same amount for the same price
                mdfn=dfn[(dfn['datetime']==row['datetime']) & (dfn['isin']==row['isin'])
                         & (dfn['change']==-row['change']) & (dfn['c_change']==row['c_change'])]
                if 1 != len(mdfn.index):
                    logging.log(logging.WARNING, f"line={i2l(idx)} erroneous transfer match")
                    continue

                # No affect for booking. Drop these rows.
                df.drop(index=idx, inplace=True)
                df.drop(index=mdfn.index, inplace=True)

        stocks=StockSearch(self.tickerCacheFile)

        def add_corr(target, corr, currency):
            if currency not in target:
                target[currency] = 0
            target[currency] += corr

        def handle_fees(vals, row, amount, line, ctx):
            return 2, "Degiro", f"Fee: {row['description']}", \
                [data.Posting(self.feesAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_liquidity_fund(vals, row, amount, line, ctx):
            return 2, "Degiro", "Liquidity fund price change", \
                [data.Posting(self.interestAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_interest(vals, row, amount, line, ctx):
            return 2, "Degiro", f"Interest: {row['description']}", \
                [data.Posting(self.interestAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_deposit(vals, row, amount, line, ctx):
            if self.depositAccount is None:
                # shall not happen anyway
                return PRIO_LAST, "", []
            return 2, "self", "Deposit/Withdrawal",  \
                [data.Posting(self.depositAccount.format(currency=amount.currency), -amount, None, None, None, None )]

        def handle_dividend(vals, row, amount, line, ctx):
            ticker=stocks.isin2ticker(row['isin'])
            return 1, row['isin'], f"Dividend {ticker}", \
                [
                    data.Posting(self.divIncomeAccount.format(currency=amount.currency, isin=row['isin'], ticker=ticker),
                                 -amount, None, None, None, None )
                ]

        def handle_dividend_tax(vals, row, amount, line, ctx):
            ticker=stocks.isin2ticker(row['isin'])
            return 2, row['isin'], f"Dividend tax {ticker}", \
                [
                    data.Posting(self.whtAccount.format(currency=amount.currency, isin=row['isin'], ticker=ticker),
                                 -amount, None, None, None, None )
                ]

        def handle_change(vals, row, amount, line, ctx):
            # Cumulate FX correction for use in transaction
            if pd.notna(row['__FX_corr']):
                add_corr(ctx['corr'], row['__FX_corr'], row['c_change'])
            # No extra posting; currency exchange has already two legs in the cvs
            # Just make a pretty description
            return 2, "Degiro", f"Currency exchange", []

        def handle_buy(vals, row, amount, line, ctx):
            cost = position.CostSpec(
                number_per=vals.price,
                number_total=None,
                currency=vals.currency,
                date=row['datetime'].date(),
                label=None,
                merge=False)

            ticker=stocks.isin2ticker(row['isin'])
            stockamount = Amount(vals.quantity,ticker)

            if vals.split:
                account = self.splitsAccount
                tdesc=f"SPLIT {row['product']}"
            elif vals.isin_change:
                account = self.stocksAccount
                tdesc=f"ISIN CHANGE {row['product']} {ticker}"
            else:
                account = self.stocksAccount
                tdesc=f"BUY {row['product']} {stockamount.number} {ticker} @ {vals.price} {vals.currency}"

            # calculate total cost rounding error
            if (vals.currency != row['c_change']):
                logging.log(logging.WARNING, f"line={line} currency price:{vals.currency}, change:{row['c_change']} mismatch")
            else:
                corr=-(vals.quantity * vals.price + row['change'])
                add_corr(ctx['corr'], corr, row['c_change'])

            return 1, ticker, tdesc, \
                [data.Posting(account.format(isin=row['isin'], ticker=ticker), stockamount, cost, None, None, None )]

        def handle_sell(vals, row, amount, line, ctx):

            ticker=stocks.isin2ticker(row['isin'])
            stockamount = Amount(-vals.quantity, ticker)

            cost=position.CostSpec(
                number_per=None,
                number_total=None,
                currency=None,
                date=None,
                label=None,
                merge=False)

            sellPrice=Amount(vals.price, vals.currency)

            if vals.split:
                account = self.splitsAccount
                tdesc=f"SPLIT {row['product']} {ticker}"
            elif vals.isin_change:
                account = self.stocksAccount
                tdesc=f"ISIN CHANGE {row['product']} {ticker}"
            else:
                account = self.stocksAccount
                tdesc=f"SELL {row['product']} {stockamount.number} {ticker} @ {vals.price} {vals.currency}"

            postings = [data.Posting(account.format(isin=row['isin'], ticker=ticker),
                                     stockamount, cost, sellPrice, None, None)]
            if not ctx['pnl']:
                # pnl posting append only once per transaction
                ctx['pnl'] = True
                postings.append(data.Posting(self.pnlAccount.format(currency=row['c_change'], isin=row['isin'], ticker=ticker),
                                             None, None, None, None, None))

            # calculate total cost rounding error
            if (vals.currency != row['c_change']):
                logging.log(logging.WARNING, f"line={line} currency price:{vals.currency}, change:{row['c_change']} mismatch")
            else:
                corr=-(-vals.quantity * vals.price + row['change'])
                add_corr(ctx['corr'], corr, row['c_change'])

            return 1, ticker, tdesc, postings

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
        def CTX_INIT():
            return {'corr': {}, 'bcorr': {}, 'pnl': False}
        ctx = CTX_INIT()

        balances={}

        entries = []

        while True:

            prev_row = row
            prev_idx = idx
            idx, row = next(it, [None, None])

            if row is not None and row['uuid'] == '':
                logging.log(logging.WARNING, f"line={i2l(idx)} no uuid description={row['description']}")

            if idx is None or ( prev_row is not None and (row['uuid'] != prev_row['uuid'])):
                # previous transaction completed
                for currency in ctx['corr']:
                    # Beancount ignores imprecision less than the half of least significant digit
                    if abs(ctx['corr'][currency]) >= 0.005:
                        postings.append(
                            data.Posting(
                                self.roundingErrorAccount.format(currency=currency),
                                Amount(ctx['corr'][currency], currency), None, None, None, None
                            )
                        )
                # now search for balance imprecisions
                for currency in ctx['bcorr']:
                    if ctx['bcorr'][currency] != 0:
                        postings.append(
                            data.Posting(
                                self.liquidityAccount.format(currency=currency),
                                Amount(ctx['bcorr'][currency], currency), None, None, None, None
                            )
                        )

                if postings:
                    uuid_meta = {'uuid':prev_row['uuid']}
                    # Use fake lineno meta prev_idx to keep order of entries
                    entries.append(data.Transaction(data.new_metadata(_file.name, prev_idx, uuid_meta),
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
                ctx=CTX_INIT()

            if idx is None:
                # prev_row was the last
                break

            # Check balance
            if row['c_balance'] in balances:
                #  Difference between reported and calculated account balance:
                bdiff = row['balance'] - (balances[row['c_balance']]['balance'] + row['change'])
                if bdiff != 0:
                    logging.log(logging.DEBUG, f"line={i2l(idx)} applying balance correction {bdiff} {row['c_balance']}")
                    add_corr(ctx['bcorr'], bdiff, row['c_balance'])
                    add_corr(ctx['corr'], -bdiff, row['c_balance'])

            # Use fake lineno meta idx to keep order of entries
            balances[row['c_balance']]={'line': idx, 'balance': row['balance'], 'date': row['datetime'].date()}

            if self.l.deposit(row['description']) and self.depositAccount is None:
                continue

            amount = Amount(row['change'],row['c_change'])

            fxprice = row['__FX'] if '__FX' in row and pd.notna(row['__FX']) else None

            postings.append(data.Posting(self.liquidityAccount.format(currency=row['c_change']), amount, None, fxprice, None, None ))

            match = False
            for t in trtypes:
                m=t.descriptor(row['description'])
                if m:
                    (np, npay, nd, npostings) = t.handler(m.vals, row, amount, i2l(idx), ctx)
                    postings += npostings
                    # Now set transaction description if posting is more important than the ones before
                    if np < prio:
                        payee=npay
                        description=nd
                        prio=np
                    match = True
                    break
            if not match:
                logging.log(logging.WARNING, f"line={i2l(idx)} no posting handler description={row['description']}")

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

        stocks.save_cache()
        return entries

