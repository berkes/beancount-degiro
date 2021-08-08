# -*- coding: utf-8 -*-
from beancount.core.number import D, Decimal
import pandas as pd
from collections import namedtuple
import re
import abc

class DegiroLangInterface(abc.ABC):

    @property
    def fields(self):
        return self.FIELDS

    @property
    def datetime_format(self):
        return self.DATETIME_FORMAT

    @abc.abstractmethod
    def fmt_number(self, value: str) -> Decimal:
        pass

    @abc.abstractmethod
    def liquidity_fund(self, d):
        pass

    @abc.abstractmethod
    def fees(self, d):
        pass

    @abc.abstractmethod
    def deposit(self, d):
        pass

    @abc.abstractmethod
    def buy(self, d):
        pass

    @abc.abstractmethod
    def sell(self, d):
        pass

    @abc.abstractmethod
    def dividend(self, d):
        pass

    @abc.abstractmethod
    def dividend_tax(self, d):
        pass

    @abc.abstractmethod
    def cst(self, d):
        pass

    @abc.abstractmethod
    def interest(self, d):
        pass

    @abc.abstractmethod
    def change(self, d):
        pass

    @abc.abstractmethod
    def payout(self, d):
        pass

    @abc.abstractmethod
    def split(self, d):
        pass

    @abc.abstractmethod
    def isin_change(self, d):
        pass

class DR:
    match = None
    vals = None
    def __bool__(self):
        return bool(self.match)

VALS = namedtuple('VALS', ['price', 'quantity', 'currency', 'split', 'isin_change'], defaults=[False])

def process(r, d, v=None):
    dr=DR()
    dr.match=re.match(r, d)
    if dr and v:
        dr.vals=v(dr.match)
    return dr

class DegiroDE(DegiroLangInterface):
    def __str__(self):
        return 'Degiro German language module'

    FIELDS = (
        'Datum',
        'Uhrze',
        'Valutadatum',
        'Produkt',
        'ISIN',
        'Beschreibung',
        'FX',
        'Änderung', # Currency of change
        '',         # Amount of change
        'Saldo',    # Currency of balance
        '',         # Amount of balance
        'Order-ID'
    )

    DATETIME_FORMAT = '%d-%m-%Y %H:%M'

    def fmt_number(self, value: str) -> Decimal:
        if value == '':
            return None
        thousands_sep = '.'
        decimal_sep = ','
        return D(value.replace(thousands_sep, '').replace(decimal_sep, '.'))

    # Descriptors for various posting types to book them automatically

    def liquidity_fund(self, d):
        return process('^Geldmarktfonds (Preisänderung|Umwandlung)', d)

    def fees(self, d):
        return process('^Transaktionsgebühr|(Gebühr für Realtimekurse)|(Einrichtung von Handelsmodalitäten)', d)

    def deposit(self, d):
        return process('(((SOFORT|flatex) )?Einzahlung)|(Auszahlung)', d)

    def buy(self, d):
        return process('^((AKTIENSPLIT: )|(ISIN-ÄNDERUNG: ))?Kauf ([\d.]+) zu je ([\d,]+) (\w+)', d,
                            lambda m:
                            VALS(price=self.fmt_number(m.group(5)), quantity=self.fmt_number(m.group(4)),
                                 currency=m.group(6), split=bool(m.group(2)), isin_change=bool(m.group(3)))
                            )

    def sell(self, d):
        return process('(((AKTIENSPLIT)|(AUSZAHLUNG ZERTIFIKAT)|(ISIN-ÄNDERUNG)): )?Verkauf ([\d.]+) zu je ([\d,]+) (\w+)', d,
                            lambda m:
                            VALS(price=self.fmt_number(m.group(7)), quantity=self.fmt_number(m.group(6)),
                                 currency=m.group(8), split=bool(m.group(3)), isin_change=bool(m.group(5)))
                       )

    def dividend(self, d):
        return process('(Dividende|(Ausschüttung.*))$', d)

    def dividend_tax(self, d):
        return process('Dividendensteuer', d)

    def cst(self, d):
        return process('(flatex|Degiro) Cash Sweep Transfer', d)

    def interest(self, d):
        return process('Flatex Interest', d)

    def change(self, d):
        return process('Währungswechsel (\(Ausbuchung\)|\(Einbuchung\))', d)

    def payout(self, d):
        return process('AUSZAHLUNG ZERTIFIKAT', d)

    def split(self, d):
        return process('AKTIENSPLIT:', d)

    def isin_change(self, d):
        return process('ISIN-ÄNDERUNG', d)
