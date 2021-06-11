# -*- coding: utf-8 -*-
from beancount.core.number import D, Decimal
import pandas as pd
from collections import namedtuple
import re

class DR:
    match = None
    vals = None
    def __bool__(self):
        return bool(self.match)

VALS = namedtuple('VALS', ['price', 'quantity', 'currency', 'split'], defaults=[False])

class DegiroDE:
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


    def fmt_number(self, value: str) -> Decimal:
        if value == '':
            return None
        thousands_sep = '.'
        decimal_sep = ','
        return D(value.replace(thousands_sep, '').replace(decimal_sep, '.'))

    datetime_format = '%d-%m-%Y %H:%M'

    def process(self, r, d, v=None):
        dr=DR()
        dr.match=re.match(r, d)
        if dr and v:
            dr.vals=v(dr.match)
        return dr

    # Descriptors for various posting types to book them automatically

    def liquidity_fund(self, d):
        return self.process('^Geldmarktfonds (Preisänderung|Umwandlung)', d)

    def fees(self, d):
        return self.process('^Transaktionsgebühr|(Gebühr für Realtimekurse)|(Einrichtung von Handelsmodalitäten)', d)

    def deposit(self, d):
        return self.process('(((SOFORT|flatex) )?Einzahlung)|(Auszahlung)', d)

    def buy(self, d):
        return self.process('^(AKTIENSPLIT: )?Kauf ([\d.]+) zu je ([\d,]+) (\w+)', d,
                            lambda m:
                            VALS(price=self.fmt_number(m.group(3)), quantity=self.fmt_number(m.group(2)),
                                currency=m.group(4),split=bool(m.group(1)))
                            )

    def sell(self, d):
        return self.process('(((AKTIENSPLIT)|(AUSZAHLUNG ZERTIFIKAT)): )?Verkauf ([\d.]+) zu je ([\d,]+) (\w+)', d,
                            lambda m:
                            VALS(price=self.fmt_number(m.group(6)), quantity=self.fmt_number(m.group(5)),
                                 currency=m.group(7), split=bool(m.group(3)))
                            )

    def dividend(self, d):
        return self.process('(Dividende|(Ausschüttung.*))$', d)

    def dividend_tax(self, d):
        return self.process('Dividendensteuer', d)

    def cst(self, d):
        return self.process('(flatex|Degiro) Cash Sweep Transfer', d)

    def interest(self, d):
        return self.process('Flatex Interest', d)

    def change(self, d):
        return self.process('Währungswechsel (\(Ausbuchung\)|\(Einbuchung\))', d)

    def payout(self, d):
        return self.process('AUSZAHLUNG ZERTIFIKAT', d)

    def split(self, d):
        return self.process('AKTIENSPLIT:', d)
