# -*- coding: utf-8 -*-
from beancount.core.number import D, Decimal
import pandas as pd
from collections import namedtuple

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


def fmt_number(value: str) -> Decimal:
    if pd.isna(value):
        return None
    thousands_sep = '.'
    decimal_sep = ','
    return D(value.replace(thousands_sep, '').replace(decimal_sep, '.'))

datetime_format = '%d-%m-%Y %H:%M'

# this should be defined in an utility module
PD = namedtuple('PD', ['re', 'vals'], defaults=[None])

# Descriptors for various posting types to book them automatically

liquidity_fund = PD(re='^Geldmarktfonds ((Preisänderung)|(Umwandlung))')

fees = PD(re='^(Transaktionsgebühr)|(Gebühr für Realtimekurse)|(Einrichtung von Handelsmodalitäten)')

deposit = PD(re='((SOFORT )?Einzahlung)|(Auszahlung)')

buy = PD(
    re='^(AKTIENSPLIT: )?Kauf ([\d.]+) zu je ([\d,]+) (\w+)',
    vals=lambda m: {
        'price': fmt_number(m.group(3)),
        'quantity': fmt_number(m.group(2)),
        'currency': m.group(4),
        'split': bool(m.group(1))
    }
)

sell = PD(
    re='(((AKTIENSPLIT)|(AUSZAHLUNG ZERTIFIKAT)): )?Verkauf ([\d.]+) zu je ([\d,]+) (\w+)',
    vals=lambda m: {
        'price': fmt_number(m.group(6)),
        'quantity': fmt_number(m.group(5)),
        'currency': m.group(7),
        'split': bool(m.group(3))
    }
)

dividend = PD(re='((Dividende)|(Ausschüttung.*))$')

dividend_tax = PD(re='Dividendensteuer')

cst = PD(re='(flatex)|(Degiro) Cash Sweep Transfer')

interest = PD(re='Flatex Interest')

change = PD(re='Währungswechsel (\(Ausbuchung\)|\(Einbuchung\))')

payout = PD(re='AUSZAHLUNG ZERTIFIKAT')

split = PD(re='AKTIENSPLIT:')
