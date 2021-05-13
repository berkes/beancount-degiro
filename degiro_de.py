# -*- coding: utf-8 -*-
from beancount.core.number import D, Decimal
import pandas as pd

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

liquidity_fund = { 're': '^Geldmarktfonds ((Preisänderung)|(Umwandlung))' }

fees = { 're' : '^(Transaktionsgebühr)|(Gebühr für Realtimekurse)|(Einrichtung von Handelsmodalitäten)' }

deposit = { 're' : '((SOFORT )?Einzahlung)|(Auszahlung)' }

buy = { 're': '^(AKTIENSPLIT: )?Kauf ([\d.]+) zu je ([\d,]+) (\w+)',
        'vals': lambda m: { 'price': fmt_number(m.group(3)),
                                   'quantity': fmt_number(m.group(2)),
                                   'currency': m.group(4)}
       }

sell = { 're' : '(((AKTIENSPLIT)|(AUSZAHLUNG ZERTIFIKAT)): )?Verkauf ([\d.]+) zu je ([\d,]+) (\w+)',
         'vals' : lambda m: { 'price': fmt_number(m.group(6)),
                              'quantity': fmt_number(m.group(5)),
                              'currency': m.group(7)}
         }

dividend = { 're': '((Dividende)|(Ausschüttung.*))$' }

dividend_tax = { 're': 'Dividendensteuer' }

cst = { 're': '(flatex)|(Degiro) Cash Sweep Transfer' }

interest = { 're': 'Flatex Interest' }

change = { 're': 'Währungswechsel (\(Ausbuchung\)|\(Einbuchung\))' }

payout = { 're': 'AUSZAHLUNG ZERTIFIKAT' }

split = { 're' : 'AKTIENSPLIT:' }
