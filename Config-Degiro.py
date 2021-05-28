# -*- coding: utf-8 -*-
from beancount.ingest import extract
from beancount_degiro import DegiroAccount, DegiroDE

# example importer config for Degiro importer
# use with "bean-extract ConfigDegiro.py /path/to/Account.csv


account = DegiroAccount(
    language = DegiroDE, # defines descriptors for transaction descriptions
                         # Feel free to add your favourite language to degiro_lang.py

    currency = 'EUR',    # main currency
                                                                              # Available tokens:
    LiquidityAccount       = 'Aktiva:Invest:Degiro:{currency}',               # {currency}
    StocksAccount          = 'Aktiva:Invest:Aktien:Degiro:{ticker}',          # {isin}, {ticker}
    SplitsAccount          = 'Aktiva:Invest:Aktiensplits:Degiro:{ticker}',    # {isin}, {ticker}
    FeesAccount            = 'Ausgaben:Invest:Gebühren:Degiro:{currency}',    # {currency}
    InterestAccount        = 'Ausgaben:Invest:Zins:Degiro',                   # {currency}
    PnLAccount             = 'Einkommen:Invest:GuV:Degiro',                   # {isin}, {ticker}, {currency}
    DivIncomeAccount       = 'Einkommen:Invest:Div',                          # {isin}, {ticker}, {currency}
    WhtAccount             = 'Ausgaben:Invest:Wht:Degiro',                    # {isin}, {ticker}, {currency}
    ExchangeRoundingErrorAccount = 'Ausgaben:Invest:Gebühren:Rundungsfehler', # {currency}

    # DepositAccount: put in your checkings account if you want deposit transactions
    #DepositAccount         = 'Aktiva:DKB:Girokonto'                          # {currency}
    # ticker cache speeds up automatic ISIN -> ticker mapping
    TickerCacheFile        = '.ticker_cache'
)

CONFIG = [account]
extract.HEADER = '' # remove unnesseccary terminal output

