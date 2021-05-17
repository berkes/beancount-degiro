# -*- coding: utf-8 -*-
from beancount.ingest import extract
from degiro import DegiroAccount

# example importer config for Degiro importer
# use with "bean-extract ConfigDegiro.py /path/to/Account.csv -f main.Ledgerfile


account = DegiroAccount(
    language='de', # defines regular expressions for transaction descriptions
                   # Feel free to add your

    currency = 'EUR',           # main currency

    LiquidityAccount       = 'Aktiva:Invest:Degiro',
    StocksAccount          = 'Aktiva:Invest:Aktien:Degiro',
    FeesAccount            = 'Ausgaben:Invest:Gebühren:Degiro',
    InterestAccount        = 'Ausgaben:Invest:Zins:Degiro',
    #InterestIncomeAccount = 'Einkommen:Invest:Zins:Degiro',
    PnLAccount             = 'Einkommen:Invest:GuV:Degiro',
    DivIncomeAccount       = 'Einkommen:Invest:Div',
    WhtAccount             = 'Ausgaben:Invest:Wht:Degiro',
    ExchangeRoundingErrorAccount = 'Ausgaben:Invest:Gebühren:Rundungsfehler',
    #DepositAccount         = 'Aktiva:DKB:Girokonto'        # put in your checkings account if you want deposit transactions


)

CONFIG = [account]
extract.HEADER = '' # remove unnesseccary terminal output

