# -*- coding: utf-8 -*-
from beancount.ingest import extract
from degiro import DegiroAccount

# example importer config for Degiro importer
# use with "bean-extract ConfigDegiro.py /path/to/Account.csv -f main.Ledgerfile


account = DegiroAccount(
    language='de', # defines regular expressions for transaction descriptions
                   # Feel free to add your

    currency = 'EUR',           # main currency

    LiquidityAccount       = 'Aktiva:Invest:Degiro:{currency}', # Available tokens: {currency}
    StocksAccount          = 'Aktiva:Invest:Aktien:Degiro',     # Available tokens: {isin}
    FeesAccount            = 'Ausgaben:Invest:Gebühren:Degiro:{currency}', # Available tokens: {currency}
    InterestAccount        = 'Ausgaben:Invest:Zins:Degiro',     # Available tokens: {currency}
    PnLAccount             = 'Einkommen:Invest:GuV:Degiro',     # Available tokens: {isin}, {currency}
    DivIncomeAccount       = 'Einkommen:Invest:Div',            # Available tokens: {isin}, {currency}
    WhtAccount             = 'Ausgaben:Invest:Wht:Degiro',      # Available tokens: {isin}, {currency}
    ExchangeRoundingErrorAccount = 'Ausgaben:Invest:Gebühren:Rundungsfehler', # Available tokens: {currency}
    # DepositAccount: put in your checkings account if you want deposit transactions
    #DepositAccount         = 'Aktiva:DKB:Girokonto'        # Available tokens: {currency}


)

CONFIG = [account]
extract.HEADER = '' # remove unnesseccary terminal output

