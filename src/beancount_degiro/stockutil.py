import requests as r
import pickle
import logging
import re

class StockSearch(object):
    def __init__(self, cachefile = None):
        self.cachefile = cachefile
        self.cache = None
        self.dirty = False

    def save_cache(self):
        if self.dirty:
            if self.cachefile is not None:
                with open(self.cachefile,'wb') as cf:
                    logging.log(logging.INFO, 'Saving dump')
                    pickle.dump(self.cache, cf)

    def isin2ticker(self, isin):
        if self.cache is None:
            if self.cachefile is not None:
                # try to use cachefile
                try:
                    with open(self.cachefile,'rb') as cf:
                        self.cache = pickle.load(cf)
                except OSError as err:
                    logging.log(logging.INFO, f"Could not open {self.cachefile}: {err}")
                    self.cache = {}
            else:
                self.cache = {}

        if isin in self.cache:
            ticker=self.cache[isin]
            logging.log(logging.DEBUG, f"Reuse from cache: {isin}:{ticker}")
            return ticker
        js = {}
        try:
            url = "https://query2.finance.yahoo.com/v1/finance/search"
            params = {'q': isin, 'quotesCount': 1, 'newsCount': 0}
            headers = {'User-Agent': 'python'} # fake user agent
            logging.log(logging.INFO, f"Querying ISIN {isin}...")
            resp = r.get(url, headers=headers, params=params)
            js = resp.json()
        except Exception as e:
            logging.log(logging.WARNING, f"Querying ISIN {isin} failed: {e}")

        if not 'quotes' in js or len(js['quotes']) < 1:
            logging.log(logging.WARNING, f"ISIN {isin} not found")
            ticker=isin  # fallback
        else:
            ticker = js['quotes'][0]['symbol']
            ticker = re.sub('\.', '-', ticker)
            logging.log(logging.INFO, f"ISIN {isin} found, ticker: {ticker}")

        self.cache[isin]=ticker
        self.dirty = True
        return ticker

#        def isin2ticker(isin):
#            if isin in tickers:
#                return tickers[isin]
#
#            try:
#                sdata=search_stocks(by='isin', value=isin)
#            except RuntimeError:
#                # isin not found
#                return row['isin']
#            # (sdata)
#            # country      name             full_name          isin currency symbol
#            # 0  united states  ENGlobal  ENGlobal Corporation  US2933061069      USD    ENG
#            rules = { 'US': ['united states'] ,
#                      'DE': ['germany'],
#                      'CA': ['united states', 'canada'],
#                      'GB': ['germany', 'united kingdom'] }
#
#            prefix=isin[:2]
#            if prefix in rules:
#                for country in rules[prefix]:
#                    cd=sdata[sdata['country'] == country]
#                    if len(cd.index) == 0:
#                        # country not found; try next
#                        continue
#                    if len(cd.index) > 1:
#                        print("Ambigous ISIN transformation: {isin}")
#                        break
#                    ticker = cd['symbol'].iloc[0]
#                    tickers[isin] = ticker
#                    return ticker
#            # no translation possible
#            return isin
