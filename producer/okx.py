from datetime import datetime

from requests import get
import json
import sys

URL1 = 'https://www.okx.com/priapi/v5/market/currency-trend?baseCcy='
URL2 = '&quoteCcy=USD&isPremium=false&bar=5m&limit=288&t=1713626934187'


def default(args):
    if len(args) == 0:
        raise Exception("Missing argument...")

    symbols = args[0]
    print("Scrapping " + symbols)

    individual_symbols = symbols.split(',')

    results = []

    for s in individual_symbols:
        final_url = URL1 + s + URL2
        result = get(final_url)

        json_data = result.json()
        coin_data = json_data['data'][0][1]
        results.append({
            'name': s,
            'date_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S%z"),
            'current_price': coin_data
        })

    return results


def main():
    args = ['BTC,ETH,ETHFI,DOGE,ZETA,BNB,SHIB']
    default(args)


if __name__ == "__main__":
    main()
