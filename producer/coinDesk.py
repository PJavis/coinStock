from datetime import datetime

from requests import get

URL = 'https://production.api.coindesk.com/v2/tb/price/ticker?assets='


def default(args):
    if len(args) == 0:
        raise Exception("Missing argument...")

    symbols = args[0]
    final_url = URL + symbols
    result = get(final_url)
    json_data = result.json()

    individual_symbols = symbols.split(',')

    results = []

    for s in individual_symbols:
        coin_data = json_data['data'][s]
        results.append({
            'iso': coin_data['iso'],
            'name': coin_data['name'],
            'date_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S%z"),
            'current_price': coin_data['ohlc']['c'],
            'open': coin_data['ohlc']['o'],
            'high': coin_data['ohlc']['h'],
            'low': coin_data['ohlc']['l'],
            'close': coin_data['ohlc']['c']
        })

    return results


def main():
    args = ['BTC,ETH,ETHFI,DOGE,ZETA,BNB,SHIB']
    default(args)


if __name__ == "__main__":
    main()
