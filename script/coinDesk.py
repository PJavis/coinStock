from datetime import datetime

from requests import get

URL = 'https://production.api.coindesk.com/v2/tb/price/ticker?assets='


def default(args):
    if len(args) == 0:
        raise Exception("Missing argument...")

    final_url = URL + args
    result = get(final_url)
    json_data = result.json()

    individual_symbols = args.split(',')

    results = []

    for s in individual_symbols:
        coin_data = json_data['data'][s]
        results.append({
            "iso": coin_data['iso'],
            "name": coin_data['name'],
            "date_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S%z"),
            "current_price": coin_data['ohlc']['c'],
            "open": coin_data['ohlc']['o'],
            "high": coin_data['ohlc']['h'],
            "low": coin_data['ohlc']['l'],
            "close": coin_data['ohlc']['c']
        })

    return results


data = default('BTC')
print(data)