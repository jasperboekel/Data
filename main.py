import asyncio
import database as db
from binance import AsyncClient, BinanceSocketManager
from scrape_binance import parse_multiplex_socket

coins = ['btcusdt@ticker', 'ethusdt@ticker']

async def main():
    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)
    ts = bm.multiplex_socket(streams=coins)
    with db.get_connection() as con:
        async with ts as tscm:
            while True:
                data = await tscm.recv()
                print(data)
                symbol = data['data']['s']
                df = parse_multiplex_socket(data)

                if df['timestamp'].dt.round(freq='1s').dt.strftime(date_format='%S').iloc[0] == '00':
                    notify_message = f"NOTIFY test, 'new {symbol}';"
                    db.execute_values(con, 'prices', 'coins', df, notify_message)
                
        await client.close_connection()

if __name__ == "__main__":

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

