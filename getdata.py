import os
os.chdir('/home/jasper/repos/Data')
import database as db
import pandas as pd


query = '''
        select timestamp as datetime,
                open,
                high,
                low,
                close,
                volume
        from prices.coins 
        where timestamp >= '2021-01-01'
        and timestamp < '2021-02-01'
        and coin_id = 1
        order by datetime
    '''

with db.get_connection() as con:
    df = pd.read_sql(query, con, parse_dates=['datetime'])
        

#df['datetime'] = df['datetime'].dt.tz_localize(None)    
df.to_csv('btcTZ.csv', index=False)
    
    
    
