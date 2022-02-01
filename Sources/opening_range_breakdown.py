from email import message
import email
from locale import currency
from pyexpat.errors import messages
import sqlite3
import config
import smtplib, ssl
import alpaca_trade_api as tradeapi
from datetime import date
from main import strategy 
from alpaca_trade_api import TimeFrame

connection = sqlite3.connect("/Users/Gokay9U/Desktop/Stock-Bot/app.db")
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

context = ssl.create_default_context()    
    
cursor.execute("""
                   SELECT id FROM strategy WHERE name = 'opening_range_breakdown'
                   """)
    
strategy_id = cursor.fetchone()['id']

cursor.execute("""
                   SELECT symbol
                   FROM stock
                   JOIN stock_strategy ON stock_strategy.stock_id = stock.id
                   WHERE stock_strategy.strategy_id = ? 
                """,(strategy_id,))   

stocks = cursor.fetchall()
symbols = [stock['symbol'] for stock in stocks]

print(symbols)

current_date = date.today().isoformat()
start_minute_bar = f"{current_date} 01:30:00+03:00"
end_minute_bar = f"{current_date} 01:45:00+03:00"

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.URL)

orders = api.list_orders(status='all',after=current_date)
existing_order_symbols = [order.symbol for order in orders if order.status != 'canceled']
print(existing_order_symbols)

messages = [] #one mail will contain all messages
index=0
for symbol in symbols:
    minute_bars = api.get_bars(symbol, TimeFrame.Minute ,start=current_date,end=current_date).df
    while  minute_bars.empty:
        symbol = symbols[index+1]
        minute_bars = api.get_bars(symbol, TimeFrame.Minute ,start=start_minute_bar ,end=end_minute_bar).df
    
    index +=1                
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]
    opening_range_low = opening_range_bars['low'].min()
    opening_range_high = opening_range_bars['high'].max()
    opening_range = opening_range_high - opening_range_low
    
    after_opening_range_mask = minute_bars.index >= end_minute_bar
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]
    after_opening_range_breakdown = after_opening_range_bars[after_opening_range_bars['close'] < opening_range_bars['open']]
    
    if not after_opening_range_breakdown.empty:
        if symbol not in existing_order_symbols:
            limit_price = after_opening_range_breakdown.iloc[0]['close']
            
            message = (f"Selling short {symbol} at {limit_price}, closed below {opening_range_low} at \n\n{after_opening_range_breakdown.iloc[0]}\n\n")
            messages.append(message)
                
            print(message)
            try:
                api.submit_order(
                symbol=symbol,
                side='buy',
                type='limit',
                qty='100',
                time_in_force='day',
                limit_price='limit_price',
                order_class='bracket',
                take_profit=dict(
                    limit_price= limit_price - opening_range,
                ),
                stop_loss=dict(
                    stop_price=limit_price + opening_range,
                )
            )
            except Exception as ex:
                print(f"could not submit order {ex}")
    else:
        print(f"Already an order for {symbol}, skipping")
                
    print(symbol)
    print(minute_bars)
    
    if not after_opening_range_breakdown.empty:
        if symbol not in existing_order_symbols:
            limit_price = after_opening_range_breakdown.iloc[0]['close']
            
            message = (f"Selling short {symbol} at {limit_price}, closed below {opening_range_low} at \n\n{after_opening_range_breakdown.iloc[0]}\n\n")
            messages.append(message)
            print(message)
    
print(messages)

with smtplib.SMTP_SSL("smtp.gmail.com", config.EMAIL_PORT, context=context) as server:
    server.login(config.EMAIL_ADRESS, config.EMAIL_PASSWORD)
    email_message = f"Trade Notifications for{current_date}\n\n"
    email_message += "\n\n".join(messages)
    server.sendmail(config.EMAIL_ADRESS,config.EMAIL_ADRESS, email_message)
    