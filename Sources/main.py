import sqlite3
import config
import alpaca_trade_api as tradeapi
from sqlite3.dbapi2 import Connection, Cursor
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from datetime import date
from pandas import date_range 



app = FastAPI()
templates = Jinja2Templates(directory="templates") 
 
@app.get("/")
def index(request: Request):
     
     stock_filter = request.query_params.get('filter', False)
     connection = sqlite3.connect("/Users/Gokay9U/Desktop/Stock-Bot/app.db")
     connection.row_factory = sqlite3.Row
     cursor = connection.cursor()
     
     if stock_filter == 'new_closing_highs':
        cursor.execute("""
                       SELECT * FROM (
                         SELECT symbol, name, stock_id, max(close),date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id 
                         GROUP BY stock_id
                         ORDER BY symbol 
			     ) WHERE date = (SELECT MAX(date) FROM stock_price )
			     """)
     elif stock_filter == 'new_closing_lows':   
         cursor.execute("""
                       SELECT * FROM (
                         SELECT symbol, name, stock_id, min(close),date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id 
                         GROUP BY stock_id
                         ORDER BY symbol 
			     ) WHERE date = (SELECT MAX(date) FROM stock_price )
			     """)
     elif stock_filter == 'rsi_overbought':   
         cursor.execute("""
                         SELECT symbol, name, stock_id, date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id  
			          WHERE rsi_14 > 70
                         AND date = (SELECT MAX(date) FROM stock_price ) 
                         ORDER BY symbol
			     """)
     elif stock_filter == 'rsi_oversold':   
         cursor.execute("""
                         SELECT symbol, name, stock_id, date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id  
			          WHERE rsi_14 < 30
                         AND date = (SELECT MAX(date) FROM stock_price ) 
                         ORDER BY symbol
			     """)
     elif stock_filter == 'above_sma_20':   
         cursor.execute("""
                         SELECT symbol, name, stock_id, date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id  
			          WHERE close > sma_20
                         AND date = (SELECT MAX(date) FROM stock_price ) 
                         ORDER BY symbol
			     """)
     elif stock_filter == 'below_sma_20':   
         cursor.execute("""
                         SELECT symbol, name, stock_id, date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id  
			          WHERE close < sma_20
                         AND date = (SELECT MAX(date) FROM stock_price ) 
                         ORDER BY symbol
			     """)
     elif stock_filter == 'above_sma_50':   
         cursor.execute("""
                         SELECT symbol, name, stock_id, date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id  
			          WHERE close > sma_50
                         AND date = (SELECT MAX(date) FROM stock_price ) 
                         ORDER BY symbol
			     """)
     elif stock_filter == 'below_sma_50':   
         cursor.execute("""
                         SELECT symbol, name, stock_id, date
                         FROM stock_price JOIN stock ON stock.id = stock_price.stock_id  
			          WHERE close < sma_50
                         AND date = (SELECT MAX(date) FROM stock_price ) 
                         ORDER BY symbol
			     """)         
     else:
          cursor.execute("""
                    SELECT id, symbol, name FROM stock ORDER BY symbol
                    """)
     
     rows = cursor.fetchall()

     #date.today().isoformat()
     current_date = '2022-01-25'  #db de olan bi tarih 
     cursor.execute("""
                    SELECT symbol, rsi_14, sma_20, sma_50, close
                    FROM stock JOIN stock_price ON stock_price.stock_id = stock.id
                    WHERE date = (SELECT MAX(date) FROM stock_price )
                    """)
     
     indicator_rows = cursor.fetchall()
     indicator_values = {}
     
     for row in indicator_rows:
          indicator_values[row['symbol']] = row
           
     
     return templates.TemplateResponse("index.html",{"request": request, "stocks":rows, "indicator_values":indicator_values} )
     
    
@app.get("/stock/{symbol}")
def stock_detail(request: Request, symbol):
     
     connection = sqlite3.connect("/Users/Gokay9U/Desktop/Stock-Bot/app.db")
     connection.row_factory = sqlite3.Row
     cursor = connection.cursor()
     
     cursor.execute("""
                    SELECT * FROM strategy
                    """)
     strategies = cursor.fetchall()
    
     cursor.execute("""
                   SELECT id, symbol, name FROM stock WHERE symbol = ?
                   """, (symbol,))
     row = cursor.fetchone()
     
     cursor.execute("""
                    SELECT * FROM stock_price WHERE stock_id = ? ORDER BY date DESC
                    """, (row['id'],))
     prices = cursor.fetchall()
     return templates.TemplateResponse("stock_detail.html",{"request": request, "stock":row,"bars":prices,"strategies":strategies} )

@app.post("/apply_strategy")
def apply_strategy(strategy_id: int = Form(...), stock_id: int = Form(...)):
     connection = sqlite3.connect("/Users/Gokay9U/Desktop/Stock-Bot/app.db")
     connection.row_factory = sqlite3.Row
     cursor = connection.cursor()
     
     cursor.execute("""
                    INSERT INTO stock_strategy (stock_id, strategy_id) VALUES (?,?)
                    """,(stock_id,strategy_id))     
     connection.commit()
     
     return RedirectResponse(url=f"/strategy/{strategy_id}", status_code=303)

@app.get("/strategies")
def strategies(request: Request):
     connection = sqlite3.connect("/Users/Gokay9U/Desktop/Stock-Bot/app.db")
     connection.row_factory = sqlite3.Row
     cursor = connection.cursor()
     cursor.execute("""
                    SELECT * FROM strategy
                    """)
     strategies = cursor.fetchall()
     
     return templates.TemplateResponse("strategies.html",{"request": request, "strategies":strategies})

@app.get("/orders")
def orders(request: Request):
     api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.URL)

     orders = api.list_orders(status='all')

     return templates.TemplateResponse("orders.html",{"request": request, "orders":orders})

@app.get("/strategy/{strategy_id}")
def strategy(request: Request, strategy_id):
     connection = sqlite3.connect("/Users/Gokay9U/Desktop/Stock-Bot/app.db")
     connection.row_factory = sqlite3.Row

     cursor = connection.cursor()
     
     cursor.execute("""
                    SELECT id, name
                    FROM strategy
                    WHERE id = ?
                    """,(strategy_id,))
      
     strategy = cursor.fetchone()    
     
     cursor.execute("""
                    SELECT symbol, name
                    FROM stock JOIN stock_strategy ON stock_strategy.stock_id = stock.id
                    WHERE strategy_id = ?
                    """,(strategy_id,))
      
     selecteds = cursor.fetchall()


     return templates.TemplateResponse("strategy.html",{"request": request, "selecteds":selecteds,"strategy":strategy} )
