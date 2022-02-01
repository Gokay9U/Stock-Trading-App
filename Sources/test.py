from msilib.schema import SelfReg
from select import select
from unicodedata import name
import backtrader, pandas , sqlite3
from datetime import date, datetime, time, timedelta

class OpeningRangeBreakout(backtrader.Strategy):
    params = dict(
        num_operating_bars = 15
    )
    
    def __init__(self):
        self.openinig_range_low=0
        self.openinig_range_high=0
        self.openinig_range=0
        self.bought_today= False
        self.order = None
       
    def log(self, txt, dt=None):
        if dt is None:
            dt = self.datas[0].datetime.datetime()
            
            print('%s, %s' %(dt, txt))
            
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        if order.status in [order.Completed]:
            order.details = f"{order.excuted.price}, Cost:{order.executed.value}, Comm:{order.executed.comm}"
            
            if order.isbuy():
                self.log(f"BUY EXECUTED, Price: {order.details}")
            else: 
                self.log(f"SELL EXECUTED, Price: {order.details}")
                
        elif order.status in [order.Canceled, order.MArgin, order.Rejected]:
            self.log('Order Canceled')
            
        self.order = None
        
    def next(self):
        current_bar_datetime = self.data.num2date(self.data.datetime[0])
        previous_bar_datetime = self.data.num2date(self.data.datetime[-1])
        
        if current_bar_datetime.date() != previous_bar_datetime.date():
            self.openinig_range_low = self.data.low[0]
            self.openinig_range_high = self.data.high[0]
            self.bought_today = False
            
        opening_range_start_time = time(9, 30, 0)
        dt = datetime.combine(date.today(), opening_range_start_time) + timedelta(minutes=self.p.num_opening_bars)
        opening_range_end_time = dt.time
        
        if current_bar_datetime.time() >= opening_range_start_time\
            and current_bar_datetime.time() < opening_range_end_time:
                self.openinig_range_high = max(self.data.high[0], self.openinig_range_high)
                self.openinig_range_low = max(self.data.low[0], self.openinig_range_low)
                self.openinig_range = self.openinig_range_high - self.openinig_range_low
        else: 
            if self.order:
                return
            if self.position and (self.data.close[0] > (self.openinig_range_high + self.openinig_range)):
                self.close()
            
            if self.data.close[0] > self.openinig_range_high and not self.position and not self.bought_today:
                self.order = self.close()
                
            if self.position and (self.data.close[0] < (self.openinig_range_high - self.openinig_range)):
                self.order = self.close()
                
            if self.position and current_bar_datetime.time() >= time(15, 45 ,0):
                self.log(' RUNNING OUT OF TIME')
                self.close()
                
    def stop(self) :
        self.log('(Num Opening Bars %2d) Ending Value %.2f' % 
                 (self.params.num.opening_bars, self.broker.getvalue()))
        
        if self.broker.getvalue() > 130000:
            self.log('*** WIN TRADE***')
        
        if self.broker.getvalue() < 70000:
            self.log('*** LOSE TRADE***')
            
if name == '__main__':
    con = sqlite3.connect('/Users/Gokay9U/Desktop/Stock-Bot/app.db')
    con.row_factory = sqlite3.Row
    cursor = con.cursor()
    cursor.execute("""
                    SELECT DISTINCT(stock.id) as stock_id FROM stock_price_minute
                   """)
    stocks = cursor.fetchall() 
    
    for stock in stocks:
        print(f"== Testing {stock['stock_id']} ==")
        
        cerebro = backtrader.Cerebro()
        cerebro.broker.setcash(100000.0)
        cerebro.addsizer(backtrader.sizers.PercentSizer, percents = 95)
        
        dataframe = pandas.read_sql("""
                                   SELECT datetime , open , high, low, close, volume
                                   from stock_price_minute
                                   where stock_id = :stock_id
                                   and strftime('%H:%M:%S', datetime) >= '09:30:00
                                   and strftime('%H:%M:%S', datetime) < '16:00:00
                                   order by datetime asc
                                   """, con, params={"stok_id":stock['stock_id']}, index_col='datetime', parse_dates=['datetime'])
        
        data = backtrader.feeds.PandasData(dataname=dataframe)
        
        cerebro.adddata(data)
        cerebro.addstrategy(OpeningRangeBreakout)
        
        cerebro.run()