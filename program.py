import sqlite3


db_path = 'C:\\Users\\b.karjoo\\Documents\\DataGrip 2017.1.5\\bin\\ExtractAlpha.sqlite'

def establish_connection():
    global con
    con = sqlite3.connect(db_path)
    return con

def get_curser():
    if not 'con' in locals():
        establish_connection()
    global con
    return con.cursor()

def execute_query(query):
    cur = get_curser()
    cur.execute(query)
    if query.lstrip().upper().startswith('SELECT'):
        return cur.fetchall()
    return None

def select_all_date_filter(table = 'tm',start_date = '2000-01-01',end_date = '2017-12-31'):
    query = "SELECT * FROM {2} WHERE date > date('{0}') AND date < date('{1}')".format(start_date,end_date,table)
    return execute_query(query)

def select_tm_where_ticker_date(ticker, date):
    query = "select tm1 from tm where ticker = '{0}' and date = date('{1}')".format(ticker,date)
    return execute_query(query)

def select_one_where_ticker_date(ticker, date, table = 'tm', value = 'tm1'):
    query = "select {3} from {2} where ticker = '{0}' and date = date('{1}')".format(ticker, date, table, value)
    return execute_query(query)

def select_eod_where_ticker_date(ticker, date):
    query = "select date, ticker, open, high, low, close, dividend, split FROM eodhq where ticker = '{0}' and date = date('{1}')".format(ticker,date)
    return execute_query(query)

def select_eod_into_the_future(ticker, date, days_plus = '+1 days'):
    query = "select date, ticker, open, high, low, close, dividend, split from eodhq where ticker = '{0}' and date >= date('{1}','{2}') ORDER BY date ASC LIMIT 1".format(ticker, date, days_plus)
    return execute_query(query)

def select_next_earnings_date(ticker, date):
    query = "select DateEntered from earnings_dates where TradeSymbol2 = '{0}' and DateEntered >= date('{1}') ORDER BY DateEntered ASC LIMIT 1".format(ticker,date)
    return execute_query(query)

def main():
    res = select_one_where_ticker_date('AAPL','2017-07-06','tress','TRESS')
    print(res[0][0])

if __name__ == '__main__':
    main()