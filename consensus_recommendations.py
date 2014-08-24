from datetime import datetime, timedelta, time
import datetime
import urllib2
from bs4 import BeautifulSoup
import psycopg2

try:
    conn = psycopg2.connect("dbname='trading' user='alisher' host='localhost' password=''")
    conn.autocommit = True
    cur = conn.cursor()
except Exception, e:
    print "I am unable to connect to the database %s" % e
    exit(0)


cur.execute("SELECT ticker FROM companies")
rows = cur.fetchall()
tickers_list = [x[0] for x in rows]

base_url = "http://investing.money.msn.com/investments/analyst-ratings/?symbol=%s"

raw_data = []

for i in range(0, len(tickers_list)):
    symbol = "mtrx"  # tickers_list[i]
    url = base_url % symbol

    try:
        request = urllib2.urlopen(url)
    except Exception, e:
        print "error. %s %s" % (url, e)

        continue

    html_doc = request.read()
    soup = BeautifulSoup(html_doc)

    analyst_ratings_table = soup.find("table", {'class': 'mnytbl'})
    ratings_tr = analyst_ratings_table.find_all("tr")[6]
    analyst_ratings_tds = ratings_tr.find_all("td")[1:]


    print

    cur.execute("SELECT id FROM companies WHERE lower(ticker) = '%s';" % symbol.lower())
    row = cur.fetchone()
    if not row or len(row) != 1:
        print "No company with this ticker", symbol.lower(), row
        continue

    company_id = row[0]
    #
    # cur.execute("SELECT date FROM earnings_calendar WHERE company_id = '%d' ORDER BY date DESC LIMIT 1;" % company_id)
    # row = cur.fetchone()
    # if not row or len(row) != 1:
    #     print "No date for %s" % symbol
    #     continue
    #
    # date = row[0]

    date = datetime.date.today()

    print date
    raw_data = []

    for j, td in enumerate(analyst_ratings_tds):
        current_date = date - timedelta(days=j*30)
        try:
            raw_data.append({
                "company_id": company_id,
                "date": current_date,
                "rating": float(td.text.strip())
            })
        except Exception, e:
            print "No rating data - %s: %s, %s" % (e, url, current_date)

    print raw_data
