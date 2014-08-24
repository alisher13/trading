# http://real-chart.finance.yahoo.com/table.csv?s=AAPL&a=11&b=12&c=1980&d=6&e=23&f=2014&g=d&ignore=.csv
import csv
import psycopg2
import urllib2

try:
    conn = psycopg2.connect("dbname='trading' user='alisher' host='localhost' password=''")
    conn.autocommit = True
except Exception, e:
    print "I am unable to connect to the database %s" % e
    exit(0)


cur = conn.cursor()
cur.execute("""SELECT id, ticker FROM companies;""")

rows = cur.fetchall()

print rows

url = "http://real-chart.finance.yahoo.com/table.csv?s=%s&a=11&b=12&c=1980&d=6&e=23&f=2014&g=d&ignore=.csv"

for i, row in enumerate(rows[63:]):
    company_url = url % row[1]
    ticker = row[1].replace("/", "_")
    try:
        request = urllib2.urlopen(company_url)
    except Exception, e:
        print "error. %s %s" % (ticker, e)
    # print request.read()
    # lines = request.read().strip().split("\n")
    with open("yahoo_company_downloads/%s.csv" % ticker, 'w') as f:
        # for line in lines:
        f.write(request.read())

    print i, ticker

