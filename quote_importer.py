from math import floor
import csv
import psycopg2


try:
    conn = psycopg2.connect("dbname='trading' user='alisher' host='localhost' password=''")
    conn.autocommit = True
except Exception, e:
    print "I am unable to connect to the database %s" % e
    exit(0)


# cur = conn.cursor()
# cur.execute("""SELECT * FROM test;""")
#
# rows = cur.fetchall()
#
# print rows

cur = conn.cursor()
cur.execute("""SELECT id, ticker FROM companies ORDER BY id;""")

rows = cur.fetchall()


quote_csv = "yahoo_company_downloads/%s.csv"

for i, row in enumerate(rows):
    ticker = row[1].replace("/", "_")
    csv_file = quote_csv % ticker

    print i, row[1]
    cur.execute("""SELECT id FROM quotes WHERE company_id = %d LIMIT 1;""" % row[0])
    if len(cur.fetchall()) == 1:
        print "    skipping"
        continue

    with open(csv_file) as f:
        quotes = []
        csv_reader = csv.reader(f, delimiter=',', quotechar='"')
        i = 0
        for csv_row in csv_reader:
            if i == 0:
                i += 1
                continue
            try:
                quotes.append({
                    "date": csv_row[0],
                    "company_id": row[0],
                    "open": float(csv_row[1]),
                    "high": float(csv_row[2]),
                    "low": float(csv_row[3]),
                    "close": float(csv_row[4]),
                    "volume": int(csv_row[5]),
                    "adj_close": float(csv_row[6])
                })
            except Exception, e:
                print "error: %s in row %s, %s" % (row[1], csv_row, e)

    cur.executemany("""INSERT INTO quotes (date, open, high, low, close, volume, adj_close, company_id) """
                    """VALUES (%(date)s,%(open)s, %(high)s, %(low)s, %(close)s, %(volume)s, %(adj_close)s, %(company_id)s)""", quotes)





