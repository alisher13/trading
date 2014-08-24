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



companies = []
file_names = ["companies/companylist amex.csv", "companies/companylist nasdaq.csv", "companies/companylist nyse.csv"]
for file_name in file_names:
    if file_name == "companies/companylist amex.csv":
        exchange_name = "amex"
    elif file_name == "companies/companylist nasdaq.csv":
        exchange_name = "nasdaq"
    else:
        exchange_name = "nyse"

    with open(file_name) as f:
        csv_reader = csv.reader(f, delimiter=',', quotechar='"')
        i = 0
        for row in csv_reader:
            if i == 0:
                i += 1
                continue

            try:
                market_cap = long(row[3])
                last_sale = long(row[2])
                shares_outstanding = int(floor(market_cap / last_sale))
            except Exception, e:
                shares_outstanding = 1

            companies.append({
                "name": row[1],
                "ticker": row[0],
                "industry": row[7],
                "sector": row[6],
                "shares_outstanding": shares_outstanding,
                "exchange": exchange_name
            })



print len(companies)
print companies[0]





# tickers_list = []
# for i in range(0, len(companies)):
#     tickers_list.append(companies[i]["ticker"])
#
#
# tickers_list_string = str(tickers_list)

cur = conn.cursor()
cur.executemany("""INSERT INTO companies (name,ticker, industry, sector, shares_outstanding, exchange) VALUES (%(name)s, %(ticker)s, %(industry)s, %(sector)s, %(shares_outstanding)s, %(exchange)s)""", companies)



# i = -1
# s = -1
#
# for j, company in enumerate(companies):
#     if company[4] > s:
#         s = company[4]
#         i = j
# print companies[i]












