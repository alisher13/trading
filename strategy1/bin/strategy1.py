import psycopg2


try:
    conn = psycopg2.connect("dbname='trading' user='alisher' host='localhost' password=''")
    conn.autocommit = True

except Exception, e:
    print "I am unable to connect to the database %s" % e
    exit(0)

cur = conn.cursor()
cur.execute("""SELECT id FROM companies;""")
rows_companies = cur.fetchall()
id_list = [x[0] for x in rows_companies]

total_long = []
total_short = []


for company_id in id_list[1000:1100]:
    cur.execute("""SELECT date, adj_close FROM quotes WHERE quotes.company_id = %d AND quotes.date > '2014, 04, 01' ORDER BY date ASC;""" % company_id)
    rows_quotes = cur.fetchall()
    long = 0
    short = 0
    i = 5
    l = len(rows_quotes) - 5
    t = 0
    k = 0
    while i < l:
        # print rows_quotes[i]
        # print rows_quotes[i-1]
        # print rows_quotes[i-2]
        # print rows_quotes[i-3]
        # print '*' * 25
        # if rows_quotes[i - 1][1] < rows_quotes[i - 2][1] < rows_quotes[i - 3][1]: #513 times
        if rows_quotes[i - 1][1] < rows_quotes[i - 2][1] < rows_quotes[i - 3][1]:
            long += round(((rows_quotes[i+1][1] - rows_quotes[i-1][1])/(rows_quotes[i-1][1])), 3)
            i += 2
            t += 1
        elif rows_quotes[i - 1][1] > rows_quotes[i - 2][1] > rows_quotes[i - 3][1]:
            short += round(((rows_quotes[i-1][1] - rows_quotes[i+1][1])/(rows_quotes[i-1][1])), 3)
            i += 2
            k += 1
        else:
            i += 1

    total_long.append(long)
    total_short.append(short)
    total_gain = sum(total_long, 0) + sum(total_short, 0)

    print "id:", company_id, "  long:", long, "  short:", short, "  gain:", long + short, "  trades:", t, "  total days:", len(rows_quotes)
print "total long: ", sum(total_long, 0), "   total short: ", sum(total_short, 0), "   total_gain: ", total_gain


