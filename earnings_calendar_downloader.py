from datetime import datetime, timedelta
import urllib2
import psycopg2
from bs4 import BeautifulSoup


try:
    conn = psycopg2.connect("dbname='trading' user='alisher' host='localhost' password=''")
    conn.autocommit = True
    cur = conn.cursor()
except Exception, e:
    print "I am unable to connect to the database %s" % e
    exit(0)

base_url = "http://biz.yahoo.com/research/earncal/%s.html"
beginning_date = datetime.strptime('01Jan1999', '%d%b%Y')
end_date = datetime.today()
day = timedelta(days=1)

while beginning_date <= end_date:
    beginning_date += day
    print "starting %s" % beginning_date
    url = base_url % beginning_date.strftime("%Y%m%d")
    try:
        request = urllib2.urlopen(url)
    except Exception, e:
        print "error. %s %s" % (beginning_date, e)
        continue

    html_doc = request.read()
    soup = BeautifulSoup(html_doc)

    tables = soup.find_all("table")

    found = False

    for table in tables:
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        tds = rows[1].find_all("td")
        if len(tds) < 1:
            continue

        if tds[0].text.lower().strip() == "company":
            rows = rows[2:]
            found = True
            break

    if not found:
        print "Table not found: %s" % url
        continue

    for row in rows:
        tds = row.find_all("td")
        if len(tds) != 4:
            continue
        symbol = tds[1].text.strip()
        if symbol == "":
            continue
        time = tds[2].text.strip().lower()
        if time == "" or time == "time not supplied":
            is_before_open = None
        elif time == "before market open":
            is_before_open = True
        elif time == "after market close":
            is_before_open = False
        elif "am" in time or "pm" in time:
            symbol_time = datetime.strptime(time, '%H:%M %p et')
            if 0 <= symbol_time.hour < 8:
                is_before_open = True
            elif 17 <= symbol_time.hour < 24:
                is_before_open = False
            else:
                is_before_open = True
        else:
            print "unknown time: %s, %s" % (url, symbol)
            continue

        cur.execute("SELECT id FROM companies WHERE lower(ticker) = '%s';" % symbol.lower())
        row = cur.fetchone()
        if not row or len(row) != 1:
            print "No company with this ticker", symbol.lower(), row
            continue
        raw_data = {
            "company_id": row[0],
            "date": beginning_date,
            "is_before_open": is_before_open
        }
        try:
            cur.execute("""INSERT INTO earnings_calendar (is_before_open, date, company_id) """
                        """VALUES (%(is_before_open)s, %(date)s, %(company_id)s)""", raw_data)
            print "    %s" % symbol

        except psycopg2.IntegrityError, e:
            print "could not insert %s, on %s: %s" % (symbol, beginning_date, e)










