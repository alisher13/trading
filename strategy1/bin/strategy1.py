import psycopg2


try:
    conn = psycopg2.connect("dbname='trading' user='alisher' host='' password=''")
    conn.autocommit = True

except Exception, e:
    print "I am unable to connect to the database %s" % e
    exit(0)

cur = conn.cursor()
cur.execute("""SELECT id FROM companies;""")
rows_companies = cur.fetchall()
id_list = [x[0] for x in rows_companies]
# 1234567890

