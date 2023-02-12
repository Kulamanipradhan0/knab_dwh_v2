import psycopg2 as pg
stage = pg.connect(host='localhost',
                    port='9000',
                    database='postgres',
                    user='postgres',
                    password='password')
print("You are connected to Stage database(User : postgres)")
