import sys
import pg8000.native

# conn = pg8000.native.Connection(sys.argv[1])
conn = pg8000.native.Connection(
    database='postgres',
    user='postgres',
    host=sys.argv[1],
    password=sys.argv[2]
)
print(conn.run('select 1'))
