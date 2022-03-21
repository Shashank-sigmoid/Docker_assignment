import psycopg2


def read_table_data():
    """ Reading the data from the table """
    cur = None
    conn = None

    def connect():
        # DB variables
        db_host = "localhost"
        db_name = "airflow"
        db_user = "airflow"
        db_pass = "airflow"
        db_port = '5432'

        # Connecting with the postgres server database
        print("Connecting to the PostgresSQL database...")
        conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
        cur = conn.cursor()
        return cur, conn

    def fetch_data(cur, conn):
        query = """ select * from exec_time; """
        cur.execute(query)
        data = cur.fetchall()
        for element in data:
            print(element)
        print("Contents of the table retrieved successfully...")

    try:
        cur, conn = connect()
        fetch_data(cur, conn)

    except:
        print("Error in the connection...")

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    read_table_data()