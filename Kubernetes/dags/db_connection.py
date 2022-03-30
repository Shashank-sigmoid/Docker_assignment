import psycopg2


def create_connection():
    """ Connect to the PostgresSQL database server """
    cur = None
    conn = None

    def connect():
        # DB variables
        db_host = "postgres-service-db"
        db_name = "airflow"
        db_user = "airflow"
        db_pass = "airflow"
        db_port = '5432'

        # Connecting with the postgres server database
        print("Connecting to the PostgresSQL database...")
        conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
        cur = conn.cursor()
        return cur, conn

    def add_data(cur, conn):
        query = """ create table if not exists exec_time as select dag_id, execution_date from dag_run order by execution_date; """
        cur.execute(query)
        conn.commit()
        print("Data is added into the table successfully...")

    try:
        cur, conn = connect()
        add_data(cur, conn)

    except:
        print("Error in the connection...")

    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    create_connection()
