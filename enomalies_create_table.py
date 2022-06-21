import psycopg2

credential = []


def read_database_file():
    with open('configtext.text') as f:
        lines = f.readlines()
        print(lines)

    for line in lines:
        credential.append(str(line).replace('\n', ''))


def make_connection():
    read_database_file()
    try:
        # establishing the connection
        conn = psycopg2.connect(
            database=credential[0], user=credential[1], password=credential[2],
            host=credential[3], port=credential[4])
        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()

        query = """CREATE TABLE anomaly_detector (uniqid serial PRIMARY KEY, id BIGINT, anomaly INTEGER,
        anomaly_score NUMERIC(20,14), anomaly_threshold NUMERIC(20,14), meterNUmber VARCHAR(15),
        meterLocalTime TIMESTAMP, flow NUMERIC(20,14), fr NUMERIC(20,14));"""
        #
        # query = """DROP Table anomaly_detector"""
        cursor.execute(query)
        print(cursor.rowcount)
        cursor.close()
        # commit the changes
        conn.commit()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # Closing the connection
        if conn:
            conn.close()
            print("PostgreSQL connection is closed")


if __name__ == '__main__':
    make_connection()
