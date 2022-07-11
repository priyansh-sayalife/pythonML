import psycopg2
import csv
import asyncio, concurrent.futures

credential = []

pool = concurrent.futures.ThreadPoolExecutor()


def read_database_file():
    with open('configtext.text') as f:
        lines = f.readlines()
        print(lines)

    for line in lines:
        credential.append(str(line).replace('\n', ''))


async def make_connection(records):
    read_database_file()
    result = 0
    try:
        # establishing the connection
        conn = psycopg2.connect(
            database=credential[0], user=credential[1], password=credential[2],
            host=credential[3], port=credential[4])
        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()

        sql_insert_query = """ INSERT INTO anomaly_detector (meterLocalTime, fr, meterNUmber, id, flow, anomaly_score,
         anomaly_threshold, anomaly)
         VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """

        # executemany() to insert multiple rows
        cursor.executemany(sql_insert_query, records)
        conn.commit()
        print(cursor.rowcount, "Record inserted successfully into mobile table")
        result = cursor.rowcount

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # Closing the connection
        if conn:
            conn.close()
            print("PostgreSQL connection is closed")
        return result


def convert(l):
    return tuple(l)


async def read_enomalies_file():
    file = open('/Users/priyanshrajput/Downloads/anomalies_marshal - anomalies_marshal.csv')
    csvreader = csv.reader(file)
    filter_list = list(filter(lambda it: (it[-1] == '1' or it[-1] == '0'), csvreader))
    records = []
    for item in filter_list:
        records.append(convert(item))

    limit = int(len(records)/100)
    for index in range(0, limit):
        if (index*100) >= 127200:
            print((index*100), ":", ((index+1)*100))
            result = pool.submit(asyncio.run, make_connection(records[(index*100):((index+1)*100)])).result()
            print('exiting synchronous_property', result)


# if __name__ == '__main__':
#     make_connection()
asyncio.run(read_enomalies_file())


