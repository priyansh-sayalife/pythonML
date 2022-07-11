import psycopg2
import pandas as pd
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

        sql_insert_query = """ INSERT INTO event_detector (eID, timestampDates, meterNumber, Duration, Volume, 
        TypeName) VALUES (%s,%s,%s,%s,%s,%s) """

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


async def read_events_marshal_file():
    df = pd.read_excel(r'/Users/priyanshrajput/Downloads/events_marshal.xlsx', usecols=['eID', 'Timestamp',
                                                                                        'meterNumber', 'Duration',
                                                                                        'Volume', 'Type'])
    # for value in df['Timestamp']:
    #     formate = str(value).replace('[', '{').replace(']', '}').replace("Timestamp(", "").replace(", freq='T')", "")
    #     print(formate)

    # print([(ix, k, v) for ix, row in df.iterrows() for k, v in row.items()])
    records = []
    for index, row in df.iterrows():
        formate = ""
        if row["Timestamp"] and len(row["Timestamp"]) > 0:
            value = row["Timestamp"]
            formate = str(value).replace('[', '{').replace(']', '}').replace("Timestamp(", "").replace(", freq='T')",
                                                                                                       "")
            if not formate.endswith("'}"):
                formate = formate + "'}"

        data = (row["eID"], formate, row["meterNumber"], row["Duration"], row["Volume"], row["Type"])
        records.append(data)
        # print("\n", row["eID"], ":", formate)

    result = pool.submit(asyncio.run, make_connection(records[2200:2300])).result()
    print('exiting synchronous_property', result)

    # print(records)
    # limit = int(len(records) / 100)
    # for index in range(0, limit):
    #     print((index * 100), ":", ((index + 1) * 100))
    #     result = pool.submit(asyncio.run, make_connection(records[(index * 100):((index + 1) * 100)])).result()
    #     print('exiting synchronous_property', result)


asyncio.run(read_events_marshal_file())
