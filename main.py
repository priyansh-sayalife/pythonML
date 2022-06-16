# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.\

import psycopg2
import pandas as pd
import datetime
import config

columns = ['meterNumber', 'meterLocalTime', 'flow', "Tod", "Duration", "Volume"]

credential = []


def read_database_file():
    with open('configtext.text') as f:
        lines = f.readlines()
        print(lines)

    for line in lines:
        credential.append(str(line).replace('\n', ''))


def make_connection():
    read_database_file()
    global conn
    meter_numbers = []
    meter_local_time = []
    ifrs = []
    try:
        # establishing the connection
        conn = psycopg2.connect(
            database=credential[0], user=credential[1], password=credential[2],
            host=credential[3], port=credential[4])
        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()

        # Retrieving data
        query = """SELECT "meterNumber","meterLocalTime","flow" FROM "WaterMeterFlowReport" WHERE "meterNumber" = %s  LIMIT 50;"""
        # query = "SELECT * FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'WaterMeterFlowReport'"
        cursor.execute(query, ['18010116'])

        # Fetching 1st row from the table
        for record in cursor.fetchall():
            # print(record[0], ",", record[1], ",", record[2])
            meter_numbers.append(record[0])
            meter_local_time.append(str(record[1]))
            ifrs.append(record[2])

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # Closing the connection
        if conn:
            conn.close()
            print("PostgreSQL connection is closed")

    read_file(meter_numbers, meter_local_time, ifrs)


def read_file(meter_numbers, meter_local_time, ifrs):
    df = pd.read_excel(r'/Users/priyanshrajput/Downloads/MeterDetails.xlsx', usecols=['meterNumber', 'meterLocalTime',
                                                                                      'flow'])
    to_d = []
    volumes = []
    durations = []

    # Added Empty
    volumes.append("")
    durations.append("")

    for value in df['meterLocalTime']:
        to_d.append(date_format(str(value)))
    # print(to_d)

    for index, elem in enumerate(df['meterLocalTime']):

        diff_ifr = 0
        dif_durations = ""
        if index + 1 < len(df['meterLocalTime']) and index - 1 >= 0:
            # Find the ifr diff
            curr_ifr = df['flow'][(index - 1)]
            next_ifr = df['flow'][index]
            if curr_ifr > 0:
                if curr_ifr < next_ifr:
                    diff_ifr = next_ifr - curr_ifr
                else:
                    diff_ifr = curr_ifr - next_ifr
            # print("diff->", diff_ifr, "current ->", curr_ifr, "next ->", next_ifr)
            volumes.append(diff_ifr)
            # Find the date diff
            curr_date = get_date_from_string(str(df['meterLocalTime'][(index - 1)]))
            next_date = get_date_from_string(str(df['meterLocalTime'][index]))
            diff_date = next_date - curr_date
            diff_minutes = divmod(diff_date.total_seconds(), 60)
            star_date_diff = str(int(diff_minutes[0])) + ' minutes ' + str(int(diff_minutes[1])) + ' seconds'
            durations.append(star_date_diff)

    # Added Empty
    volumes.append("")
    durations.append("")

    data = []
    for idx, item in enumerate(meter_numbers):
        print(idx, item)
        row = []
        row.append(meter_numbers[idx])
        row.append(meter_local_time[idx])
        row.append(ifrs[idx])
        row.append(to_d[idx])
        row.append(durations[idx])
        row.append(volumes[idx])
        data.append(row)

    data = pd.DataFrame(data, columns=columns)
    data.to_csv('/Users/priyanshrajput/Downloads/MeterDetails_data.csv', index=False)

    # Create DataFrame from multiple lists
    # data = pd.DataFrame(list(zip(meter_numbers, meter_local_time, ifrs, to_d, durations, volumes)), columns=columns)

    # Write DataFrame to Excel file
    # writer = pd.ExcelWriter(r'/Users/priyanshrajput/Downloads/MeterDetails.xlsx', engine='xlsxwriter')
    # data.to_excel(writer, sheet_name='Sheet1')
    # writer.save()

    print(data)


def date_format(date_time):
    datem = datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    return datem.strftime("%p")


def get_date_from_string(date_time):
    return datetime.datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")


if __name__ == '__main__':
    make_connection()
    # read_file()
    # date_format("2021-05-28 01:30:00")

# Server=52.40.141.127;
# Port=5432;
# Database=SAYA;
# User Id=postgres;
# Password=test#123
