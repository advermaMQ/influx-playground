from influxdb_client import InfluxDBClient as InfluxDBClient_V2
from influxdb_client import Point
import requests

import time
import datetime
import math
import pprint
import os
import sys

client = None
measurement = 'sinwave'
token = os.environ.get("INFLUXDB_TOKEN")
org = os.environ.get("INFLUXDB_ORG")
dbname = os.environ.get("INFLUXDB_BUCKET")


def db_exists():
    '''returns True if the database exists'''

    # Note: This whole function can be replaced by client.buckets_api.find_bucket_by_name(dbname)

    dbs = client.buckets_api().find_buckets().buckets
    for db in dbs:
        if db.name == dbname:
            return db

def wait_for_server(host, port, nretries=5):
    '''wait for the server to come online for waiting_time, nretries times.'''
    url = 'http://{}:{}'.format(host, port)
    waiting_time = 1
    for i in range(nretries):
        try:
            requests.get(url)
            return 
        except requests.exceptions.ConnectionError:
            print('waiting for', url)
            time.sleep(waiting_time)
            waiting_time *= 2
            pass
    print('cannot connect to', url)
    sys.exit(1)

def connect_db(host, port, reset):
    '''connect to the database, and create it if it does not exist'''
    global client
    url = 'http://{}:{}'.format(host,port)
    print('connecting to database: {}'.format(url))
    client = InfluxDBClient_V2(url=url, token=token, org=org)
    wait_for_server(host, port)
    buckets = client.buckets_api()
    db = db_exists()
    if db is not None:
        buckets.delete_bucket(db)
    print('creating database...')
    client.buckets_api().create_bucket(bucket_name=dbname)    # Not sure these lines are needed for v2
    # client.switch_database(dbname)
    # if not create and reset:
    #     client.delete_series(measurement=measurement)

    
def measure(nmeas=0):
    '''insert dummy measurements to the db.
    nmeas = 0 means : insert measurements forever. 
    '''
    write_api = client.write_api()
    i = 0
    if nmeas==0:
        nmeas = sys.maxsize
    for i in range(nmeas):
        x = i/10
        y = math.sin(x)
        data = [{
            'measurement':measurement,
            'time':datetime.datetime.now(),
            'tags': {
                'input' : x
                },
            'fields' : {
                'output' : y
                },
            }]
        point = Point(f"{measurement}").tag("input", x).field("output", y).time(datetime.datetime.now())
        try:
            res=write_api.write(bucket=dbname, org=org, record=point)
            pprint.pprint(point)
            print(res)
        except Exception as e:
            print(f"ERROR in writing record: {data}")
            sys.exit(-1)

        time.sleep(2)

def get_entries():
    '''returns all entries in the database.'''
    query_api = client.query_api()
    
    query_string = f'''from(bucket: "{dbname}")
        |> range(start:-30d)'''
    try:
        tables = query_api.query(query_string)
    except Exception as e:
        print(f"ERROR in reading record from bucket: {dbname}")
        sys.exit(-1)
    
    results = []
    
    for table in tables:
        # from influxdb_client.client.flux_table import FluxStructureEncoder
        # jsontemp = FluxStructureEncoder().default(table)
        for record in table.records:
            results.append({'time': record.get_time(), 
                            f'{record.get_field()}': record.get_value(),
                            'input': record['input'],
                            'table': record['table']
                            })
    return results

    
if __name__ == '__main__':
    
    from optparse import OptionParser
    parser = OptionParser('%prog [OPTIONS] <host> <port>')
    parser.add_option(
        '-r', '--reset', dest='reset',
        help='reset database',
        default=False,
        action='store_true'
        )
    parser.add_option(
        '-n', '--nmeasurements', dest='nmeasurements',
        type='int', 
        help='reset database',
        default=5
        )
    
    options, args = parser.parse_args()
    if len(args)!=2:
        parser.print_usage()
        print('please specify two arguments')
        sys.exit(1)
    host, port = args
    connect_db(host, port, options.reset)

    measure(options.nmeasurements)

    print("\n\n\n")
    print("*******************************************")
    
    pprint.pprint(get_entries())

