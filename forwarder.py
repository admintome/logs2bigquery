import time
import datetime
from google.cloud import bigquery


def sendto_bigquery(entry):
    pass


def parse_log_line(line):
    try:
        print("raw: {}".format(line))
        strptime = datetime.datetime.strptime
        temp_log = line.split(' ')
        entry = {}
        entry['ip'] = temp_log[0]
        time = temp_log[3][1::]
        entry['time'] = strptime(time, "%d/%b/%Y:%H:%M:%S")
        request = " ".join((temp_log[5], temp_log[6], temp_log[7]))
        entry['request'] = request
        entry['status_code'] = int(temp_log[8])
        entry['size'] = int(temp_log[9])
        entry['client'] = temp_log[11]
        return entry
    except ValueError:
        print("Got back a log line I don't understand: {}".format(line))
        return None


def show_entry(entry):
    print("ip: {} time: {} request: {} status_code: {} size: {} client: {}".format(
        entry['ip'],
        entry['time'],
        entry['request'],
        entry['status_code'],
        entry['size'],
        entry['client']))


def follow(syslog_file):
    client = bigquery.Client()
    dataset_ref = client.dataset('www_logs')
    table_ref = dataset_ref.table('access_logs')
    schema = [
        bigquery.SchemaField('ipaddress', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('time', 'TIMESTAMP', mode='REQUIRED'),
        bigquery.SchemaField('request', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('status_code', 'NUMERIC', mode='REQUIRED'),
        bigquery.SchemaField('size', 'NUMERIC', mode='REQUIRED'),
        bigquery.SchemaField('client', 'STRING', mode='REQUIRED')
    ]
    syslog_file.seek(0, 2)
    while True:
        line = syslog_file.readline()
        if not line:
            time.sleep(0.1)
            continue
        else:
            entry = parse_log_line(line)
            if not entry:
                continue
            show_entry(entry)
            row = (
                entry['ip'],
                entry['time'].strftime("%Y-%m-%d %H:%M"),
                entry['request'],
                entry['status_code'],
                entry['size'],
                entry['client']
            )
            error = client.insert_rows(
                table_ref, [row], selected_fields=schema)
            print("error: {}".format(error))


f = open("/var/log/apache2/access.log", "rt")
follow(f)
