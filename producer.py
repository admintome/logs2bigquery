import time
import datetime
import json
from google.cloud import bigquery
from google.cloud import pubsub_v1


def sendto_bigquery(entry):
    pass


def parse_log_line(line):
    try:
        print("raw: {}".format(line))
        strptime = datetime.datetime.strptime
        temp_log = line.split(' ')
        entry = {}
        entry['ipaddress'] = temp_log[0]
        time = temp_log[3][1::]
        entry['time'] = strptime(
            time, "%d/%b/%Y:%H:%M:%S").strftime("%Y-%m-%d %H:%M")
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
        entry['ipaddress'],
        entry['time'],
        entry['request'],
        entry['status_code'],
        entry['size'],
        entry['client']))


def follow(syslog_file):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(
        'admintome-bigdata-test', 'www_logs')
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
                entry['ipaddress'],
                entry['time'],
                entry['request'],
                entry['status_code'],
                entry['size'],
                entry['client']
            )
            result = publisher.publish(topic_path, json.dumps(entry).encode())
            print("payload: {}".format(json.dumps(entry).encode()))
            print("Result: {}".format(result.result()))


f = open("/var/log/apache2/access.log", "rt")
follow(f)
