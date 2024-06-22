import datetime as dt
from dateutil import tz
import json

from confluent_kafka import Consumer, KafkaError

EIA_TIMEZONE = tz.tzutc()
EIA_TIME_FORMAT = '%Y-%m-%dT%H'
# Eg. 'June 20 at 12PM'
DISPLAY_FORMAT = '%B %d at %I%p'

def _decode_message(msg):
    '''Deserialize the message from Kafka and return the status and details of the message.'''
    # Status is 'clean' or 'dirty', taken from the topic name.
    status = msg.topic().split('-')[0]
    details = None
    try:
        details = json.loads(msg.value().decode('utf-8'))
    except json.decoder.JSONDecodeError as e:
        print(f'Unable to decode "{msg}": {e}')

    return status, details

def report_clean_power():
    '''Consume messages from the clean-power Kafka topic and print them in a user-friendly format
    to the console.
    '''

    c = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'notification-group',
        'auto.offset.reset': 'earliest',
    })

    c.subscribe(['clean-power'])

    try:
        # Poll for messages continuously
        while True:
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                status, details = _decode_message(msg)

            if details:
                # Parse and convert UTC to local time
                the_time = dt.datetime.strptime(details['period'], EIA_TIME_FORMAT)
                the_time = the_time.replace(tzinfo=EIA_TIMEZONE).astimezone(tz=tz.tzlocal())
                pct_clean = "{0:.0%}".format(float(details['pct_clean']))

                user_msg = f"The power was {pct_clean} {status} for {details['name']} customers on "
                user_msg += f"{the_time.strftime(DISPLAY_FORMAT)}!"

                print(user_msg)

    except KeyboardInterrupt:
        pass

    finally:
        c.close()

if __name__ == '__main__':
    report_clean_power()
