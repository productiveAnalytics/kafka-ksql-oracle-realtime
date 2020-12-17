from confluent_kafka import Producer

""" 
TODO Oracle CDC is Change Data Capture
nifi load schema also used
update - push operation and row level data to the kafka topic, timestamps as well
delete primary key and some change attributes
data type not needed derive data type from schema registry
watermark for table -> cdc id some ops can happen within same second use cdc id as the differentiator
only last iteration is written to landing from nifi
conformance json expected output for the data. column data type inferred from schema registry
"""

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def main():
    p = Producer({'bootstrap.servers': 'mybroker1,mybroker2'})

    for data in data_source:
        # Trigger any available delivery report callbacks from previous produce() calls
        p.poll(0)

        # Asynchronously produce a message, the delivery report callback
        # will be triggered from poll() above, or flush() below, when the message has
        # been successfully delivered or failed permanently.
        p.produce('mytopic', data.encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()

if __name__ == '__main__'():
    main()
