#!/usr/bin/python3

import logging
from paho.mqtt import client as mqtt_client
from parser import telegraf_parser
import argparse

###########################################################

def data_received(client, userdata, data):
    tp.send(data)

def data_transmit(topic, payload, retain=False):
    # logging.debug(f"Publishing to {topic} the payload {payload}")
    client.publish(topic, payload, retain=retain, qos=1)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT Broker!")
    else:
        logging.info("Failed to connect, return code %d\n", rc)


# logger = logging.getLogger(__name__)
logging.basicConfig(
    format='[%(asctime)s] %(levelname)-2s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


# Construct an argument parser
all_args = argparse.ArgumentParser()

# Add arguments to the parser
all_args.add_argument("--user", required=False)
all_args.add_argument("--pass", required=False)
all_args.add_argument("--broker-ip", required=False, default="192.168.1.5")
all_args.add_argument("--port", required=False, default=1883)
all_args.add_argument("--topic", required=False, default="telegraf/#")
all_args.add_argument("--calc", required=False, default="")
all_args.add_argument("--log-level", required=False, default="info")
all_args.add_argument("--listen-topics", required=False, default="")

args = vars(all_args.parse_args())

# configure logging
loglevel = logging.INFO
if args[log_level] == 'trace':
    loglevel = logging.NOTSET
elif args[log_level] == 'debug':
    loglevel = logging.DEBUG
elif args[log_level] == 'info':
    loglevel = logging.INFO
elif args[log_level] == 'warning':
    loglevel = logging.WARNING
elif args[log_level] == 'error':
    loglevel = logging.ERROR
elif args[log_level] == 'fatal':
    loglevel = logging.CRITICAL
logging.geLogger().setLevel(loglevel)

## Configure client
client = mqtt_client.Client("telegraf2mqtt")
# client.enable_logger(logger)
client.username_pw_set(args['user'], args['pass'])
client.on_connect = on_connect
client.on_message = data_received

# Connect to HA broker, and subscribe to telegraf topics
client.connect(args['broker_ip'], int(args['port']))
client.subscribe(args['topic'])

# Pass the data transmit callback and the list of
# values to calculate
tp = telegraf_parser(data_transmit, loglevel, args['calc'], args['listen_topics'])

logging.info("Setup finished")

client.loop_forever()
