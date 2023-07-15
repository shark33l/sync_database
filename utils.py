import logging
from logging.handlers import TimedRotatingFileHandler
import os
import json
import socket
import struct
import binascii


# Create logger with necessary configuration
def create_logger(logger_name):
    dir_name = "logs/"
    try:
        os.makedirs(os.path.dirname(dir_name), exist_ok=True)
    except TypeError:
        os.makedirs(dir_name)

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("[%(asctime)s] [%(filename)s:%(name)s.%(funcName)s:%(lineno)d] %(levelname)s - %(message)s")

    # Handlers
    file_handler = TimedRotatingFileHandler(
        f'{dir_name}app.log',
        when='midnight',
        backupCount=14
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


# Function to get configs from json
def get_config(file):
    try:
        with open(file) as config_file:
            data = json.load(config_file)
        return data
    except Exception as error:
        raise error


# Convert IP to integer and Integer to IP
def convert_ip2int(ip_address):
    return struct.unpack("!I", socket.inet_aton(ip_address))[0]


def convert_int2ip(ip_address):
    return socket.inet_ntoa(struct.pack("!I", ip_address))


# Convert MAC Address to Varbinary (Remove ; and convert to decimal (UNHEX)) and vice versa
def convert_mac2int(mac_address):
    return binascii.unhexlify((mac_address.replace(':', '')))


def convert_int2mac(mac_address_int):
    mac_address_str = binascii.hexlify(mac_address_int).decode('ascii')

    # add : after every 2 char
    mac_address = ':'.join(format(s, '02x') for s in bytes.fromhex(mac_address_str))

    return mac_address
