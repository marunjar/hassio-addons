from cmath import log
import json
import logging
import hashlib
from copy import deepcopy
import re

VERSION = "0.2"
HA_PREFIX = "homeassistant/sensor"
STATE_PREFIX = "telegraf2ha"

logging.basicConfig(
    format='[%(asctime)s] %(levelname)-2s %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S')


class calc_measurement():
    def __init__(self, uid):
        self.id = uid
        self.__prev_value = 0
        self.__prev_t = 0.0

    def set_name(self, name):
        self.name = name
        self.name_calc = f"{self.name}_dt"

    def get_rate(self, value, time):
        delta = value - self.__prev_value
        rate = float(delta) / (time - self.__prev_t)

        self.__prev_value = value
        self.__prev_t = time

        # First time being called
        # no previous known value
        if value == delta:
            rate = 0.0

        return rate


class telegraf_parser():
    def __init__(self, transmit_callback, loglevel, cm_str_list, listen_topics) -> None:
        logging.getLogger().setLevel(loglevel)
        self.hosts = {}
        self.cm_dict = {}
        self.transmit_callback = transmit_callback
        self.lt_list = []

        for uid in cm_str_list.split(","):
            # Initialize a dict with the desired calculated values UIDs
            self.cm_dict[uid] = calc_measurement(uid)
        for t in listen_topics.split(","):
            # Initialize a dict with the desired calculated values UIDs
            try:
                self.lt_list += [re.compile(t)]
            except Exception as e:
                logging.error(f"Error compiling pattern for listen_topics {t}: {e}")

    def __get_host_name(self, jdata):
        # Build the host name of the current meassage
        host = jdata['tags']['host']
        return re.sub("[^a-zA-Z0-9_-]", "_", host)

    def __get_sensor_name(self, jdata):
        # Build up the sensor name
        sensor_name = jdata['name']

        # add more detailed name from tags
        ext_name = jdata['tags'].get('name', "")
        if ext_name: 
            if ext_name in sensor_name:
                pass
            elif sensor_name in ext_name:
                sensor_name = ext_name
            elif sensor_name != ext_name:
                sensor_name = sensor_name + "_" + ext_name

        # Use properties names to differentiate measurements with same name
        if len(jdata['tags']) > 1:
            sensor_name += ('_' + jdata['tags'].get('cpu', "")).rstrip("_")
            sensor_name += ('_' + jdata['tags'].get('device', "")).rstrip("_")
            sensor_name += ('_' + jdata['tags'].get('interface', "")).rstrip("_")
            sensor_name += ('_' + jdata['tags'].get('feature', "")).rstrip("_")

        # Append this unique suffix to differ same-sensor-named topics
        # that contain different tags, that confuse hassio
        uid = hashlib.sha1(str(self.jdata_recv['fields'].keys()).encode()).hexdigest()[0:2]
        sensor_name += f"_{uid}"

        return sensor_name

    def __get_unique_id(self, jdata, measurement_name):
            host_name = self.__get_host_name(jdata)
            sensor_name = self.__get_sensor_name(jdata)

            return f"{host_name}_{sensor_name}_{measurement_name}"

    def __get_measurements_list(self, jdata):
        return jdata['fields'].keys()

    def add_calc(self, jdata_o):
        jdata = deepcopy(jdata_o)
        for measurement_name in self.__get_measurements_list(jdata_o):

            uid = self.__get_unique_id(jdata, measurement_name)

            # Add calc sensor and calculated value
            if uid in self.cm_dict.keys():
                self.cm_dict[uid].set_name(measurement_name)

                value = jdata["fields"][self.cm_dict[uid].name]
                t = jdata["timestamp"]

                jdata["fields"][self.cm_dict[uid].name_calc] = self.cm_dict[uid].get_rate(value, t)

        return jdata


    def announce_new(self, host_name, sensor_name, jdata):
        # Add current host if unknown
        current_host, is_new_h = self.add_host(host_name)
        # Add unknown sensors to host
        current_sensor, is_new_s = current_host.add_sensor(sensor_name)
        # Add unknown measurements to each sensor 
        for measurement_name in self.__get_measurements_list(jdata):
            _, is_new_m = current_sensor.add_measurement(measurement_name, self.lt_list)

        if is_new_s and current_sensor.enabled:
            logging.info(f"Added sensor: {self.print(jdata)}")

        return (is_new_s | is_new_h | is_new_m), current_sensor

    def send(self, data):
        # Once all the unknown sensors are announced,
        # start sending their data only
        self.jdata_recv = json.loads(data.payload.decode())
        jdata = self.add_calc(self.jdata_recv)

        host_name = self.__get_host_name(jdata)
        sensor_name = self.__get_sensor_name(jdata)

        is_new, current_sensor = self.announce_new(host_name, sensor_name, jdata)

        if current_sensor.enabled:
            # current_sensor.announce()
            topic_data = f"{STATE_PREFIX}/{host_name}/{sensor_name}/data"
            self.transmit_callback(topic_data, json.dumps(jdata['fields']))

    def print(self, jdata):
        # jdata = json.loads(data.payload.decode())
        host_name = self.__get_host_name(jdata)
        sensor_name = self.__get_sensor_name(jdata)
        measurements = ""

        for measurement in self.__get_measurements_list(jdata):
            measurements += f"{measurement},"
        measurements = measurements.rstrip(",")

        return f"{STATE_PREFIX}/{host_name}/{sensor_name}/[{measurements}]" 

    def add_host(self, host_name):
        current_host = self.hosts.get(host_name)
        if current_host is None:
            current_host = host(self, host_name)
            self.hosts[host_name] = current_host
            return current_host, True

        return current_host, False

class host():
    def __init__(self, parent_listener, name) -> None:
        self.name = name
        self.sensors = {}
        self.parent_listener = parent_listener

        self.info = {}
        self.info["identifiers"] = "bridge_" + self.name
        self.info["model"] = "Telegraf 2 Home Assistant Bridge"
        self.info["name"] = self.name
        self.info["sw_version"] = VERSION
        self.info["manufacturer"] = "telegraf2ha"
        self.enabled = False
        logging.debug(f"Created host: {self.name}")

    def add_sensor(self, sensor_name):
        # To create the sensor name, also check for extra tags (for the case of disks for example)
        current_sensor = self.sensors.get(sensor_name)
        if current_sensor is None:
            current_sensor = sensor(self, sensor_name)
            self.sensors[sensor_name] = current_sensor
            return current_sensor, True

        return current_sensor, False


class sensor():
    def __init__(self, parent_host, name) -> None:
        self.name = name
        self.measurements = {}
        self.parent_host = parent_host
        self.enabled = False
        logging.debug(f"Created sensor: {self.name}")

    def add_measurement(self, measurement_name, lt_list):
        current_measurement = self.measurements.get(measurement_name)
        if current_measurement is None:
            current_measurement = measurement(self, measurement_name, lt_list)
            self.measurements[measurement_name] = current_measurement
            return current_measurement, current_measurement.enabled
        
        return current_measurement, False
    
    def announce(self):
        for measurement_name, current_measurement in self.measurements:
            current_measurement.announce()
    

class measurement():    
    def __init__(self, parent_sensor, name, lt_list) -> None:
        self.name = name
        self.parent_sensor = parent_sensor
        self.topic = f"{HA_PREFIX}/{self.parent_sensor.parent_host.name}/{self.parent_sensor.name}_{self.name}"
        self.uid = f"{self.parent_sensor.parent_host.name}_{self.parent_sensor.name}_{self.name}"
        self.full_name = f"{self.parent_sensor.name[0:-3]}_{self.name}"
        self.unit = self.parseUnit(self.full_name)
        self.clazz = self.parseClazz(self.full_name)
        self.enabled = False
        for prog in lt_list:
            if prog.search(self.topic) != None:
                self.enabled = True
                parent_sensor.enabled = True
                parent_sensor.parent_host.enabled = True
                break
        logging.debug(f"Created measurement: {self.name}, {self.topic}, enabled={self.enabled}")

        self.announce()


    def announce(self):
        if (self.enabled):
            config_payload = {
                # "~": self.topic,
                "name": f"{self.parent_sensor.parent_host.name}_{self.full_name}",
                "state_topic": f"{STATE_PREFIX}/{self.parent_sensor.parent_host.name}/{self.parent_sensor.name}/data",
                "device_class": self.clazz,
                "unit_of_measurement": self.unit,
                "icon": self.getIcon(self.full_name),
                "device": self.parent_sensor.parent_host.info,
                "unique_id": self.uid,
                "platform": "mqtt",
                # Make the template such that we can use the telegraph topic straight
                "value_template": f"{{{{ value_json.{self.name} | round(2) }}}}",
            }

            # If it is a new measumente, announce it to hassio
            self.parent_sensor.parent_host.parent_listener.transmit_callback(f"{self.topic}/config", json.dumps(config_payload), retain=False)
            logging.debug(f"Announce measurement: {self.name}, {self.topic}")

    def parseUnit(self, name):
        if (("_bytes" in name) or ("bytes_" in name)):
            return "B"
        if ("percent" in name):
            return "%"
        if ("_temp_c" in name):
            return "°C"
        else:
            return None

    def parseClazz(self, name):
        if (("_bytes" in name) or ("bytes_" in name)):
            return "data_size"
        if ("percent" in name):
            return None
        if ("_temp_c" in name):
            return "temperature"
        else:
            return None

    def getIcon(self, name):
        if ("_temp_c" in name):
            return "mdi:thermometer"
        if ("cpu_" in name):
            return "mdi:cpu-64-bit"
        if ("mem_" in name):
            return "mdi:memory"
        if ("disk_" in name):
            return "mdi:harddisk"
        if ("diskio_" in name):
            return "mdi:harddisk"
        if ("net_" in name):
            return "mdi:lan"
        if ("pf_" in name):
            return "mdi:lan"
        if ("smart_attribute_" in name):
            return "mdi:harddisk-plus"
        if ("smart_device_" in name):
            return "mdi:harddisk-plus"
        if ("zfs_" in name):
            return "mdi:harddisk-plus"
        else:
            return None
