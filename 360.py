"""
2020年9月26日 360 kafka 数据转换问题
"""
import concurrent.futures
import copy
import json
import multiprocessing
import time

CPU_NUM = multiprocessing.cpu_count()


PLATFORM_MAPPING = {
            '': {
                    "os": "UNKNOWN",
                    "platform": "H5"
                    },
            'iOS': {
                "os": "iOS",
                "platform": "iOS"
            },
            'Andorid': {
                "os": "Andorid",
                "platform": "Andorid"
            }
        }


class DataConvert:
    """json data convert"""
    @staticmethod
    def convert(old_value: dict):
        """
        type: old_value dict
        rtype: new_value dict
        """
        flag_num = 100
        # new_value = copy.deepcopy(old_value)
        new_value = old_value
        new_value['location'] = {}

        if old_value['app_type'] == flag_num:
            new_value['common'] = PLATFORM_MAPPING.get(old_value['client']['user_agent'], {})
        new_value['common']['version'] = old_value.get('version', '')

        # 超过精度使用 from decimal import Decimal 转换
        new_value['location']['lat'] = float(old_value['latitude'])  # string to double
        new_value['location']['lng'] = float(old_value['longtitude'])  # string to double

        timeArray = time.strptime(old_value['log_time'], "%Y-%m-%d %H:%M:%S")
        new_value['timestampMs'] = int(time.mktime(timeArray))  # string YYYY-MM-DD HH:MM:SS to int

        # del keys
        del new_value['client']['user_agent']
        del new_value['latitude']
        del new_value['longtitude']
        del new_value['log_time']

        return new_value


class Product:
    """默认kafka生产服务没有问题"""
    def send(self, key: str, msg: str):
        pass


class Consumer:
    """默认kafka消费服务没有问题"""
    def __iter__(self):
        pass
        return self

    def __next__(self):
        pass
        return self


def handle_msg(proc_pool, max_num=1250):
    consumer = []  # KafkaConsumer
    container, i = [], 0
    for msg in consumer:
        if not isinstance(msg, dict):
            msg = json.loads(msg)
        container.append(msg)
        i += 1
        if i > max_num:
            proc_pool.submit(run, container)
            container, i = [], 0


def run(user_data_list: list):
    with concurrent.futures.ThreadPoolExecutor(max_workers=125) as executor:
        futures = [executor.submit(DataConvert.convert, item) for item in user_data_list]
        for future in concurrent.futures.as_completed(futures):
            # print(future.result())
            msg = future.result()
            producer = Product()  # KafkaProducer
            producer.send(msg.get('user_id'), json.dumps(msg))


def main():
    with concurrent.futures.ProcessPoolExecutor(max_workers=CPU_NUM) as proc_pool:
        for _ in range(CPU_NUM):
            handle_msg(proc_pool)


if __name__ == "__main__":
    from pprint import pprint
    old_value = {
        "app_package_name": "com.google.app",
        "app_type": 100,
        "app_uuid": "6e1343ff1942828",
        "auth_passed": True,
        "client": {
            "dev_manufacturer": "vivo",
            "dev_model": "vivo 1612",
            "user_agent": "iOS"
        },
        "country": "ID",
        "latitude": "120.3399",
        "log_time": "2020-08-15 00:00:02",
        "longtitude": "30.1211",
        "os_lang": "in_ID",
        "remote_addr": "110.139.149.102",
        "user_id": "8371792",
        "version": "3.3.1"
        }
    pprint(DataConvert.convert(old_value=old_value))

