# 360
数据开发机试题目
l 要求：请使用 Python 独立编码，提供代码的仓库链接即可
日志转换
从某个 kafka topic 中中获取日志，按一定要求转换为另外一种格式后输出到第二个
kafka topic 中，日志前后均为 JSON
转换有几种类型：
1. 只需要调整字段的位置
2. 需要同时调整字段位置和字段类型
3. 可能需要根据一个或者多个字段的值，映射为新位置的新值
在本题中初始日志 JSON 如下：
{
"app_package_name": "com.google.app",
"app_type": 100,
"app_uuid": "6e1343ff1942828",
"auth_passed": true,
"client": {
"dev_manufacturer": "vivo",
"dev_model": "vivo 1612",
"user_agent": ""
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
需要完成的调整包括：
老字段 类型 新字段 类型
version string common.version string
latitude string location.lat double
longtitude string location.lng double
log_time string YYYY-MM-DD HH:MM:SS timestampMs int
另外当 app_type 为 100 时，platform.user_agent 和 common.platform 映射关系为：
client.user_agent common.platform common.os
空 H5 UNKNOWN
iOS iOS iOS
Andorid Android Andorid
调整完成后的 JSON 为：
{
"app_package_name": "com.google.app",
"app_type": 100,
"app_uuid": "6e1343ff1942828",
"auth_passed": true,
"client": {
"dev_manufacturer": "vivo",
"dev_model": "vivo 1612"
},
"common": {
"os": "UNKNOWN",
"platform": "H5",
"version": "3.3.1"
},
"country": "ID",
"location": {
"lat": 120.3399,
"lng": 30.1211
},
"os_lang": "in_ID",
"remote_addr": "110.139.149.102",
"timestampMs": 1597420802000,
"user_id": "8371792",
"version": "3.3.1"
}
其他限定：
输入输出的 kafka 均有 6 个 partition，kafka 不会成为性能瓶颈。高峰期日志约 5000
条/秒，程序运行环境为一台 4 核 8G 的虚拟机，请注意按 user_id 保证日志收发的时
序一致
