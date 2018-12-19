use flow_logs;
add jar file:///home/hadoop/uber-jar.jar;

create external table if not exists ip_traffic_by_minute 
row format serde 'org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer'
with serdeproperties (
    'serialization.class' = 'edu.uchicago.ddevere.IpAddressByMinute.model.IpTrafficByMinute',
    'serialization.format' = 'org.apache.thrift.protocol.TBinaryProtocol'
)
stored as sequencefile
location "s3://${hiveconf:b}/sequence-files/ip_traffic_by_minute/";


create external table if not exists ip_traffic_by_minute_hbase (
    `ipAddress` string, 
    `inBytes` map<string,bigint>,
    `inCount` map<string,int>,
    `outBytes` map<string,bigint>,
    `outCount` map<string,int>
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,inBytes:,inCount:,outBytes:,outCount:')
TBLPROPERTIES ('hbase.table.name' = 'ip_traffic_by_minute');

insert overwrite table ip_traffic_by_minute_hbase 
select `ipAddress`, map(cast(`minute` as string), `inBytes`), map(cast(`minute` as string),`inCount`), map(cast(`minute` as string), `outBytes`), map(cast(`minute` as string), `outCount`)
from ip_traffic_by_minute;

create external table if not exists top_ip_addresses (
    `ipAddress` string,
    `count` bigint
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,count:val')
TBLPROPERTIES ('hbase.table.name' = 'top_ip_addresses');

insert overwrite table top_ip_addresses 
select `ipAddress`, sum(`inCount` + `outCount`) as cnt from ip_traffic group by `ipAddress` order by cnt desc limit 500;