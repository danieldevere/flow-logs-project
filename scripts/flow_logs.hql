create database if not exists flow_logs;
use flow_logs;

create external table if not exists flow_logs_raw (
    `version` string,
    `account_id` string,
    `interface_id` string,
    `srcaddr` string,
    `dstaddr` string,
    `srcport` string,
    `dstport` string,
    `protocol` string,
    `packets` int,
    `bytes` bigint,
    `start` bigint,
    `stop` bigint,
    `action` string,
    `log_status` string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'

with serdeproperties (
    "separatorChar" = "\ ",
    "quoteChar" = "\""
)
stored as textfile
location "${hiveconf:a}"
tblproperties ("skip.header.line.count" = "1");

create external table if not exists flow_logs (
    `version` string,
    `account_id` string,
    `interface_id` string,
    `srcaddr` string,
    `dstaddr` string,
    `srcport` string,
    `dstport` string,
    `protocol` string,
    `packets` int,
    `bytes` bigint,
    `start` bigint,
    `stop` bigint,
    `action` string,
    `log_status` string
)
stored as orc
location "s3://${hiveconf:b}/orc-tables/flow_logs/";



create external table if not exists ip_traffic (
    `ipAddress` string,
    `inBytes` bigint,
    `inCount` int,
    `outBytes` bigint,
    `outCount` int,
    `start` bigint,
    `stop` bigint
)
stored as orc
location "s3://${hiveconf:b}/orc-tables/ip_traffic/";

