use flow_logs;
insert overwrite table flow_logs select * from flow_logs_raw where log_status != "NODATA";
insert into table ip_traffic select srcaddr as ipAddress, 0, 0, bytes as outBytes, 1, `start`, `stop` from flow_logs;
insert into ip_traffic select dstaddr as ipAddress, bytes as inBytes, 1, 0, 0, `start`, `stop` from flow_logs;