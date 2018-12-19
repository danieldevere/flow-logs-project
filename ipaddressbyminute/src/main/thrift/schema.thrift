namespace java edu.uchicago.ddevere.IpAddressByMinute.model

struct FlowLog {
    1: required string version,
    2: required string account_id,
    3: required string interface_id,
    4: required string srcaddr,
    5: required string dstaddr,
    6: required string srcport,
    7: required string dstport,
    8: required string protocol,
    9: required i32 packets,
    10: required i64 bytes,
    11: required i64 start,
    12: required i64 stop,
    13: required string action,
    14: required string log_status
}

struct IpTraffic {
    1: required string ipAddress,
    2: required i64 inBytes,
    3: required i32 inCount,
    4: required i64 outBytes,
    5: required i32 outCount,
    6: required i64 start,
    7: required i64 stop
}

struct IpTrafficByMinute {
    1: required string ipAddress,
    2: required i32 minute,
    3: required i64 inBytes,
    4: required i32 inCount,
    5: required i64 outBytes,
    6: required i32 outCount
}
