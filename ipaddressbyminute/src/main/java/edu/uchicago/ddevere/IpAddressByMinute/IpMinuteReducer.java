package edu.uchicago.ddevere.IpAddressByMinute;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.uchicago.ddevere.IpAddressByMinute.model.IpTrafficByMinute;

public class IpMinuteReducer extends TableReducer<Text, BytesWritable, ImmutableBytesWritable> {
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			TableReducer<Text, BytesWritable, ImmutableBytesWritable>.Context context) throws IOException, InterruptedException {
		TDeserializer des = new TDeserializer(new TBinaryProtocol.Factory());
		String ipAddress = null;
		Integer minute = 0;
		Long inBytes = 0L;
		Integer inCount = 0;
		Long outBytes = 0L;
		Integer outCount = 0;
		try {
			for(BytesWritable value : values) {
				IpTrafficByMinute item = new IpTrafficByMinute();
				des.deserialize(item, value.getBytes());
				ipAddress = item.getIpAddress();
				minute = item.getMinute();
				inBytes += item.getInBytes();
				inCount += item.getInCount();
				outBytes += item.getOutBytes();
				outCount += item.getOutCount();
			}
			Put put = new Put(ipAddress.getBytes());
			put.addColumn("inBytes".getBytes(), minute.toString().getBytes(), inBytes.toString().getBytes());
			put.addColumn("inCount".getBytes(), minute.toString().getBytes(), inCount.toString().getBytes());
			put.addColumn("outBytes".getBytes(), minute.toString().getBytes(), outBytes.toString().getBytes());
			put.addColumn("outCount".getBytes(), minute.toString().getBytes(), outCount.toString().getBytes());
			context.write(new ImmutableBytesWritable(ipAddress.getBytes()), put);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
