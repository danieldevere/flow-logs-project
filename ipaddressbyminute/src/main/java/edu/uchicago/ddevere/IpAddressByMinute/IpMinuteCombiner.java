package edu.uchicago.ddevere.IpAddressByMinute;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.uchicago.ddevere.IpAddressByMinute.model.IpTrafficByMinute;

public class IpMinuteCombiner extends Reducer<Text, BytesWritable, Text, BytesWritable> {
	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,
			Reducer<Text, BytesWritable, Text, BytesWritable>.Context context) throws IOException, InterruptedException {
		TDeserializer des = new TDeserializer(new TBinaryProtocol.Factory());
		TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
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
			IpTrafficByMinute output = new IpTrafficByMinute(ipAddress, minute, inBytes, inCount, outBytes, outCount);
			context.write(key, new BytesWritable(ser.serialize(output)));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
