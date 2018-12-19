package edu.uchicago.ddevere.IpAddressByMinute;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.orc.mapred.OrcStruct;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import edu.uchicago.ddevere.IpAddressByMinute.model.IpTraffic;
import edu.uchicago.ddevere.IpAddressByMinute.model.IpTrafficByMinute;

public class IpMinuteMapper extends Mapper<NullWritable, OrcStruct, Text, BytesWritable> {
	
	private Logger logger = Logger.getLogger(IpMinuteMapper.class);
	
	@Override
	protected void map(NullWritable key, OrcStruct value,
			Mapper<NullWritable, OrcStruct, Text, BytesWritable>.Context context)
			throws IOException, InterruptedException {
		logger.info("map start");
		TSerializer ser = new TSerializer(new TBinaryProtocol.Factory());
		try {
			
			Text ipAddress = (Text)value.getFieldValue("ipaddress");
			LongWritable inBytes = (LongWritable)value.getFieldValue("inbytes");
			IntWritable inCount = (IntWritable)value.getFieldValue("incount");
			LongWritable outBytes = (LongWritable)value.getFieldValue("outbytes");
			IntWritable outCount = (IntWritable)value.getFieldValue("outcount");
			LongWritable start = (LongWritable)value.getFieldValue("start");
			LongWritable stop = (LongWritable)value.getFieldValue("stop");
			IpTraffic traffic = new IpTraffic(ipAddress.toString(), inBytes.get(), inCount.get(), outBytes.get(), outCount.get(), start.get(), stop.get());
			
			
			BigDecimal startMinute = new BigDecimal(traffic.getStart()).divide(new BigDecimal("60"), RoundingMode.HALF_UP);
			BigDecimal stopMinute = new BigDecimal(traffic.getStop()).divide(new BigDecimal("60"), RoundingMode.HALF_UP);
			BigDecimal range = stopMinute.subtract(startMinute);
			if(range.compareTo(BigDecimal.ZERO) == 0) {
				IpTrafficByMinute byMinute = new IpTrafficByMinute(
						traffic.getIpAddress(), 
						startMinute.intValue(), 
						new BigDecimal(traffic.getInBytes()).intValue(), 
						traffic.getInCount(), 
						new BigDecimal(traffic.getOutBytes()).intValue(), 
						traffic.getOutCount());
				BytesWritable output = new BytesWritable(ser.serialize(byMinute));
				context.write(new Text(traffic.getIpAddress() + ":" + startMinute.longValue()), output);
			} else {
				for(int i = startMinute.intValue(); i < stopMinute.intValue(); i++) {
					IpTrafficByMinute byMinute = new IpTrafficByMinute(
							traffic.getIpAddress(), 
							i, 
							new BigDecimal(traffic.getInBytes()).divide(range, RoundingMode.HALF_UP).intValue(), 
							traffic.getInCount(), 
							new BigDecimal(traffic.getOutBytes()).divide(range, RoundingMode.HALF_UP).intValue(), 
							traffic.getOutCount());
					BytesWritable output = new BytesWritable(ser.serialize(byMinute));
					context.write(new Text(traffic.getIpAddress() + ":" + i), output);
				}
			}
		} catch(Exception e) {
			logger.error("error in mapper", e);
			e.printStackTrace();
		}
	}
}
