package edu.uchicago.ddevere.IpAddressByMinute;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.orc.mapreduce.OrcInputFormat;

public class IpAddressByMinute {
	
	public static void main(String[] args) {
		Logger logger = Logger.getLogger(IpAddressByMinute.class);
		logger.info("start");
		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "IpAddressByMinuteJob");
			job.setJarByClass(IpAddressByMinute.class);
			job.setMapperClass(IpMinuteMapper.class);
			job.setReducerClass(IpMinuteCombiner.class);
			job.setCombinerClass(IpMinuteCombiner.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(BytesWritable.class);
			job.setInputFormatClass(OrcInputFormat.class);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			OrcInputFormat.setInputDirRecursive(job, true);
			OrcInputFormat.addInputPath(job, new Path(args[0]));
			logger.info("line 31");
//			job.setMapOutputKeyClass(Text.class);
//			job.setMapOutputValueClass(BytesWritable.class);
//			TableMapReduceUtil.initTableReducerJob("ip_traffic_by_minute", IpMinuteReducer.class, job);

			

			
			SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
			logger.info("starting job");
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			logger.info("job done");
		} catch(Exception e) {
			logger.error("error", e);
			e.printStackTrace();
		}
	}
}
