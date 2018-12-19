package edu.uchicago.ddevere.hbaserest.config;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.protobuf.ServiceException;

@Configuration
public class ContextConfig {
	
	@Bean
	public Connection connection() throws MasterNotRunningException, ZooKeeperConnectionException, IOException, ServiceException {
		org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
		String path = this.getClass().getClassLoader().getResource("hbase-site.xml").getPath();
		config.addResource(new Path(path));
		HBaseAdmin.checkHBaseAvailable(config);
		return ConnectionFactory.createConnection(config);
	}
}
