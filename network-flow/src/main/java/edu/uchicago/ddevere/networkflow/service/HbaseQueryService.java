package edu.uchicago.ddevere.networkflow.service;

import java.time.ZonedDateTime;
import java.util.List;

import edu.uchicago.ddevere.networkflow.vo.Coordinate;
import edu.uchicago.ddevere.networkflow.vo.IpCount;

public interface HbaseQueryService {
	public List<Coordinate> findByIpAddress(String ipAddress, String field, Integer chunksize);
	public List<Coordinate> findByIpAddressWithinInterval(String ipAddress, String field, ZonedDateTime from, ZonedDateTime to, Integer chunksize);
	public List<IpCount> getAllIpAddresses();
	public List<String> getAllIpAddressesInInterval(ZonedDateTime from, ZonedDateTime to);
}
