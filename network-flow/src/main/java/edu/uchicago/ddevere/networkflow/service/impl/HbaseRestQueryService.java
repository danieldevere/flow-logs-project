package edu.uchicago.ddevere.networkflow.service.impl;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import edu.uchicago.ddevere.networkflow.config.ServiceConfig;
import edu.uchicago.ddevere.networkflow.service.HbaseQueryService;
import edu.uchicago.ddevere.networkflow.vo.Column;
import edu.uchicago.ddevere.networkflow.vo.Coordinate;
import edu.uchicago.ddevere.networkflow.vo.IpCount;
import edu.uchicago.ddevere.networkflow.vo.Row;

@Service
public class HbaseRestQueryService implements HbaseQueryService {
	
	
	
	private static final String batchTable = "ip_traffic_by_minute";
	
	private static final String speedTable = "ip_traffic_by_minute_speed";
	
	@Autowired
	private RestTemplate restTemplate;
	
	@Autowired
	private ServiceConfig config;
	
	@Override
	public List<Coordinate> findByIpAddress(String ipAddress, String field, Integer chunksize) {
		try {
			String url = config.getHbaseServer() + "/{table}?row={row}&family={family}";
			System.out.println(url);
			url = config.getHbaseServer() + "/getTime";
			System.out.println(url);
			ResponseEntity<String> res2 = restTemplate.getForEntity(url, String.class);
			System.out.println(res2.getBody());
			url = config.getHbaseServer() + "/{table}?row={row}&family={family}&timeRangeFrom={t1}&timeRangeTo={t2}";
			System.out.println(url);
			ResponseEntity<Row> response = restTemplate.getForEntity(url, Row.class, speedTable, ipAddress, field, res2.getBody(), Instant.now().getEpochSecond());
			System.out.println(response.getBody());
			Row row = addSpeedToBatch(response.getBody());
			return rowToCoordinates(row, chunksize);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private Row addSpeedToBatch(Row speed) {
//		if(speed == null || speed.getColumns() == null) {
//			return batch;
//		}
		Map<String, Column> updates = new HashMap<>();
		for(Column col : speed.getColumns()) {
			if(updates.containsKey(col.getQualifier())) {
				Column c = updates.get(col.getQualifier());
				c.setValue(String.valueOf(Long.valueOf(c.getValue()) + Long.valueOf(col.getValue())));
				updates.put(col.getQualifier(), c);
			} else {
				updates.put(col.getQualifier(), col);
			}
		}
		Row view = new Row(speed.getName(), new ArrayList<>());
		for(Map.Entry<String, Column> entry : updates.entrySet()) {
			view.getColumns().add(entry.getValue());
		}
		return view;
//		if(batch != null) {
//			for(Column col : batch.getColumns()) {
//				if(updates.containsKey(col.getQualifier())) {
//					col.setValue(String.valueOf(Long.valueOf(col.getValue()) + Long.valueOf(updates.get(col.getQualifier()).getValue())));
//					updates.remove(col.getQualifier());
//				}
//			}
//		} else {
//			
//		}
		
	}
	
	

	@Override
	public List<Coordinate> findByIpAddressWithinInterval(String ipAddress, String field, ZonedDateTime from, ZonedDateTime to, Integer chunksize) {
		try {
			String fromMinute = String.valueOf(from.toEpochSecond() / 60);
			String toMinute = String.valueOf(to.toEpochSecond() / 60);
			ResponseEntity<String> res = restTemplate.getForEntity(config.getHbaseServer() + "/getTime", String.class);
			ResponseEntity<Row> response = restTemplate.getForEntity(config.getHbaseServer() + "/{table}?row={row}&family={family}&fromMinute={fromMinute}&toMinute={toMinute}&timeRangeFrom={t1}&timeRangeTo={t2}", Row.class, speedTable, ipAddress, field, fromMinute, toMinute, res.getBody(), Instant.now().getEpochSecond());
			Row row = addSpeedToBatch(response.getBody());
			return rowToCoordinates(row, chunksize);
		} catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private List<Coordinate> rowToCoordinates(Row row, Integer chunksize) {
		if(row == null || row.getColumns() == null) {
			return null;
		}
		row.getColumns().sort(new Comparator<Column>() {

			@Override
			public int compare(Column a, Column b) {
				long al = Long.valueOf(a.getQualifier()).longValue();
				long bl = Long.valueOf(b.getQualifier()).longValue();
				if(al == bl) {
					return 0;
				} else if(al < bl) {
					return -1;
				} else {
					return 1;
				}
			}
		});
		Map<Long, Long> utilMap = new LinkedHashMap<>(row.getColumns().size() / chunksize);
		for(Column column : row.getColumns()) {
			BigDecimal chunk = new BigDecimal(column.getQualifier()).divide(new BigDecimal(chunksize), RoundingMode.HALF_UP);
			if(utilMap.containsKey(chunk.longValue())) {
				utilMap.put(chunk.longValue(), utilMap.get(chunk.longValue()) + Long.valueOf(column.getValue()));
			} else {
				utilMap.put(chunk.longValue(), Long.valueOf(column.getValue()));
			}
		}
		List<Coordinate> coordinates = new ArrayList<>();
		for(Map.Entry<Long, Long> entry : utilMap.entrySet()) {
			if(entry.getValue().longValue() == 0) {
				continue;
			}
			coordinates.add(new Coordinate(ZonedDateTime.ofInstant(Instant.ofEpochSecond(entry.getKey() * 60L * Long.valueOf(chunksize)), ZoneId.of("UTC")).withZoneSameInstant(ZoneId.of("America/Chicago")).toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), entry.getValue().toString()));
		}
//		coordinates.sort(new Comparator<Coordinate>() {
//
//			@Override
//			public int compare(Coordinate a, Coordinate b) {
//				if(a.getX().equals(b.getX())) {
//					return 0;
//				} else if(a.getX().isBefore(b.getX())) {
//					return -1;
//				} else {
//					return 1;
//				}
//			}
//		});
//		
//		
//		for(Coordinate coordinate : coordinates) {
//			Long chunk = coordinate.getX().atZone(ZoneId.of("America/Chicago")).toEpochSecond() / 60 / chunksize;
//			if(utilMap.containsKey(key))
//		}
		return coordinates;
	}

	@Override
	public List<IpCount> getAllIpAddresses() {
		ResponseEntity<List<Row>> response = restTemplate.exchange(config.getHbaseServer() + "/{table}/scan", HttpMethod.GET, null, new ParameterizedTypeReference<List<Row>>() {}, "top_ip_addresses");
		List<IpCount> addresses = new ArrayList<>();
		for(Row row : response.getBody()) {
			addresses.add(new IpCount(row.getName(), Long.valueOf(row.getColumns().get(0).getValue())));
		}
		addresses.sort(new Comparator<IpCount>() {

			@Override
			public int compare(IpCount a, IpCount b) {
				if(a.getCount().longValue() == b.getCount().longValue()) {
					return 0;
				} else if(a.getCount().longValue() < b.getCount().longValue()) {
					return 1;
				} else {
					return -1;
				}
			}
		});
		return addresses;
	}

	@Override
	public List<String> getAllIpAddressesInInterval(ZonedDateTime from, ZonedDateTime to) {
		// TODO Auto-generated method stub
		return null;
	}

}
