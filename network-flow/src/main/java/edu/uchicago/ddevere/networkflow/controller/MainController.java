package edu.uchicago.ddevere.networkflow.controller;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import edu.uchicago.ddevere.networkflow.service.HbaseQueryService;
import edu.uchicago.ddevere.networkflow.vo.Coordinate;
import edu.uchicago.ddevere.networkflow.vo.IpCount;

@RestController
public class MainController {
	
	@Autowired
	private HbaseQueryService hbaseQueryService;
	
	@GetMapping("/")
	public ModelAndView index(ModelAndView modelAndView) {
		List<IpCount> ipAddresses = hbaseQueryService.getAllIpAddresses();
		modelAndView.addObject("ipAddresses", ipAddresses);
		modelAndView.setViewName("index");
		return modelAndView;
	}
	
	@GetMapping("/hello")
	public String hello() {
		return "hello world";
	}
	
	@GetMapping("/data")
	public ResponseEntity<Map<String, List<Coordinate>>> data(
			@RequestParam(value = "ipAddress", required = true)List<String> ipAddresses, 
			@RequestParam(value = "field", required = true)String field,
			@RequestParam(value = "fromTime", required = false)String fromTime,
			@RequestParam(value = "toTime", required = false)String toTime,
			@RequestParam(value = "chunkSize", defaultValue = "1")Integer chunksize) {
		if(StringUtils.isBlank(field) || ipAddresses == null || ipAddresses.isEmpty()) {
			return ResponseEntity.badRequest().build();
		}
		Map<String, List<Coordinate>> results = new HashMap();
		if(StringUtils.isNoneBlank(fromTime, toTime)) {
			ZonedDateTime from = ZonedDateTime.of(LocalDateTime.parse(fromTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME), ZoneId.of("America/Chicago"));
			ZonedDateTime to = ZonedDateTime.of(LocalDateTime.parse(toTime, DateTimeFormatter.ISO_LOCAL_DATE_TIME), ZoneId.of("America/Chicago"));
			System.out.println("from: " + from.toString());
			System.out.println("to: " + to.toString());
			for(String ipAddress : ipAddresses) {
				List<Coordinate> coordinates = hbaseQueryService.findByIpAddressWithinInterval(ipAddress, field, from, to, chunksize);
				if(coordinates != null && !coordinates.isEmpty()) {
					results.put(ipAddress, coordinates);
				}
			}
		} else {
			for(String ipAddress : ipAddresses) {
				List<Coordinate> coordinates = hbaseQueryService.findByIpAddress(ipAddress, field, chunksize);
				if(coordinates != null && !coordinates.isEmpty()) {
					results.put(ipAddress, coordinates);
				}
				
			}
		}
		return new ResponseEntity<Map<String, List<Coordinate>>>(results, HttpStatus.OK);
	}
}
