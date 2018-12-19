package edu.uchicago.ddevere.hbaserest.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import edu.uchicago.ddevere.hbaserest.vo.Column;
import edu.uchicago.ddevere.hbaserest.vo.Row;
import lombok.extern.slf4j.Slf4j;

@RestController
@Slf4j
public class TableController {
	
	@Autowired
	private Connection connection;
	
	public static ConcurrentMap<String, Long> timeHolder = new ConcurrentHashMap<>();
	
	@GetMapping("/{table}/ipAddresses")
	public ResponseEntity<List<String>> allIpAddresses(
			@PathVariable("table")String table,
			@RequestParam(value = "fromMinute", required = false)String fromTime,
			@RequestParam(value = "toMinute", required = false)String toTime) {
		try {
			Table tab = connection.getTable(TableName.valueOf(table));
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			ResultScanner scanner = tab.getScanner(scan);
			List<String> ipAddresses = new ArrayList<>();
			for(Result result : scanner) {
				ipAddresses.add(Bytes.toString(result.getRow()));
			}
			return ResponseEntity.ok(ipAddresses);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.notFound().build();
		}
	}
	
	@GetMapping("/{table}")
	public ResponseEntity<Row> getRow(
			@PathVariable("table")String table, 
			@RequestParam("row")String row,
			@RequestParam(value = "family", required = false)String family,
			@RequestParam(value = "qualifier", required = false)String qualifier,
			@RequestParam(value = "timeRangeFrom", required = false)Long timeRangeFrom,
			@RequestParam(value = "timeRangeTo", required = false)Long timeRangeTo,
			@RequestParam(value = "fromMinute", required = false)String fromQualifier,
			@RequestParam(value = "toMinute", required = false)String toQualifier) {
		try {
			Table tab = connection.getTable(TableName.valueOf(table));
			Get get = new Get(row.getBytes());
			if(StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
				get.addColumn(family.getBytes(), qualifier.getBytes());
			} else if(StringUtils.isNotBlank(family)) {
				get.addFamily(family.getBytes());
			}
			if(timeRangeFrom != null && timeRangeTo != null) {
				get.setTimeRange(timeRangeFrom * 1000, timeRangeTo * 1000);
			}
			if(StringUtils.isNoneBlank(fromQualifier, toQualifier)) {
				ColumnRangeFilter f = new ColumnRangeFilter(fromQualifier.getBytes(), true, toQualifier.getBytes(), true);
				get.setFilter(f);
			}
			Result result = tab.get(get);
			List<Column> columns = getColumns(result);
			Row r = new Row();
			r.setName(row);
			r.setColumns(columns);
			return ResponseEntity.ok(r);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.notFound().build();
		}
	}
	
	private List<Column> getColumns(Result result) {
		List<Column> columns = new ArrayList<>();
		while(result.advance()) {
			Cell cell = result.current();
			Column column = new Column();
			column.setTimestamp(String.valueOf(cell.getTimestamp()));
			column.setValue(Bytes.toString(CellUtil.cloneValue(cell)));
			column.setFamily(Bytes.toString(CellUtil.cloneFamily(cell)));
			column.setQualifier(Bytes.toString(CellUtil.cloneQualifier(cell)));
			columns.add(column);
		}
		return columns;
	}
	
	@GetMapping("/{table}/scan")
	public ResponseEntity<List<Row>> scanTable(@PathVariable("table")String table) {
		try {
			Table tab = connection.getTable(TableName.valueOf(table));
			Scan scan = new Scan();
			ResultScanner scanner = tab.getScanner(scan);
			List<Row> rows = new ArrayList<>();
			for(Result result : scanner) {
				Row row = new Row();
				row.setName(Bytes.toString(result.getRow()));
				row.setColumns(getColumns(result));
				rows.add(row);
			}
			return ResponseEntity.ok(rows);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@PostMapping("/batch")
	public ResponseEntity<?> postRowsBatch(@RequestParam("table")String tables[], @RequestBody List<Row> rows) {
		log.info("Inserting batch into table numRows: {}", rows.size());
		try {
			for(String table : tables) {
				log.info("Inserting into table: {}", table);
				for(Row row : rows) {
					postRow(table, row);
				}
			}
			log.info("done");
			return ResponseEntity.ok("OK");
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@PostMapping("/{table}")
	public ResponseEntity<Row> postRow(@PathVariable("table")String table, @RequestBody Row row) {
		try {
//			log.info("Inserting into table: {}, row: {}", table, row.toString());
			Table t = connection.getTable(TableName.valueOf(table));
			Put put = new Put(row.getName().getBytes());
			for(Column column : row.getColumns()) {
				put.addColumn(column.getFamily().getBytes(), column.getQualifier().getBytes(), column.getValue().getBytes());
				
			}
			t.put(put);
//			log.info("saved row");
			return getRow(table, row.getName(), null, null, null, null, null, null);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.badRequest().build();
		}
	}
	
	@DeleteMapping("/{table}")
	public ResponseEntity<?> deleteRow(
			@PathVariable("table")String table,
			@RequestParam("row")String row,
			@RequestParam(value = "family", required = false)String family,
			@RequestParam(value = "qualifier", required = false)String qualifier,
			@RequestParam(value = "timestamp", required = false)Long timestamp) {
		try {
			Table t = connection.getTable(TableName.valueOf(table));
			Delete delete = new Delete(row.getBytes());
			if(StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier) && timestamp != null) {
				delete.addColumn(family.getBytes(), qualifier.getBytes(), timestamp);
			} else if(StringUtils.isNotBlank(family) && StringUtils.isNotBlank(qualifier)) {
				delete.addColumn(family.getBytes(), qualifier.getBytes());
			} else if(StringUtils.isNotBlank(family)) {
				delete.addFamily(family.getBytes());
			}
			t.delete(delete);
			return ResponseEntity.ok().build();
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.notFound().build();
		}
	}
	
	@GetMapping("/updateTime")
	public String updateTime(@RequestParam("time")long epochSeconds) {
		timeHolder.put("lastBatch", epochSeconds);
		return "OK";
	}
	
	@GetMapping("/getTime")
	public String getTime() {
		return timeHolder.get("lastBatch").toString();
	}
	
	@GetMapping("/{table}/range")
	public ResponseEntity<Row> getRange(
			@PathVariable("table")String table,
			@RequestParam("row")String row,
			@RequestParam(value = "family", required = false)String family,
			@RequestParam("fromMinute")String fromQualifier,
			@RequestParam("toMinute")String toQualifier) {
		try {
			Table t = connection.getTable(TableName.valueOf(table));
			Get get = new Get(row.getBytes());
			if(StringUtils.isNotBlank(family)) {
				get.addFamily(family.getBytes());
			}
			ColumnRangeFilter f = new ColumnRangeFilter(fromQualifier.getBytes(), true, toQualifier.getBytes(), true);
			get.setFilter(f);
			Result result = t.get(get);
			List<Column> columns = getColumns(result);
			Row r = new Row();
			r.setName(row);
			r.setColumns(columns);
			return ResponseEntity.ok(r);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.notFound().build();
		}
	}
}
