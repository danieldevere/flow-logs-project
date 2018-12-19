package edu.uchicago.ddevere.hbaserest.controller;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import edu.uchicago.ddevere.hbaserest.vo.ColumnSchema;
import edu.uchicago.ddevere.hbaserest.vo.TableSchema;

@RestController
public class SchemaController {
	
	@Autowired
	private Connection connection;
	
	@GetMapping("/{table}/schema")
	public ResponseEntity<TableSchema> getSchema(@PathVariable("table")String table) {
		try {
			Table tab = connection.getTable(TableName.valueOf(table));
			TableSchema schema = new TableSchema();
			List<ColumnSchema> columnSchemas = new ArrayList<>();
			for(HColumnDescriptor descriptor : tab.getTableDescriptor().getColumnFamilies()) {
				ColumnSchema columnSchema = new ColumnSchema();
				columnSchema.setName(descriptor.getNameAsString());
				columnSchemas.add(columnSchema);
			}
			schema.setColumnSchemas(columnSchemas);
			return ResponseEntity.ok(schema);
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.notFound().build();
		}
		
	}
	
	@PutMapping("/{table}/schema")
	public ResponseEntity<TableSchema> tableSchema(@PathVariable("table")String table, @RequestBody TableSchema tableSchema) {
		try {
			Admin admin = connection.getAdmin();
			if(admin.tableExists(TableName.valueOf(table))) {
				for(ColumnSchema column : tableSchema.getColumnSchemas()) {
					admin.addColumn(TableName.valueOf(table), new HColumnDescriptor(column.getName()));
				}
			} else {
				HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(table));
				for(ColumnSchema column : tableSchema.getColumnSchemas()) {
					descriptor.addFamily(new HColumnDescriptor(column.getName()));
				}
				admin.createTable(descriptor);
			}
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.unprocessableEntity().build();
		}
		return getSchema(table);
	}
	
	@DeleteMapping("/{table}/schema")
	public ResponseEntity<?> deleteTable(@PathVariable("table")String table) {
		try {
			Admin admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(table));
			admin.deleteTable(TableName.valueOf(table));
			return ResponseEntity.ok().build();
		} catch(Exception e) {
			e.printStackTrace();
			return ResponseEntity.notFound().build();
		}
	}
}
