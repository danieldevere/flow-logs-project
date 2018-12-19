package edu.uchicago.ddevere.hbaserest.vo;

import java.util.List;

import lombok.Data;

@Data
public class TableSchema {
	private List<ColumnSchema> columnSchemas;
}
