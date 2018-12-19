package edu.uchicago.ddevere.hbaserest.vo;

import java.util.List;

import lombok.Data;

@Data
public class Row {
	private String name;
	private List<Column> columns;
}
