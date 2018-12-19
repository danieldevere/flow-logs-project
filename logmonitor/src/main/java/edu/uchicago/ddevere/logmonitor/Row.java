package edu.uchicago.ddevere.logmonitor;

import java.util.List;

import lombok.Data;

@Data
public class Row {
	private String name;
	private List<Column> columns;
}
