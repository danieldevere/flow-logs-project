package edu.uchicago.ddevere.networkflow.vo;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Row {
	private String name;
	private List<Column> columns; 
}
