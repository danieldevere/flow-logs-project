package edu.uchicago.ddevere.hbaserest.vo;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Column {
	private String family;
	private String qualifier;
	@JsonProperty(required = false)
	private String timestamp;
	private String value;
}
