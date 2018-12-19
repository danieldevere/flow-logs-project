package edu.uchicago.ddevere.logmonitor;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor 
@NoArgsConstructor
public class Column {
	private String family;
	private String qualifier;
	@JsonProperty(required = false)
	private String timestamp;
	private String value;
}
