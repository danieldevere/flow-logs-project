package edu.uchicago.ddevere.networkflow.vo;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Column {
	private String family;
	private String qualifier;
	@JsonProperty(required = false)
	private String timestamp;
	private String value;
}
