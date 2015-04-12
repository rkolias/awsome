package io.awsome.cloudsearch;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Model for adding or deleting batches documents in Cloudsearch.
 * <p>
 * This class is package protected.
 * 
 * @author kiblerj
 * 
 */
@JsonSerialize
class UploadActionBatch<T extends Object> {
	
	private final List<UploadAction<T>> actions;

	public UploadActionBatch(List<UploadAction<T>> actions) {
		this.actions = actions;
	}
	
	@JsonValue
	public List<UploadAction<T>> getActions() {
		return actions;
	}
	
	@Override
	public String toString() {
		ObjectMapper mapper = new ObjectMapper();
		try {
			return mapper.writeValueAsString(this);
		} catch (JsonProcessingException e) {
			return "error";
		}		
	}
}
