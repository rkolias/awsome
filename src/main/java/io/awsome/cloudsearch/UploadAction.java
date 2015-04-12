package io.awsome.cloudsearch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Model for adding or deleting documents in Cloudsearch.
 * <p>
 * This class is package protected.
 * 
 * @author kiblerj
 * 
 */
@JsonSerialize
@JsonInclude(JsonInclude.Include.NON_NULL)
class UploadAction<T extends Object> {

	@JsonProperty(value = "type")
	private final String type;

	@JsonProperty(value = "id")
	private final String id;

	@JsonProperty(value = "fields")
	private final T fields;

	/**
	 * Constructor for adding or updating documents.
	 * 
	 * @param id
	 *            The id of the document to add or update.
	 * @param fields
	 *            The fields names and values.
	 */
	public UploadAction(String id, T fields) {
		this.type = "add";
		this.id = id;
		this.fields = fields;
	}

	/**
	 * Constructor for deleting documents.
	 * 
	 * @param id
	 *            The id of the document to delete.
	 */
	public UploadAction(String id) {
		this.type = "delete";
		this.id = id;
		this.fields = null;
	}
	
	public String getType() {
		return type;
	}
	
	public String getId() {
		return id;
	}
	
	public T getFields() {
		return fields;
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
