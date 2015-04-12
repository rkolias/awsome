package io.awsome.cloudsearch;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.amazonaws.services.cloudsearchdomain.model.UploadDocumentsRequest;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

public class UploadDocumentsBuilderTest {

	private static final String ADD_DOC_JSON = "{\"type\":\"add\",\"id\":\"id.1\",\"fields\":{\"s\":\"test\",\"i\":10,\"d\":20.0,\"sa\":[\"abc\",\"def\"]}}";
	private static final String DEL_DOC_JSON = "{\"type\":\"delete\",\"id\":\"id.2\"}";
	private static final String BATCH_JSON = String.format("[%s,%s]", ADD_DOC_JSON, DEL_DOC_JSON);
	
	@Test
	public void test() throws IOException {
		
		ExampleDocument doc1 = new ExampleDocument("test", 10, 20.0, Arrays.asList("abc","def"));
		
		UploadAction<ExampleDocument> addAction = new UploadAction<>("id.1", doc1);
		assertEquals(ADD_DOC_JSON, addAction.toString());
		
		UploadAction<ExampleDocument> deleteAction = new UploadAction<>("id.2");
		assertEquals(DEL_DOC_JSON, deleteAction.toString());
		
		UploadActionBatch<ExampleDocument> batch = new UploadActionBatch<>(Arrays.asList(addAction, deleteAction));		
		assertEquals(BATCH_JSON, batch.toString());
		
		UploadDocumentsBuilder<ExampleDocument> builder = new UploadDocumentsBuilder<>();
		builder.add("id.1", doc1);
		builder.delete("id.2");
		UploadDocumentsRequest request = builder.build();
		String requestJson = IOUtils.toString(request.getDocuments());
		assertEquals(BATCH_JSON, requestJson);
	}
	
	@JsonSerialize
	@SuppressWarnings("unused")
	private static final class ExampleDocument {
		@JsonProperty(value="s")
		private final String strValue;
		@JsonProperty(value="i")
		private final Integer intValue;
		@JsonProperty(value="d")
		private final Double dblValue;
		@JsonProperty(value="sa")
		private final List<String> strArrayValue;		
		public ExampleDocument(String strValue, Integer intValue, Double dblValue, List<String> strArrayValue) {
			this.strValue = strValue;
			this.intValue = intValue;
			this.dblValue = dblValue;
			this.strArrayValue = strArrayValue;
		}
	}
}
