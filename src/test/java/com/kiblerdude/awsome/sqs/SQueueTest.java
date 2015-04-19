package com.kiblerdude.awsome.sqs;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Maps;


public class SQueueTest {
    
    @Mock
    private AmazonSQSClient client;
    private SQueue<ExampleMessage> instance;
    
    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);        
        
        Message message = new Message();
        message.setBody("{\"s\":\"test\"}");
        
        GetQueueUrlResult gqr = new GetQueueUrlResult().withQueueUrl("url");
        when(client.getQueueUrl("queue")).thenReturn(gqr);
        
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("ApproximateNumberOfMessages", "100");
        GetQueueAttributesResult gqar = new GetQueueAttributesResult().withAttributes(attributes);
        when(client.getQueueAttributes(eq("url"), any(List.class))).thenReturn(gqar);
        when(client.sendMessage(any(SendMessageRequest.class))).thenReturn(new SendMessageResult().withMessageId("id"));
        when(client.receiveMessage(any(ReceiveMessageRequest.class))).thenReturn(new ReceiveMessageResult().withMessages(message));      
        
        instance = new SQueue<>("queue", client, ExampleMessage.class);
    }
	
	@Test
	public void test() throws Exception {    
	    assertTrue(instance.size() == 100);
	    assertFalse(instance.isEmpty());
	    assertTrue(instance.push(new ExampleMessage("test")).isPresent());
	    assertTrue(instance.pop().isPresent());
	}
	
	@JsonSerialize
	@SuppressWarnings("unused")
	private static final class ExampleMessage {
		@JsonProperty(value="s")
		private String strValue;
		public ExampleMessage() {}
		public ExampleMessage(String strValue) {
			this.strValue = strValue;
		}
	}
}
