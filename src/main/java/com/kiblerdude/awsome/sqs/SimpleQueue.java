package com.kiblerdude.awsome.sqs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

public final class SimpleQueue<M extends Object> implements Queue<M> {

    private static final int DEFAULT_SEND_BATCH_SIZE = 10;
    private static final int DEFAULT_RECV_MAX_MESSAGES = 10;
    private static final int DEFAULT_RECV_MAX_TIME_SECONDS = 2;
    private static final String QUEUE_ATTR_LENGTH = "ApproximateNumberOfMessages";

    private final AmazonSQSClient client;
    private final ObjectMapper mapper;
    private final String endpoint;
    private final Queue<M> receiveBuffer;
    private final Class<M> clazz;

    public SimpleQueue(String queueName, AmazonSQSClient client, Class<M> clazz) {
        this.client = client;
        this.mapper = new ObjectMapper();
        this.endpoint = client.getQueueUrl(queueName).getQueueUrl();
        this.receiveBuffer = new LinkedList<>();
        this.clazz = clazz;
    }

    @Override
    public int size() {
        GetQueueAttributesResult result = client.getQueueAttributes(endpoint, Lists.newArrayList(QUEUE_ATTR_LENGTH));
        Optional<String> length = Optional.fromNullable(result.getAttributes().get(QUEUE_ATTR_LENGTH));
        if (length.isPresent()) {
            return Integer.parseInt(length.get());
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public Iterator<M> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] toArray() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean remove(Object o) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean addAll(Collection<? extends M> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void clear() {
        PurgeQueueRequest request = new PurgeQueueRequest().withQueueUrl(endpoint);
        client.purgeQueue(request);
    }

    @Override
    public boolean add(M e) {
        return offer(e);
    }

    @Override
    public boolean offer(M e) {
        try {
            List<M> messages = Lists.newArrayListWithCapacity(1);
            messages.add(e);
            return sendBatch(messages);
        } catch (IOException e1) {
            return false;
        }
    }

    @Override
    public M remove() {
        try {
            fillBuffer();
        } catch (IOException e) {
            throw new NullPointerException("no element");
        }
        return receiveBuffer.remove();
    }

    @Override
    public M poll() {
        try {
            fillBuffer();
        } catch (IOException e) {
            return null;
        }
        return receiveBuffer.poll();
    }

    @Override
    public M element() {
        try {
            fillBuffer();
        } catch (IOException e) {
            throw new NullPointerException("no element");
        }
        return receiveBuffer.element();
    }

    @Override
    public M peek() {
        try {
            fillBuffer();
        } catch (IOException e) {
            return null;
        }
        return receiveBuffer.peek();
    }

    /**
     * Fills the buffer with messages from SQS.
     * 
     * @return <code>true</code> if the request was successful
     */
    private boolean fillBuffer() throws IOException {
        if (receiveBuffer.isEmpty()) {
            ReceiveMessageRequest request = new ReceiveMessageRequest()
                    .withMaxNumberOfMessages(DEFAULT_RECV_MAX_MESSAGES).withQueueUrl(endpoint)
                    .withWaitTimeSeconds(DEFAULT_RECV_MAX_TIME_SECONDS);
            ReceiveMessageResult result = client.receiveMessage(request);
            List<Message> messages = result.getMessages();
            for (Message message : messages) {
                String json = message.getBody();
                M msg = mapper.readValue(json, clazz);
                receiveBuffer.offer(msg);
            }
        }
        return true;
    }

    /**
     * Sends one or more messages to the SQS queue.
     * @param messages
     * @return <code>true</code> if the messages were sent.
     * @throws IOException
     */
    private boolean sendBatch(Collection<M> messages) throws IOException {
        if (messages.isEmpty()) {
            return false;
        }
        SendMessageBatchRequest request = new SendMessageBatchRequest().withQueueUrl(endpoint);
        Collection<SendMessageBatchRequestEntry> entries = new ArrayList<>(messages.size());
        for (M message : messages) {
            String id = UUID.randomUUID().toString();
            String json = mapper.writeValueAsString(message);
            SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry(id, json);
            entries.add(entry);
        }        
        request.withEntries(entries);
        SendMessageBatchResult result = client.sendMessageBatch(request);
        return result.getFailed().isEmpty();        
    }
}
