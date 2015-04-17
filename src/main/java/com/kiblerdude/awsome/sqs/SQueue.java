package com.kiblerdude.awsome.sqs;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

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

/**
 * Provides an easy to use interface for working with AWS SQS queues.  Messages pushed and popped from the
 * queue are serialized to JSON using the Jackson ObjectMapper.
 * <p>
 * This class is thread safe.
 * <p>
 * Example:
 * <pre>
 * SQueue queue = new SQueue("myqueue", "client", MyMessage.class);
 * 
 * queue.push(new MyMessage("hello"));
 * queue.push(new MyMessage("world"));
 * 
 * Optional<MyMessage> message = queue.pop(); // "hello"
 * 
 * if (message.isPresent()) {
 *     // do something...
 * }
 * </pre>
 * @author kiblerj
 *
 * @param <M> A Jackson annotated class representing the messages in the queue.
 */
public final class SQueue<M extends Object> {

    // TODO implement Iterable
    
    private static final int DEFAULT_SEND_BATCH_SIZE = 10;
    private static final int DEFAULT_RECV_MAX_MESSAGES = 10;
    private static final int DEFAULT_RECV_MAX_TIME_SECONDS = 2;
    private static final String QUEUE_ATTR_LENGTH = "ApproximateNumberOfMessages";

    private final AmazonSQSClient client;
    private final ObjectMapper mapper;
    private final String endpoint;
    private final Queue<M> receiveBuffer;
    private final Class<M> clazz;

    /**
     * Constructor
     * 
     * @param queueName
     *            The name of the SQS queue
     * @param client
     *            The {@link AmazonSQSClient}
     * @param clazz
     *            The class type of the message
     */
    public SQueue(String queueName, AmazonSQSClient client, Class<M> clazz) {
        this.client = client;
        this.mapper = new ObjectMapper();
        this.endpoint = client.getQueueUrl(queueName).getQueueUrl();
        this.receiveBuffer = new ConcurrentLinkedQueue<>();
        this.clazz = clazz;
    }

    /**
     * Returns the current number of messages in the queue.
     * @return int
     */
    public int size() {
        GetQueueAttributesResult result = client.getQueueAttributes(endpoint, Lists.newArrayList(QUEUE_ATTR_LENGTH));
        Optional<String> length = Optional.fromNullable(result.getAttributes().get(QUEUE_ATTR_LENGTH));
        if (length.isPresent()) {
            return Integer.parseInt(length.get());
        }
        return -1;
    }

    /**
     * Returns <code>true</code> if the queue is empty.
     * @return boolean
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Removes all of the messages from the queue.
     */
    public void clear() {
        PurgeQueueRequest request = new PurgeQueueRequest().withQueueUrl(endpoint);
        client.purgeQueue(request);
    }

    /**
     * Pushes one or more messages on to the queue.
     * @param message The messages to push
     * @return <code>true</code> if the message was pushed, <code>false</code> otherwise.
     */
    @SuppressWarnings("unchecked")
    public boolean push(M... messages) throws IOException {
        checkNotNull(messages, "messages is null");        
        return push(messages);
    }
    
    /**
     * Pushes a collection of message on to the queue.
     * @param messages A Collection of messages to push
     * @return <code>true</code> if all of the message were pushed, <code>false</code> otherwise.
     * @throws IOException 
     */
    public boolean push(Collection<M> messages) throws IOException {
        checkNotNull(messages, "messages is null");
        
        if (messages.isEmpty()) {
            return false;
        }
        
        Iterator<M> iter = messages.iterator();
        List<M> batch = new ArrayList<>(DEFAULT_SEND_BATCH_SIZE);
        while (iter.hasNext()) {
            batch.add(iter.next());            
            if (batch.size() == DEFAULT_SEND_BATCH_SIZE || !iter.hasNext()) {
                sendBatch(batch);
                batch.clear();
            }
        }       
        return true;
    }

    /**
     * Pops a message from the queue.
     * @return An {@link Optional} contain the next message on the queue, or absent if no message was popped.
     */
    public Optional<M> pop() throws IOException {
        fillBuffer();
        return Optional.fromNullable(receiveBuffer.poll());
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
