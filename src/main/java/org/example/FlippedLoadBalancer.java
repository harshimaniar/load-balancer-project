package org.example;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.logging.Level;

// ================= Message Structure =================
class Message {
    private final String messageId;
    private final String sourceServerId;
    private final String destinationId;
    private final String messageType;
    private final int priority;
    private final long timestamp;
    private final JsonObject payload;

    public Message(String messageId, String sourceServerId, String destinationId,
                   String messageType, int priority, JsonObject payload) {
        this.messageId = messageId;
        this.sourceServerId = sourceServerId;
        this.destinationId = destinationId;
        this.messageType = messageType;
        this.priority = priority;
        this.timestamp = System.currentTimeMillis();
        this.payload = payload;
    }

    public String getMessageId() { return messageId; }
    public String getSourceServerId() { return sourceServerId; }
    public String getDestinationId() { return destinationId; }
    public String getMessageType() { return messageType; }
    public int getPriority() { return priority; }
    public long getTimestamp() { return timestamp; }
    public JsonObject getPayload() { return payload; }

    @Override
    public String toString() {
        return String.format("Message[id=%s, src=%s, dest=%s, type=%s, priority=%d]",
                messageId, sourceServerId, destinationId, messageType, priority);
    }
}

// ================= Customer State =================
class CustomerState {
    private final String customerId;
    private final Set<String> receivedMessages = ConcurrentHashMap.newKeySet();
    private final Map<String, Integer> mealTypeCount = new ConcurrentHashMap<>();

    public CustomerState(String customerId) {
        this.customerId = customerId;
        mealTypeCount.put("breakfast", 0);
        mealTypeCount.put("lunch", 0);
        mealTypeCount.put("dinner", 0);
    }

    public boolean hasReceived(String messageId) {
        return receivedMessages.contains(messageId);
    }

    public void markReceived(String messageId, String mealType) {
        receivedMessages.add(messageId);
        mealTypeCount.merge(mealType, 1, Integer::sum);
    }

    public Map<String, Integer> getMealCounts() {
        return new HashMap<>(mealTypeCount);
    }

    public String getCustomerId() { return customerId; }
}

// ================= Message Ingestion =================
class MessageIngester {
    private static final Logger logger = Logger.getLogger(MessageIngester.class.getName());
    private final BlockingQueue<Message> messageQueue;
    private final List<ServerConnection> serverConnections = new ArrayList<>();
    private final ExecutorService ingestionExecutor;

    public MessageIngester(BlockingQueue<Message> messageQueue, List<String> serverEndpoints) {
        this.messageQueue = messageQueue;
        this.ingestionExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "MessageIngester-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });

        for (String endpoint : serverEndpoints) {
            serverConnections.add(new ServerConnection(endpoint));
        }
    }

    public void start() {
        for (ServerConnection conn : serverConnections) {
            ingestionExecutor.submit(conn);
        }
        logger.info("Message ingestion started for " + serverConnections.size() + " servers");
    }

    public void shutdown() {
        ingestionExecutor.shutdownNow();
        serverConnections.forEach(ServerConnection::close);
    }

    private class ServerConnection implements Runnable {
        private final String endpoint;
        private volatile boolean running = true;

        public ServerConnection(String endpoint) {
            this.endpoint = endpoint;
        }

        @Override
        public void run() {
            while (running && !Thread.currentThread().isInterrupted()) {
                try {
                    String[] parts = endpoint.split(":");
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);

                    try (Socket socket = new Socket(host, port);
                         BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                        String messageJson;
                        while ((messageJson = reader.readLine()) != null && running) {
                            try {
                                JsonObject json = JsonParser.parseString(messageJson).getAsJsonObject();
                                Message message = parseMessage(json);
                                messageQueue.offer(message, 100, TimeUnit.MILLISECONDS);

                                // Store in DB at ingestion
                                DatabaseHelper.saveMessage(message, "INGESTED");

                                logger.fine("Ingested message: " + message);
                            } catch (Exception e) {
                                logger.log(Level.WARNING, "Failed to parse message: " + messageJson, e);
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Connection failed for " + endpoint + ", retrying in 5s", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        private Message parseMessage(JsonObject json) {
            return new Message(
                    json.get("messageId").getAsString(),
                    json.get("sourceServerId").getAsString(),
                    json.get("destinationId").getAsString(),
                    json.get("messageType").getAsString(),
                    json.get("priority").getAsInt(),
                    json.has("payload") ? json.getAsJsonObject("payload") : new JsonObject()
            );
        }

        public void close() {
            running = false;
        }
    }
}

// ================= Scheduler =================
class MessageScheduler {
    private static final Logger logger = Logger.getLogger(MessageScheduler.class.getName());
    private final PriorityQueue<Message> priorityQueue = new PriorityQueue<>(
            Comparator.comparing(Message::getPriority)
                    .thenComparing(Message::getTimestamp)
    );
    private final BlockingQueue<Message> inputQueue;
    private final BlockingQueue<Message> outputQueue;
    private final ExecutorService schedulerExecutor;
    private volatile boolean running = true;

    public MessageScheduler(BlockingQueue<Message> inputQueue, BlockingQueue<Message> outputQueue) {
        this.inputQueue = inputQueue;
        this.outputQueue = outputQueue;
        this.schedulerExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "MessageScheduler");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        schedulerExecutor.submit(this::scheduleMessages);
        logger.info("Message scheduler started");
    }

    public void shutdown() {
        running = false;
        schedulerExecutor.shutdownNow();
    }

    private void scheduleMessages() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                List<Message> batch = new ArrayList<>();
                Message first = inputQueue.poll(1, TimeUnit.SECONDS);
                if (first != null) {
                    batch.add(first);
                    inputQueue.drainTo(batch, 50);
                    synchronized (priorityQueue) {
                        priorityQueue.addAll(batch);
                    }
                    processScheduledMessages();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error in message scheduling", e);
            }
        }
    }

    private void processScheduledMessages() {
        synchronized (priorityQueue) {
            while (!priorityQueue.isEmpty()) {
                Message message = priorityQueue.poll();
                try {
                    outputQueue.offer(message, 100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}

// ================= Delivery =================
class MessageDelivery {
    private static final Logger logger = Logger.getLogger(MessageDelivery.class.getName());
    private final BlockingQueue<Message> scheduledQueue;
    private final Map<String, CustomerState> customerStates = new ConcurrentHashMap<>();
    private final ExecutorService deliveryExecutor;
    private final AtomicLong deliveredCount = new AtomicLong(0);
    private final AtomicLong duplicateCount = new AtomicLong(0);
    private volatile boolean running = true;

    public MessageDelivery(BlockingQueue<Message> scheduledQueue, List<String> customerIds) {
        this.scheduledQueue = scheduledQueue;
        this.deliveryExecutor = Executors.newFixedThreadPool(10, r -> {
            Thread t = new Thread(r, "MessageDelivery-" + System.currentTimeMillis());
            t.setDaemon(true);
            return t;
        });

        for (String customerId : customerIds) {
            customerStates.put(customerId, new CustomerState(customerId));
        }
    }

    public void start() {
        for (int i = 0; i < 5; i++) {
            deliveryExecutor.submit(this::deliverMessages);
        }
        logger.info("Message delivery started with " + customerStates.size() + " customers");
    }

    public void shutdown() {
        running = false;
        deliveryExecutor.shutdownNow();
    }

    private void deliverMessages() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                Message message = scheduledQueue.poll(1, TimeUnit.SECONDS);
                if (message != null) {
                    deliverMessage(message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error in message delivery", e);
            }
        }
    }

    private void deliverMessage(Message message) {
        String destinationId = message.getDestinationId();
        CustomerState customer = customerStates.get(destinationId);

        if (customer == null) {
            logger.warning("Unknown customer: " + destinationId);
            return;
        }

        if (customer.hasReceived(message.getMessageId())) {
            duplicateCount.incrementAndGet();
            return;
        }

        try {
            customer.markReceived(message.getMessageId(), message.getMessageType());
            deliveredCount.incrementAndGet();

            // Store in DB when delivered
            DatabaseHelper.saveMessage(message, "DELIVERED");

            logger.info(String.format("DELIVERED: %s to customer %s (priority %d, type %s)",
                    message.getMessageId(), destinationId, message.getPriority(), message.getMessageType()));

            Thread.sleep(10);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to deliver message: " + message, e);
        }
    }

    public void printStats() {
        logger.info("=== DELIVERY STATISTICS ===");
        logger.info("Total delivered: " + deliveredCount.get());
        logger.info("Duplicates filtered: " + duplicateCount.get());
    }
}

// ================= Main =================
public class FlippedLoadBalancer {
    private static final Logger logger = Logger.getLogger(FlippedLoadBalancer.class.getName());

    private final BlockingQueue<Message> ingestedQueue = new LinkedBlockingQueue<>(10000);
    private final BlockingQueue<Message> scheduledQueue = new LinkedBlockingQueue<>(10000);

    private MessageIngester ingester;
    private MessageScheduler scheduler;
    private MessageDelivery delivery;

    public void initialize() {
        List<String> serverEndpoints = Arrays.asList(
                "localhost:8001",
                "localhost:8002",
                "localhost:8003",
                "localhost:8004"
        );

        List<String> customerIds = Arrays.asList("table1", "table2", "table3", "table4");

        ingester = new MessageIngester(ingestedQueue, serverEndpoints);
        scheduler = new MessageScheduler(ingestedQueue, scheduledQueue);
        delivery = new MessageDelivery(scheduledQueue, customerIds);

        logger.info("Flipped Load Balancer initialized");
    }

    public void start() {
        ingester.start();
        scheduler.start();
        delivery.start();
    }

    public void shutdown() {
        ingester.shutdown();
        scheduler.shutdown();
        delivery.shutdown();
        delivery.printStats();
    }

    public static void main(String[] args) {
        FlippedLoadBalancer app = new FlippedLoadBalancer();
        Runtime.getRuntime().addShutdownHook(new Thread(app::shutdown));

        try {
            app.initialize();
            app.start();
            Thread.sleep(30000);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Application error", e);
        } finally {
            app.shutdown();
        }
    }
}
