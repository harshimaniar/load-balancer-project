# 🔄 Flipped Load Balancer

A high-performance message routing and multiplexing system that implements a "flipped" load balancing architecture. Instead of distributing incoming requests across multiple servers, this system multiplexes messages from multiple sources into a single unified stream, then intelligently routes them to appropriate destinations based on priority and customer requirements.

## 🎯 Architecture Overview

The Flipped Load Balancer operates in three main stages:

1. **Message Ingestion**: Async TCP connections to multiple Python message servers
2. **Priority Scheduling**: OS-inspired scheduling with priority queues and batch processing
3. **Smart Delivery**: Demultiplexed routing to customers with duplicate detection

```
[Server 1:8001] ──┐
[Server 2:8002] ──┤    ┌─────────────────┐    ┌─[Table 1 VIP]
[Server 3:8003] ──┼────┤ Flipped Load    ├────┼─[Table 2]
[Server 4:8004] ──┘    │ Balancer        │    ├─[Table 3]
                       └─────────────────┘    └─[Table 4]
```

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd flipped-load-balancer
```

### 2. Database Setup

Start MySQL server (via XAMPP or standalone) and create the database:

```sql
CREATE DATABASE flipped_lb;
USE flipped_lb;

CREATE TABLE messages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    messageId VARCHAR(255) UNIQUE NOT NULL,
    sourceServerId VARCHAR(50) NOT NULL,
    destinationId VARCHAR(50) NOT NULL,
    messageType VARCHAR(50) NOT NULL,
    priority INT NOT NULL,
    timestamp BIGINT NOT NULL,
    payload TEXT,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_destination ON messages(destinationId);
CREATE INDEX idx_priority ON messages(priority);
CREATE INDEX idx_timestamp ON messages(timestamp);
```

### 3. Install Dependencies

**Java Dependencies** (add to `pom.xml`):
```xml
<dependencies>
    <dependency>
        <groupId>com.google.code.gson</groupId>
        <artifactId>gson</artifactId>
        <version>2.8.9</version>
    </dependency>
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.33</version>
    </dependency>
</dependencies>
```

**Python Dependencies**:
```bash
# No external dependencies required - uses only standard library
```

### 4. Start Message Servers

```bash
# Start all Python message servers
python3 message_server_simulator.py

# Or start individual servers
python3 message_server_simulator.py --server server1 --port 8001
```

### 5. Launch Load Balancer

```bash
# Compile and run the Java application
javac -cp "lib/*" org/example/FlippedLoadBalancer.java
java -cp ".:lib/*" org.example.FlippedLoadBalancer
```

## 📖 Detailed Usage

### Message Server Simulator

The Python simulator creates realistic restaurant order messages:

```python
# Example message format
{
    "messageId": "server1-123-abc12345",
    "sourceServerId": "server1",
    "destinationId": "table1",
    "messageType": "breakfast",
    "priority": 1,
    "timestamp": 1693123456789,
    "payload": {
        "serverInfo": "server1",
        "customerTable": "table1",
        "orderDetails": {
            "item": "pancakes",
            "quantity": 2,
            "prepTime": 15,
            "cost": 12.99
        },
        "specialRequests": "no butter"
    }
}
```

### Priority System

Messages are prioritized based on:
- **Priority 1 (High)**: VIP customers (table1) and breakfast orders
- **Priority 2 (Medium)**: Regular lunch and dinner orders
- **Priority 3 (Low)**: Non-peak time orders

### Configuration Options

**Server Endpoints** (modify in FlippedLoadBalancer.java):
```java
List<String> serverEndpoints = Arrays.asList(
    "localhost:8001",
    "localhost:8002",
    "localhost:8003",
    "localhost:8004"
);
```

**Customer IDs**:
```java
List<String> customerIds = Arrays.asList(
    "table1", "table2", "table3", "table4"
);
```

**Database Connection** (modify in DatabaseHelper.java):
```java
private static final String URL = "jdbc:mysql://localhost:3306/flipped_lb";
private static final String USER = "root";
private static final String PASSWORD = "";
```

## 📊 Monitoring & Analytics

### Real-time Statistics

The application provides comprehensive statistics:

```
=== DELIVERY STATISTICS ===
Total delivered: 1,247
Duplicates filtered: 23
Average latency: 45ms
Priority 1 messages: 312
Priority 2 messages: 623
Priority 3 messages: 312
```

### Database Queries

Monitor message flow with SQL queries:

```sql
-- Messages by priority
SELECT priority, COUNT(*) as count 
FROM messages 
GROUP BY priority 
ORDER BY priority;

-- Customer message distribution
SELECT destinationId, messageType, COUNT(*) as orders
FROM messages 
WHERE status = 'DELIVERED'
GROUP BY destinationId, messageType;

-- Hourly message volume
SELECT HOUR(created_at) as hour, COUNT(*) as message_count
FROM messages 
GROUP BY HOUR(created_at) 
ORDER BY hour;
```

## 🏗️ Architecture Components

### 1. MessageIngester
- Establishes TCP connections to all Python servers
- Multiplexes messages into unified queue
- Handles connection failures and reconnections
- Performs initial message parsing and validation

### 2. MessageScheduler  
- Implements priority-based scheduling algorithm
- Uses PriorityQueue with timestamp tiebreaking
- Batch processing for improved throughput
- Thread-safe queue operations

### 3. MessageDelivery
- Demultiplexes messages by destination
- Maintains customer state and message history
- Duplicate detection and filtering
- Concurrent delivery with thread pool

### 4. DatabaseHelper
- Persists messages at ingestion and delivery
- Provides analytics and monitoring data
- Handles database connection management
- JSON payload serialization

## 🔧 Configuration & Tuning

### Performance Tuning

**Queue Sizes**:
```java
private final BlockingQueue<Message> ingestedQueue = new LinkedBlockingQueue<>(10000);
private final BlockingQueue<Message> scheduledQueue = new LinkedBlockingQueue<>(10000);
```

**Thread Pool Sizing**:
```java
// Adjust based on CPU cores and expected load
private final ExecutorService deliveryExecutor = Executors.newFixedThreadPool(10);
```

**Message Generation Rate** (Python servers):
```python
# Adjust timing in _generate_messages()
time.sleep(random.uniform(1.0, 3.0))  # Batch interval
time.sleep(random.uniform(0.1, 0.5))  # Inter-message delay
```
