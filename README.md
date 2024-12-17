# Java Redis Clone

## Overview
This is a basic Redis clone implemented in Java for educational purposes. It mimics core functionalities of Redis 2.x, handling multiple clients using an event-driven approach. This project was developed as part of a challenge, with all implementations done by the author based on a prompt from CodeCrafters.

## Key Features
- **Event Loop for Concurrency**: Efficiently handles multiple client connections without the overhead of threading.
- **RESP Protocol Support**: Full parsing and encoding support for all data types defined up to Redis 2.x.
- **Core Commands**:
   - Configuration: `ConfigCommand`
   - Transaction control: `MultiCommand`, `ExecCommand`, `DiscardCommand`
   - Basic operations: `SetCommand`, `GetCommand`, `IncrCommand`, `EchoCommand`
   - Information retrieval: `InfoCommand`, `KeysCommand`
   - Connection checks: `PingCommand`
- **String Key-Value Storage**: Supports adding and retrieving string data.
- **Transaction Management**: Implements `MULTI`, `EXEC`, and `DISCARD` for transactional command blocks.
- **RDB File Parsing**: Supports loading data from RDB files.
- **Replication**: Capable of running in master or slave mode and performing corresponding behaviors.

## Future Enhancements
- **Streams and Lists**:
   - Support for commands like `XRange`, `XAdd`, `XRead`, `LPush`, `RPop`, etc.
- **Pub/Sub**:
   - Implementation of `PUBLISH` and `SUBSCRIBE` for real-time messaging.
- **Custom Data Structures**:
   - Enhance performance for streams and lists with optimized data structures.
- **Optimistic Locking**:
   - Introduce the `WATCH` command to enable conditional execution.
- **AOF Persistence**:
   - Implement Append-Only File (AOF) persistence for durability.

## Getting Started
### Prerequisites
- Java 17 or higher
- Maven 3.x

### Installation and Running
1. Clone the repository:
   ```bash
   git clone <repository-url>
   ```
2. Build the project:
   ```bash
   mvn clean install
   ```
3. Run the Redis server:
   ```bash
   java -jar target/redis-clone.jar
   ```

### Usage
Connect to the server using a Redis client or a custom script to send supported commands.

## Acknowledgments
This project was inspired by a challenge from **CodeCrafters**. All implementations were done by the author, following the guidelines provided by the challenge.