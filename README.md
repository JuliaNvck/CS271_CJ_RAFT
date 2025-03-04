# Distributed Transaction Processing System (CS 271: Distributed Systems)

This project implements a fault-tolerant distributed transaction processing system for a simple banking application. The system uses the Raft protocol for intra-shard transactions and the Two-Phase Commit (2PC) protocol for cross-shard transactions, ensuring consistency and fault tolerance across distributed clusters.

---

## Project Description

### Banking Application
The application supports transfer transactions in the form `(x, y, amt)`, where:
- `x` is the sender
- `y` is the receiver
- `amt` is the amount to transfer

### System Architecture
- **Data Partitioning**: The data is partitioned into three shards, each managed by a cluster of three servers.
- **Fault Tolerance**: Data shards are fully replicated within their respective clusters, allowing for fail-stop tolerance with at most one server failure per cluster.
- **Transaction Types**:
  - **Intra-Shard Transactions**: Transactions within the same shard, processed using the Raft protocol.
  - **Cross-Shard Transactions**: Transactions involving multiple shards, managed by the Two-Phase Commit (2PC) protocol.

---

## Features

- **Fault Tolerance**: Supports fail-stop failures with replication.
- **Consensus Mechanism**: Utilizes the Raft protocol for intra-shard transactions.
- **Two-Phase Commit (2PC)**: Manages cross-shard transactions for atomicity.
- **Locking Mechanism**: Implements lock tables to prevent concurrent updates to data items.
- **Performance Metrics**: Measures throughput and latency of transactions.
- **Efficient Communication**: Uses non-blocking UDP sockets to facilitate fast message delivery and reduced overhead.

---

## Implementation Details

### Data Management
- **Dataset Size**: 3000 data items (`id` from 1 to 3000), evenly distributed across clusters.
- **Shard Mapping**:
  - Cluster 1: Items 1 - 1000
  - Cluster 2: Items 1001 - 2000
  - Cluster 3: Items 2001 - 3000
- **Initial Balance**: All data items start with a balance of 10 units.

### Client Functions

#### 1. `PrintBalance <account_id>`
- Displays the balance of a given client across all servers in the relevant cluster.

#### 2. `PrintDatastore`
- Shows the set of committed transactions on each server.

#### 3. `Performance`
- Outputs throughput and latency metrics.

---

## How It Works

### Intra-Shard Transactions (Raft Protocol)
1. Client sends a transaction to a server in the relevant cluster.
2. Raft protocol ensures consensus within the cluster.
3. The leader server verifies:
   - No locks exist on accounts `x` and `y`.
   - Account `x` has sufficient balance.
4. On consensus, the transaction is committed and the client is notified.

### Cross-Shard Transactions (2PC Protocol)
1. Client acts as the 2PC coordinator.
2. Sends requests to leaders of the involved clusters.
3. Each cluster uses the Raft protocol to achieve consensus.
4. Lock tables are used to prevent concurrent data modifications.
5. The client gathers votes from clusters:
   - If all clusters vote "yes", a commit message is broadcasted.
   - If any cluster votes "no" or fails, an abort message is sent.

### UDP Socket Communication
- **Non-blocking Communication**: The server and client use non-blocking UDP sockets (`socket.AF_INET`, `socket.SOCK_DGRAM`) for lightweight, fast, and connectionless message passing.
- **Asynchronous Processing**: Threads handle message listening and transaction processing concurrently.
- **Error Handling**: Implemented to handle timeouts, lost packets, and server failures gracefully.

---

## Testing and Test Cases

The system handles a variety of scenarios, including:
- Independent intra-shard and cross-shard transactions.
- Transactions with overlapping data items.
- Concurrent Transactions (both intra-shard and cross-shard)
- Failures and timeout scenarios in the 2PC protocol.
- No consensus when too many servers fail.

---

## Running the Project
./launch.sh

