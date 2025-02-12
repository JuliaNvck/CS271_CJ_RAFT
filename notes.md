STEPS
1. define servers - done
2. udp communication - done
3. RAFT
4. define clusters, add more clients to clusters
5. 2PC




1. Setup and Preparation
Install any required libraries: asyncio, grpc (for RPC communication), and optionally sqlalchemy for database operations.
Define the system architecture with three clusters and nine servers.
Create a file-based or in-memory key-value store to maintain the balance of 3000 data items.
2. Implement Server Clusters
Each cluster (C1, C2, C3) manages one shard of data.
Define data replication within each cluster to provide fault tolerance.
Implement a communication layer between servers using gRPC or socket communication.
3. Implement Intra-Shard Transactions (Raft Protocol)
Implement the Raft consensus algorithm:
Leader election among servers in a cluster.
Log replication: Ensure that transactions are added to each server's log.
Commit logic: Execute transactions only after the leader commits them.
Define conditions:
No locks on data items.
Sufficient balance in the sender’s account.
Use locks to handle concurrent access and updates.
4. Implement Cross-Shard Transactions (Two-Phase Commit Protocol)
Implement the client-coordinated 2PC protocol:
Prepare Phase:
The client sends a prepare request to the leaders of the involved shards.
Each leader initiates Raft consensus to approve or abort the transaction.
Commit/Abort Phase:
If all shards approve, the client sends commit messages.
If any shard aborts, the client sends abort messages.
Implement a lock table to prevent conflicts during the transaction process.
5. Define System Functions
PrintBalance Function: Read and print the balance for a specified client ID from all servers in the corresponding cluster.
PrintDatastore Function: Print all committed transactions on each server.
Performance Function: Measure and print throughput and latency.
6. Handle Concurrent Transactions
Implement transaction queues and lock management to support concurrent intra- and cross-shard transactions.
Use Python’s asyncio or multithreading for asynchronous transaction processing.
7. Implement Failure and Timeout Handling
Simulate various failure scenarios such as:
Node failure during consensus.
Lock contention causing transaction aborts.
Timeout or communication failure in the 2PC protocol.
Ensure the system can handle these failures gracefully.
8. Input and Output Handling
Read input transactions from a file containing triplets (x, y, amt).
Use the shard mapping to determine if a transaction is intra-shard or cross-shard.
Implement logging for debugging but remove logs for the final submission.
9. Testing and Validation
Create test cases to verify the following scenarios:
Concurrent transactions on different shards.
Concurrent transactions on the same data items.
Failures during both intra-shard and cross-shard transactions.
No consensus scenarios.
