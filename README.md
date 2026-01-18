# Raft Lite: Distributed Consensus Algorithm

This repository contains a simplified implementation of the Raft Consensus Algorithm ("Raft Lite") written in Python. It demonstrates core distributed systems concepts including Leader Election, Log Replication, and Fault Tolerance using HTTP JSON APIs.

## 📋 Prerequisites & Infrastructure

Before running the code, you must provision the infrastructure. This project is designed to run on **3 separate nodes** (Virtual Machines), though it can be tested locally on different ports.

### 1. Provision Servers
You need to create **3 Virtual Machines** (e.g., AWS EC2 instances).
*   **OS:** Ubuntu 22.04 LTS (recommended)
*   **Type:** t2.micro or t3.micro
*   **Network:** Ensure all nodes are in the same VPC/Subnet.

### 2. Network Configuration (Security Groups)
**Crucial:** You must configure your Firewall / AWS Security Groups to allow traffic:
*   **SSH (Port 22):** For your management access.
*   **Custom TCP (Port 5000):** For internal node communication (Node-to-Node).

### 3. Software Dependencies
On **each** node, install Python 3 and the required libraries:

```bash
sudo apt update
sudo apt install python3-pip -y
pip3 install flask requests --break-system-packages
```

How to Run

1. Deploy Code
Copy node.py to all three servers. You can use scp or git clone.

2. Identify IPs
Write down the Private IPs of your 3 nodes. Let's assume:

Node A: 10.0.0.1
Node B: 10.0.0.2
Node C: 10.0.0.3
3. Start the Cluster
SSH into each server and run the corresponding command.
Note: We use Private IPs for peer communication.

On Node A:

code
Bash
python3 node.py A 5000 10.0.0.2:5000,10.0.0.3:5000
On Node B:

code
Bash
python3 node.py B 5000 10.0.0.1:5000,10.0.0.3:5000
On Node C:

code
Bash
python3 node.py C 5000 10.0.0.1:5000,10.0.0.2:5000
Once started, the nodes will automatically perform a Leader Election. One node will print Received majority votes -> Leader.

	Testing & Usage
To send commands to the cluster, you act as a client sending a POST request to the Leader's Public IP.

Submit a Command
code
Bash
curl -X POST -H "Content-Type: application/json" \
-d '{"cmd": "SET x=100"}' \
http://<LEADER_PUBLIC_IP>:5000/submit
If successful, all nodes will replicate the log and print:
Entry committed & applied ... SET x=100

Test Fault Tolerance
Identify the Leader.
Stop the Leader process (Ctrl+C).
Observe the other nodes detecting the failure and electing a new Leader.
Submit a new command to the new Leader to verify availability.
	File Structure
node.py: Main entry point. Contains Raft class, Flask server, and consensus logic.
README.md: Instructions for deployment.
	Implementation Notes
Heartbeats: Sent every 1.0s.
Election Timeout: Randomized between 2.0s - 4.0s to minimize split votes.
Consistency: A command is only applied to the State Machine after being replicated to a majority (2 out of 3) of nodes.
