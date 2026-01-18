import sys
import time
import random
import threading
import requests
import logging
from flask import Flask, request, jsonify

log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

# ================= RAFT NODE CLASS =================
class RaftNode:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        
        # --- Persistent State ---
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Format: [{'term': 1, 'cmd': 'SET x=5'}]
        
        # --- Volatile State ---
        self.commit_index = -1 
        self.last_applied = -1
        self.state = "FOLLOWER"
        self.leader_id = None
        
        # Leader state: replication tracking
        self.match_index = {} 
        
        # Timers
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(2.0, 4.0) 
        
        self.running = True
        # Background timer loop
        threading.Thread(target=self.run_loop, daemon=True).start()
        print(f"[{self.node_id}] Node started at port {self.port} as FOLLOWER")

    # Main loop checking timeouts and state
    def run_loop(self):
        while self.running:
            current_time = time.time()
            
            if self.state == "LEADER":
                self.send_heartbeats()
                time.sleep(1.0) # Heartbeat interval
            
            elif self.state in ["FOLLOWER", "CANDIDATE"]:
                if current_time - self.last_heartbeat > self.election_timeout:
                    print(f"[{self.node_id}] Timeout! Starting election...")
                    self.start_election()
            
            # Apply committed entries to state machine
            while self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied]
                print(f"[{self.node_id}] Entry committed & applied (index={self.last_applied}): {entry['cmd']}")
            
            time.sleep(0.1)

    # --- ELECTION LOGIC ---
    def start_election(self):
        self.state = "CANDIDATE"
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(2.0, 4.0)
        
        print(f"[{self.node_id}] Candidate (term {self.current_term})")
        
        votes = 1 # Vote for self
        
        # Helper to send vote request
        def ask_vote(peer):
            try:
                url = f"http://{peer}/request_vote"
                res = requests.post(url, json={
                    "term": self.current_term,
                    "candidate_id": self.node_id
                }, timeout=0.5)
                if res.status_code == 200:
                    return res.json()
            except:
                return None

        # Request votes in parallel
        threads = []
        results = []
        for peer in self.peers:
            t = threading.Thread(target=lambda: results.append(ask_vote(peer)))
            t.start()
            threads.append(t)
        
        for t in threads: t.join() # Wait for threads
        
        # Count votes
        for res in results:
            if res:
                if res['term'] > self.current_term:
                    self.step_down(res['term'])
                    return
                if res['vote_granted']:
                    votes += 1

        majority = (len(self.peers) + 1) // 2 + 1
        print(f"[{self.node_id}] Votes received: {votes}/{len(self.peers)+1}")
        
        if self.state == "CANDIDATE" and votes >= majority:
            print(f"[{self.node_id}] Received majority votes → Leader")
            self.become_leader()

    def become_leader(self):
        self.state = "LEADER"
        self.leader_id = self.node_id
        self.match_index = {peer: -1 for peer in self.peers}
        self.send_heartbeats()

    def step_down(self, term):
        if term > self.current_term:
            self.current_term = term
            self.state = "FOLLOWER"
            self.voted_for = None
            self.leader_id = None
            print(f"[{self.node_id}] Stepped down to FOLLOWER (Term {self.current_term})")

    # --- REPLICATION LOGIC ---
    def send_heartbeats(self):
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries, args=(peer,)).start()

    def send_append_entries(self, peer):
        try:
            url = f"http://{peer}/append_entries"
            # Send full log (Lite implementation)
            data = {
                "term": self.current_term,
                "leader_id": self.node_id,
                "entries": self.log, 
                "leader_commit": self.commit_index
            }
            res = requests.post(url, json=data, timeout=0.5)
            
            if res.status_code == 200:
                resp = res.json()
                if resp['term'] > self.current_term:
                    self.step_down(resp['term'])
                elif resp['success']:
                    # Update replication index on success
                    self.match_index[peer] = len(self.log) - 1
                    self.update_commit_index()
        except:
            pass

    def update_commit_index(self):
        # Check if entry is replicated on majority
        indexes = sorted(list(self.match_index.values()) + [len(self.log) - 1], reverse=True)
        majority_idx = (len(self.peers) + 1) // 2
        N = indexes[majority_idx] 
        
        if N > self.commit_index:
            self.commit_index = N

# ================= HTTP HANDLERS =================
node = None

# Handle vote requests from other nodes
@app.route('/request_vote', methods=['POST'])
def request_vote():
    data = request.json
    term = data['term']
    candidate_id = data['candidate_id']
    
    if term > node.current_term:
        node.step_down(term)
    
    vote_granted = False
    if term == node.current_term and (node.voted_for is None or node.voted_for == candidate_id):
        vote_granted = True
        node.voted_for = candidate_id
        node.last_heartbeat = time.time()
        print(f"[{node.node_id}] Voted for {candidate_id}")
        
    return jsonify({"term": node.current_term, "vote_granted": vote_granted})

# Handle log replication (Heartbeat)
@app.route('/append_entries', methods=['POST'])
def append_entries():
    data = request.json
    term = data['term']
    leader_id = data['leader_id']
    entries = data['entries']
    leader_commit = data['leader_commit']
    
    if term >= node.current_term:
        node.current_term = term
        node.state = "FOLLOWER"
        node.leader_id = leader_id
        node.last_heartbeat = time.time()
        
        # Overwrite log if leader has more data (Lite logic)
        if len(entries) >= len(node.log):
            node.log = entries
        
        # Sync commit index
        if leader_commit > node.commit_index:
            node.commit_index = min(leader_commit, len(node.log) - 1)
            
        return jsonify({"term": node.current_term, "success": True})
    
    return jsonify({"term": node.current_term, "success": False})

# Client API to add commands
@app.route('/submit', methods=['POST'])
def submit():
    cmd = request.json.get('cmd')
    
    if node.state != "LEADER":
        return jsonify({"error": "Not leader", "leader is": node.leader_id}), 400
        
    entry = {'term': node.current_term, 'cmd': cmd}
    node.log.append(entry)
    print(f"[{node.node_id}] Client command received: {cmd}. Appended to log.")
    return jsonify({"status": "received"})

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("Usage: python3 node.py <ID> <PORT> <PEERS>")
        sys.exit(1)
    
    # Args: ID, Port, Peers List
    my_id = sys.argv[1]
    port = int(sys.argv[2])
    peers = sys.argv[3].split(',') if sys.argv[3] else []
    
    node = RaftNode(my_id, port, peers)
    app.run(host='0.0.0.0', port=port)