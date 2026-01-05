# lygus 

Distributed KV store with Raft consensus and linearizable reads.

(Work in progress, most functionalities are complete just need to iron out details.) 

## Build
```bash
mkdir build && cd build
cmake ..
make
```

## Run
```bash
# Create peers file
cat > peers.txt << EOF
0 127.0.0.1 5000 5001
1 127.0.0.1 5010 5011
2 127.0.0.1 5020 5021
EOF

# Start 3 nodes
./lygus-server -n 0 -p peers.txt -d /tmp/node0 -l 8080 &
./lygus-server -n 1 -p peers.txt -d /tmp/node1 -l 8081 &
./lygus-server -n 2 -p peers.txt -d /tmp/node2 -l 8082 &
```

## Usage
```bash
# Check status
echo "STATUS" | nc localhost 8080

# Write (base64 encoded key/value)
echo "PUT $(echo -n 'mykey' | base64) $(echo -n 'myvalue' | base64)" | nc localhost 8080

# Read
echo "GET $(echo -n 'mykey' | base64)" | nc localhost 8080
```

## Docker
```bash
docker-compose up
```


## license

MIT 