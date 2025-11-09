# Distributed Image Processing System

A scalable, fault-tolerant distributed system designed to parallelize image processing tasks across multiple nodes using Apache Kafka for orchestration and Redis for state management.

## 👥 Team & Network Configuration

| Role | Team Member | IP Address | Responsibilities |
|---|---|---|---|
| **Broker Node** | Sanjit | `172.23.54.181` | Hosts Kafka Cluster, Zookeeper, and Redis Server |
| **Master Node** | Sarang | `172.23.150.205` | Runs Flask Web UI, splits images, monitors system health |
| **Worker Node 1** | Shreyas | `172.23.168.12` | Consumes tasks, applies transformations (Blur/Grayscale) |
| **Worker Node 2** | Sanjoli | `172.23.159.0` | Consumes tasks, applies transformations (Blur/Grayscale) |

## 🛠️ Technology Stack

- **Language:** Python 3.10+
- **Message Broker:** Apache Kafka (Confluent)
- **State Management:** Redis
- **Web Framework:** Flask
- **Processing:** OpenCV, NumPy, Pillow

## 🏗️ Architecture

The system utilizes a Master-Worker architecture decoupled by Kafka:

1. **Master** splits high-res images (>1024px) into 512x512 tiles.
2. **Kafka** balances these tile tasks across 2 partitions (one for each worker).
3. **Workers** consume tiles in parallel, apply selected transformations, and push results back.
4. **Master** listens for results and reconstructs the final image once all tiles are processed.
5. **Monitor** tracks worker health via continuous heartbeats.

## ⚙️ Setup & Deployment

### 1. Prerequisites (All Nodes)

Ensure Python 3.10+, Git, and ZeroTier are installed and connected.

```bash
git clone <REPOSITORY_URL>
cd bd_project
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Broker Node Setup (Sanjit - 172.23.54.181)

Requires Java (JDK 11+) and Redis Server.

1. Configure Kafka: Ensure `server.properties` has `advertised.listeners=PLAINTEXT://172.23.54.181:9092`.
2. Start Services:

```bash
sudo systemctl start redis-server
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

### 3. Master Node Setup (Sarang - 172.23.150.205)

1. Initial Setup (Run Once):

```bash
python3 setup_kafka.py
```

2. Start Application:

```bash
python3 monitor.py &  # Starts monitoring in background
python3 app.py        # Starts Web UI on port 5000
```

### 4. Worker Node Setup (Shreyas & Sanjoli)

Start the worker processes on respective VMs:

```bash
# On Shreyas's VM (172.23.168.12)
python3 worker.py worker-shreyas

# On Sanjoli's VM (172.23.159.0)
python3 worker.py worker-sanjoli
```

## ✅ Usage Guide

1. Open a browser and navigate to `http://172.23.150.205:5000`.
2. Check the `/dashboard` to confirm both workers are `alive`.
3. Select a transformation (Grayscale or Blur) and upload a high-resolution image (minimum 1024x1024).
4. Wait for parallel processing to complete and download the result.

## 📝 Notes

- Ensure all nodes are connected via ZeroTier before starting services.
- Workers send heartbeats every 5 seconds to maintain `alive` status.
- Image tiles are automatically cleaned up after processing completes.
