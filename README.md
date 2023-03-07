# Distributed Systems Assignment 2
Design and implement a multi broker distributed logging queue system by dividing a topic queue into multiple partition queues with producers and consumers who can push or pop message logs specific to a topic in any partition or a round robin fashion if not specified. The queue needs to be persistent. 
Also, develop client libraries for easy usage of the Queue API

## Problem Statement
Steps of the problem statement
- Implementing a logging distributed queue 
- Adding persistence to the system
- Developing library for client producers and consumers
- Horizontally scale topics into partitions
- Customize brokers to handle partitions
- Implement a broker manager to handle client, topic, partition and broker metadata
- Add functionality of read-only replica of broker manager
- Implement healthcheck mechanism for clients and brokers

## Languages and Libraries
The whole software is written in **Python** programming language. **FastAPI** framework is used to implement HTTP APIs of the logging distributed queue. **SQLAlchemy** is used for ORM of database to support persistence in the system. Currently the brokers use seperate **Sqlite3** databases and managers use a shared **PostgreSQL** database. 

## How to run
### Running from source
1. Clone this repository
```
git clone https://github.com/Shashvat6264/DS-Assignment-2.git
```

2. Change directory to the repository
```
cd DS-Assignment-2
```

3. Install all dependencies
```
pip install -r Manager/requirements.txt
```

4. Start the broker manager
```
chmod +x Manager/start.sh
./Manager/start.sh
```

5. Start a broker on a different port
```
export PORT=8001
chmod +x Broker/start.sh
./Broker/start.sh
```

5. Run your own programs or visit [localhost:8000/docs](localhost:8000/docs) to view the API provided

## Environment variables
For the broker, it requires 3 environment variables.
1. PORT: The port on which the server runs. Default is set to be 8000
2. MANAGER_ADDRESS: Address of the main manager. Default is set to http://127.0.0.1:8000
3. CURRENT_ADDRESS: Address of the broker. Default is set to http://127.0.0.1:$PORT

For the manager, it requires 2 environment variables.
1. PORT: The port on which the server runs. Default is set to be 8000
2. READ_ONLY: This variables specifies if the manager instance to be started is a read-only replica. This can be set True by using -r flag. Default is set to be False.


## Using the client library
The client library is named as **myqueue**. The library can be imported in python programs and used directly as long as the logging-queue service is running. Detailed documentation for the library is provided in documentation file.

