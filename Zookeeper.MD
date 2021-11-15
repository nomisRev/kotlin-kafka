
# Zookeeper

Apache Kafka uses Zookeeper to store metadata about the Kafka cluster, as well as consumer client details,

## Terminology 

- ensemble: Zookeeper is designed to work as a cluster, called an ensemble, to ensure high availability.
  => It is recommended that ensembles contain an odd number of servers. (Recommended: 5)
  => With 5, 2 nodes can go down. 1 maintenance, and 1 fatal. Higher introduces latency without observers
  => Observer nodes for help in balancing read-only traffic.

