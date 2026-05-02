# distributed-weather-observability-platform
A distributed IoT weather monitoring system built with Kafka, Java, and Kubernetes. Simulates real-time weather stations streaming data through a data-intensive pipeline including stream processing, Bitcask key-value storage, Parquet archiving, and Elasticsearch/Kibana analytics.


## DONE
- Weather stations mock
- Kafka Configuration
- Integrate API to weather stations
- Kafka humidity processor
- BitCask Implementation

## TO-DO
- Revise BitCask implementation + Implement BitCask Client
- Parquet files implementation (In central station)
- Parquet files (Elastic search + Kibana)
- JFR monitoring
- Deployment using kubernetes
- EXTRA: GUI that utilizes bitcask client , JFR monitoring and observability 
- BONUS: Integration patterns