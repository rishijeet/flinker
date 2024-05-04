# Apache Flink
Apache Flink in best of its action with the streaming data.
Use JDK 11 for the existing code. 
Installation

## Install influxdb
```
brew install influxdb@1
/opt/homebrew/opt/influxdb@1/bin/influxd -config /opt/homebrew/etc/influxdb.conf
```

Check the version /opt/homebrew/Cellar/influxdb@1/1.11.5

## Install Grafana
```
brew services start grafana
```
## Listener
```
nc -lk 9999
```

# Disclaimer
Apache®, Apache Flink™, Flink™, and the Apache feather logo are trademarks of [The Apache Software Foundation](http://apache.org).
