package com.maverick.sinks;

import com.maverick.data.DataPoint;
import com.maverick.data.KeyedDataPoint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;

import java.util.concurrent.TimeUnit;

public class InfluxDBSink<T extends DataPoint<? extends Number>> extends RichSinkFunction<T> {

  private transient InfluxDB influxDB = null;
  private static String dataBaseName = "sineWave";
  private static String fieldName = "value";
  private String measurement;

  public InfluxDBSink(String measurement){
    this.measurement = measurement;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    influxDB = InfluxDBFactory.connect("http://localhost:8086", "admin",
            "admin");
//    influxDB.setLogLevel(InfluxDB.LogLevel.valueOf("FULL"));
    influxDB.query(new Query("CREATE DATABASE " + dataBaseName));
    influxDB.setDatabase(dataBaseName);
    String retentionPolicyName = "one_day_only";
    influxDB.query(new Query("CREATE RETENTION POLICY " + retentionPolicyName
            + " ON " + dataBaseName + " DURATION 1d REPLICATION 1 DEFAULT"));
    influxDB.setRetentionPolicy(retentionPolicyName);
    influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);

    //Thread.sleep(20_000L);
  }

  @Override
  public void close() throws Exception {
    super.close();
  }

  @Override
  public void invoke(T dataPoint) throws Exception {
    Point.Builder builder = Point.measurement(measurement)
      .time(dataPoint.getTimeStampMs(), TimeUnit.MILLISECONDS)
      .addField(fieldName, dataPoint.getValue());

    if(dataPoint instanceof KeyedDataPoint){
      builder.tag("key", ((KeyedDataPoint<?>) dataPoint).getKey());
    }

    Point p = builder.build();
    influxDB.write(p);
  }
}
