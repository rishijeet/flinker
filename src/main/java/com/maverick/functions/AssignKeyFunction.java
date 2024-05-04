package com.maverick.functions;

import com.maverick.data.DataPoint;
import com.maverick.data.KeyedDataPoint;
import org.apache.flink.api.common.functions.MapFunction;

public class AssignKeyFunction implements MapFunction<DataPoint<Double>, KeyedDataPoint<Double>> {

  private String key;

  public AssignKeyFunction(String key) {
    this.key = key;
  }

  @Override
  public KeyedDataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
    return dataPoint.withKey(key);
  }
}
