package com.maverick.functions;

import com.maverick.data.DataPoint;
import org.apache.flink.api.common.functions.RichMapFunction;

public class SquareWaveFunction extends RichMapFunction<DataPoint<Double>, DataPoint<Double>> {
  @Override
  public DataPoint<Double> map(DataPoint<Double> dataPoint) throws Exception {
    double value = 0.0;
    if(dataPoint.getValue() > 0.4){
      value = 1.0;
    }
    return dataPoint.withNewValue(value);
  }
}
