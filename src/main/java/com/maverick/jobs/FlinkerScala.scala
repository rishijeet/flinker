package com.maverick.jobs

import java.lang.Double

import com.maverick.data.KeyedDataPoint.sum
import com.maverick.data.{ControlMessage, KeyedDataPoint}
import com.maverick.functions.{AmplifierFunction, MaxTimestampAssigner}
import com.maverick.sinks.InfluxDBSink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkerScala {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.enableCheckpointing(1000)

    val stream = DataGenerator.stream(env)

    stream
      .addSink(new InfluxDBSink[KeyedDataPoint[Double]]("sensors"))

    val controlStream = env.socketTextStream("localhost", 9999)
        .map(ControlMessage.fromString(_))

    val amplifierStream = stream
      .keyBy(_.getKey)
      .connect(controlStream.keyBy(_.getKey))
      .flatMap(new AmplifierFunction)

    amplifierStream
      .addSink(new InfluxDBSink[KeyedDataPoint[Double]]("AmplifiedSensors"))

    amplifierStream
      .keyBy(_.getKey)
      .timeWindow(Time.seconds(1))
      .reduce(sum _)
      .addSink(new InfluxDBSink[KeyedDataPoint[Double]]("summedSensors"))


    env.execute("Flinker")
  }
}
