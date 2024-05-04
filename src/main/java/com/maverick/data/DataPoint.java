package com.maverick.data;

public class DataPoint<T> {

  private long timeStampMs;
  private T value;

  public DataPoint() {
    this(0, null);
  }

  public DataPoint(long timeStampMs, T value) {
    this.timeStampMs = timeStampMs;
    this.value = value;
  }

  public long getTimeStampMs() {
    return timeStampMs;
  }

  public void setTimeStampMs(long timeStampMs) {
    this.timeStampMs = timeStampMs;
  }

  public T getValue() {
    return value;
  }

  public void setValue(T value) {
    this.value = value;
  }

  public <R> DataPoint<R> withNewValue(R newValue){
    return new DataPoint<>(this.timeStampMs, newValue);
  }

  public <R> KeyedDataPoint<R> withNewKeyAndValue(String key, R newValue){
    return new KeyedDataPoint<>(key, this.timeStampMs, newValue);
  }

  public KeyedDataPoint withKey(String key){
    return new KeyedDataPoint<>(key, this.timeStampMs, this.getValue());
  }

  @Override
  public String toString() {
    return String.format("DataPoint(timestamp=%d, value=%s)", timeStampMs, value);
  }
}
