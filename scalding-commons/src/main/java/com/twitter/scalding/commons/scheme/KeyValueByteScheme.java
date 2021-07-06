package com.twitter.scalding.commons.scheme;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import cascading.flow.FlowProcess;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
// import com.twitter.elephantbird.cascading3.scheme.CombinedWritableSequenceFile; // FIXME(jonshea)
import com.twitter.elephantbird.cascading3.scheme.CombinedSequenceFile;


/**
 *
 */
public class KeyValueByteScheme extends CombinedSequenceFile {
  public KeyValueByteScheme(Fields fields) {
    // FIXME(jonshea)
    // super(fields, BytesWritable.class, BytesWritable.class);
  }

  public static byte[] getBytes(BytesWritable key) {
    return Arrays.copyOfRange(key.getBytes(), 0, key.getLength());
  }

  @Override
  public boolean source(FlowProcess<? extends Configuration> flowProcess,
      SourceCall<Object[], RecordReader> sourceCall) throws IOException {
    BytesWritable key = (BytesWritable) sourceCall.getContext()[0];
    BytesWritable value = (BytesWritable) sourceCall.getContext()[1];
    boolean result = sourceCall.getInput().next(key, value);

    if (!result) { return false; }

    Tuple tuple = sourceCall.getIncomingEntry().getTuple();
    tuple.clear();

    tuple.add(getBytes(key));
    tuple.add(getBytes(value));

    return true;
  }

  @Override
  public void sink(FlowProcess<? extends Configuration> flowProcess, SinkCall<Void, OutputCollector> sinkCall)
      throws IOException {
    TupleEntry tupleEntry = sinkCall.getOutgoingEntry();

    byte[] key = (byte[]) tupleEntry.getObject(0);
    byte[] val = (byte[]) tupleEntry.getObject(1);

    sinkCall.getOutput().collect(new BytesWritable(key), new BytesWritable(val));
  }
}

