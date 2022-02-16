package com.twitter.scalding.tap;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;

import com.twitter.scalding.tuple.HadoopTupleEntrySchemeIterator;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;

public class ScaldingHfs extends cascading.tap.hadoop.Hfs {
  protected ScaldingHfs() {
  }

  protected ScaldingHfs(Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme) {
    super(scheme);
  }

  public ScaldingHfs(Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, String stringPath) {
    super(scheme, stringPath);
  }

  public ScaldingHfs(Scheme<Configuration, RecordReader, OutputCollector, ?, ?> scheme, String stringPath, SinkMode sinkMode) {
    super(scheme, stringPath, sinkMode);
  }

  @Override
  public TupleEntryIterator openForRead(FlowProcess<? extends Configuration> flowProcess, RecordReader input) throws IOException {
    return new HadoopTupleEntrySchemeIterator((FlowProcess<Configuration>) flowProcess, this, input);
  }
}
