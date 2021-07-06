package com.twitter.scalding.parquet.thrift;

import com.twitter.scalding.parquet.ParquetValueScheme;
import com.twitter.scalding.parquet.ScaldingDeprecatedParquetInputFormat;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.mapred.MapredParquetOutputCommitter;
import org.apache.thrift.TBase;

import cascading.flow.FlowProcess;
import cascading.tap.Tap;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.mapred.DeprecatedParquetOutputFormat;
import org.apache.parquet.hadoop.thrift.ThriftReadSupport;
import org.apache.parquet.hadoop.thrift.TBaseWriteSupport;
import org.apache.parquet.thrift.TBaseRecordConverter;

import static org.apache.parquet.hadoop.ParquetInputFormat.READ_SUPPORT_CLASS;

public class ParquetTBaseScheme<T extends TBase<?,?>> extends ParquetValueScheme<T> {

  // In the case of reads, we can read the thrift class from the file metadata
  public ParquetTBaseScheme() {
    this(new Config<T>());
  }

  public ParquetTBaseScheme(Class<T> thriftClass) {
    this(new Config<T>().withRecordClass(thriftClass));
  }

  public ParquetTBaseScheme(FilterPredicate filterPredicate) {
    this(new Config<T>().withFilterPredicate(filterPredicate));
  }

  public ParquetTBaseScheme(FilterPredicate filterPredicate, Class<T> thriftClass) {
    this(new Config<T>().withRecordClass(thriftClass).withFilterPredicate(filterPredicate));
  }

  public ParquetTBaseScheme(ParquetValueScheme.Config<T> config) {
    super(config);
  }

  @Override
  public void sourceConfInit(FlowProcess<? extends Configuration> fp,
      Tap<Configuration, RecordReader, OutputCollector> tap, Configuration jobConf) {
    super.sourceConfInit(fp, tap, jobConf);

    // FIXME(jonshea)
//    jobConf.setInputFormat(ScaldingDeprecatedParquetInputFormat.class);
//    ParquetInputFormat.setReadSupportClass(jobConf, ThriftReadSupport.class);
//    ThriftReadSupport.setRecordConverterClass(jobConf, TBaseRecordConverter.class);

    // Note(jonshea) My current best guess at the fix follows
    jobConf.setClass("mapred.input.format.class", ScaldingDeprecatedParquetInputFormat.class, InputFormat.class);
    jobConf.set(READ_SUPPORT_CLASS, ThriftReadSupport.class.getName());
    ThriftReadSupport.setRecordConverterClass(jobConf, TBaseRecordConverter.class);
  }

  @Override
  public void sinkConfInit(FlowProcess<? extends Configuration> fp,
      Tap<Configuration, RecordReader, OutputCollector> tap, Configuration jobConf) {

    if (this.config.getKlass() == null) {
      throw new IllegalArgumentException("To use ParquetTBaseScheme as a sink, you must specify a thrift class in the constructor");
    }
    // FIXME(jonshea)
//    DeprecatedParquetOutputFormat.setAsOutputFormat(jobConf);
//    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, TBaseWriteSupport.class);
//    TBaseWriteSupport.<T>setThriftClass(jobConf, this.config.getKlass());

    // Note(jonshea) my best guess at the fix.
    jobConf.setClass("mapred.output.format.class", DeprecatedParquetOutputFormat.class, OutputFormat.class);
    jobConf.setClass("mapred.output.committer.class", MapredParquetOutputCommitter.class, OutputCommitter.class);
    DeprecatedParquetOutputFormat.setWriteSupportClass(jobConf, TBaseWriteSupport.class);
    TBaseWriteSupport.<T>setThriftClass(jobConf, this.config.getKlass());
  }
}
