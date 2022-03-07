/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.twitter.scalding

import cascading.flow.FlowConnector
import cascading.tap.Tap
import cascading.tuple.TupleEntryIterator
import org.apache.hadoop.conf.Configuration

case class ModeException(message: String) extends RuntimeException(message)

case class ModeLoadException(message: String, origin: ClassNotFoundException) extends RuntimeException(origin)

object Mode {

  /**
   * This is a Args and a Mode together. It is used purely as a work-around for the fact that Job only accepts
   * an Args object, but needs a Mode inside.
   */
  private class ArgsWithMode(argsMap: Map[String, List[String]], val mode: Mode) extends Args(argsMap) {
    override def +(keyvals: (String, Iterable[String])): Args =
      new ArgsWithMode(super.+(keyvals).m, mode)
  }

  /** Attach a mode to these Args and return the new Args */
  def putMode(mode: Mode, args: Args): Args = new ArgsWithMode(args.m, mode)

  /** Get a Mode if this Args was the result of a putMode */
  def getMode(args: Args): Option[Mode] = args match {
    case withMode: ArgsWithMode => Some(withMode.mode)
    case _                      => None
  }

  val CascadingFlowConnectorClassKey = "cascading.flow.connector.class"
  val CascadingFlowProcessClassKey = "cascading.flow.process.class"

  val DefaultHadoop3Mr1FlowConnector = "cascading.flow.hadoop3.Hadoop3MRFlowConnector"
  val DefaultHadoop3Mr1FlowProcess = "cascading.flow.hadoop.HadoopFlowProcess"

  val DefaultHadoop3TezFlowConnector = "cascading.flow.tez.Hadoop3TezFlowConnector"
  val DefaultHadoop3TezFlowProcess = "cascading.flow.tez.Hadoop3TezFlowProcess"

  val DefaultHadoopFlowConnector = DefaultHadoop3Mr1FlowConnector
  val DefaultHadoopFlowProcess = DefaultHadoop3Mr1FlowProcess

  // This should be passed ALL the args supplied after the job name
  def apply(args: Args, config: Configuration): Mode = {
    val strictSources = args.boolean("tool.partialok") == false
    if (!strictSources) {
      // TODO we should do smarter logging here
      println("[Scalding:INFO] using --tool.partialok. Missing log data won't cause errors.")
    }

    if (args.boolean("local"))
      Local(strictSources)
    else if (
      args.boolean("hdfs")
    ) /* FIXME: should we start printing deprecation warnings ? It's okay to set manually c.f.*.class though */
      Hdfs(strictSources, config)
    else if (args.boolean("hadoop3")) {
      config.set(CascadingFlowConnectorClassKey, DefaultHadoopFlowConnector)
      config.set(CascadingFlowProcessClassKey, DefaultHadoopFlowProcess)
      Hdfs(strictSources, config)
    } else if (args.boolean("hadoop3-mr1")) {
      config.set(CascadingFlowConnectorClassKey, DefaultHadoop3Mr1FlowConnector)
      config.set(CascadingFlowProcessClassKey, DefaultHadoop3Mr1FlowProcess)
      Hdfs(strictSources, config)
    } else if (args.boolean("hadoop3-tez")) {
      config.set(CascadingFlowConnectorClassKey, DefaultHadoop3TezFlowConnector)
      config.set(CascadingFlowProcessClassKey, DefaultHadoop3TezFlowProcess)
      Hdfs(strictSources, config)
    } else
      throw ArgsException(
        "[ERROR] Mode must be one of --local, --hadoop3, --hadoop3-mr1, --hadoop3-tez or --hdfs, you provided none"
      )
  }

  @deprecated(
    "Use CascadingMode.cast(mode) or pattern match directly on known CascadingModes (e.g. Hdfs, Local)",
    "0.18.0"
  )
  implicit class DeprecatedCascadingModeMethods(val mode: Mode) extends AnyVal {
    private def cmode: CascadingMode = CascadingMode.cast(mode)

    def openForRead(config: Config, tap: Tap[_, _, _]): TupleEntryIterator =
      cmode.openForRead(config, tap)

    def openForRead(tap: Tap[_, _, _]): TupleEntryIterator =
      openForRead(Config.defaultFrom(mode), tap)

    // Returns true if the file exists on the current filesystem.
    def fileExists(filename: String): Boolean =
      cmode.fileExists(filename)

    /** Create a new FlowConnector for this cascading planner */
    def newFlowConnector(props: Config): FlowConnector =
      cmode.newFlowConnector(props)
  }
}

trait Mode extends java.io.Serializable {

  /**
   * Make the Execution.Writer for this platform
   */
  def newWriter(): Execution.Writer
}
