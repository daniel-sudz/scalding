/*
Copyright 2013 Inkling, Inc.

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

import cascading.tap.SinkMode
import cascading.tap.Tap
import cascading.tuple.Fields

/**
 * This is a base class for template based output sources
 */
@deprecated("TemplateSource has been removed. Use PartitionSource instead", "0.19")
abstract class TemplateSource extends SchemedSource with HfsTapProvider {
  /**
   * Creates the template tap.
   *
   * @param readOrWrite Describes if this source is being read from or written to.
   * @param mode The mode of the job. (implicit)
   *
   * @return A cascading TemplateTap.
   */
  @deprecated("TemplateSource has been removed. Use PartitionSource instead", "0.19")
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    throw new NotImplementedError("")
  }

  /**
   * Validates the taps, makes sure there are no nulls as the path or template.
   *
   * @param mode The mode of the job.
   */
  @deprecated("TemplateSource has been removed. Use PartitionSource instead", "0.19")
  override def validateTaps(mode: Mode): Unit = {
  }
}

/**
 * An implementation of TSV output, split over a template tap.
 *
 * @param basePath The root path for the output.
 * @param template The java formatter style string to use as the template. e.g. %s/%s.
 * @param pathFields The set of fields to apply to the path.
 * @param writeHeader Flag to indicate that the header should be written to the file.
 * @param sinkMode How to handle conflicts with existing output.
 * @param fields The set of fields to apply to the output.
 */
@deprecated("TemplatedTsv has been removed. Use PartitionTsv instead", "0.19")
case class TemplatedTsv(
  val basePath: String,
  val template: String,
  val pathFields: Fields = Fields.ALL,
  override val writeHeader: Boolean = false,
  override val sinkMode: SinkMode = SinkMode.REPLACE,
  override val fields: Fields = Fields.ALL)
  extends TemplateSource with DelimitedScheme

/**
 * An implementation of SequenceFile output, split over a template tap.
 *
 * @param basePath The root path for the output.
 * @param template The java formatter style string to use as the template. e.g. %s/%s.
 * @param sequenceFields The set of fields to use for the sequence file.
 * @param pathFields The set of fields to apply to the path.
 * @param sinkMode How to handle conflicts with existing output.
 */
@deprecated("TemplatedSequenceFile has been removed. Use PartitionSequenceFile instead", "0.19")
case class TemplatedSequenceFile(
  val basePath: String,
  val template: String,
  val sequenceFields: Fields = Fields.ALL,
  val pathFields: Fields = Fields.ALL,
  override val sinkMode: SinkMode = SinkMode.REPLACE)
  extends TemplateSource with SequenceFileScheme