/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.flink

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.CoreOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.hadoop.tracing.TraceAdminPB.ConfigPairOrBuilder
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterResult}
import org.slf4j.{Logger, LoggerFactory}

class FlinkScalaBatchSqlInterpreter(scalaInterpreter: FlinkScalaInterpreter,
                                    z: FlinkZeppelinContext,
                                    maxRow: Int) {
  lazy val LOGGER: Logger = LoggerFactory.getLogger(getClass)

  private val btenv: BatchTableEnvironment = scalaInterpreter.getBatchTableEnvironment()
  private val senv: StreamExecutionEnvironment = scalaInterpreter.getStreamExecutionEnvironment()

  def interpret(code: String, context: InterpreterContext): InterpreterResult = {
    try {
      val parallelism = context.getLocalProperties.getOrDefault("parallelism",
        scalaInterpreter.getDefaultParallelism + "").toInt
      this.btenv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, parallelism)
      LOGGER.info("Run Flink batch sql job with parallelism: " + parallelism)
      val table = this.btenv.sqlQuery(code)
      val result = z.showTable(table, code)
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, result)
    } catch {
      case e: Exception =>
        LOGGER.error("Fail to run flink batch sql", e)
        return new InterpreterResult(InterpreterResult.Code.ERROR, ExceptionUtils.getStackTrace(e))
    }
  }
}
