package org.embulk.output.cassandra

import com.google.common.base.Optional
import org.embulk.config.TaskReport
import org.embulk.config.Config
import org.embulk.config.ConfigDefault
import org.embulk.config.ConfigDiff
import org.embulk.config.ConfigSource
import org.embulk.config.Task
import org.embulk.config.TaskSource
import org.embulk.spi.Exec
import org.embulk.spi.OutputPlugin
import org.embulk.spi.Schema
import org.embulk.spi.TransactionalPageOutput

object CassandraOutputPlugin {

  trait PluginTask extends Task {
    @Config("concat-points")
    @ConfigDefault("[\"127.0.0.1\"]") def getConcatPoints: java.util.List[String]

    @Config("port")
    @ConfigDefault("9042") def getPort: Int

    @Config("keyspace")
    @ConfigDefault("null") def getKeyspace: Optional[String]

    @Config("query") def getQuery: String

    @Config("timeout-in-ms")
    @ConfigDefault("15000") def getTimeoutMillis: Long

    @Config("user")
    @ConfigDefault("null") def getUser: Optional[String]

    @Config("passwd")
    @ConfigDefault("null") def getPasswd: Optional[String]
  }

}

class CassandraOutputPlugin extends OutputPlugin {
  import CassandraOutputPlugin._
  def transaction(config: ConfigSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control): ConfigDiff = {
    val task: PluginTask = config.loadConfig(classOf[PluginTask])
    // retryable (idempotent) output:
    // return resume(task.dump(), schema, taskCount, control);
    // non-retryable (non-idempotent) output:
    control.run(task.dump)
    Exec.newConfigDiff
  }

  def resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control): ConfigDiff =
    throw new UnsupportedOperationException("cassandra output plugin does not support resuming")

  def cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: java.util.List[TaskReport]): Unit = {}

  def open(taskSource: TaskSource, schema: Schema, taskIndex: Int): TransactionalPageOutput =
    CassandraPageOutput(taskSource, schema, taskIndex)
}