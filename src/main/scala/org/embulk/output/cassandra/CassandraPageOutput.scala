package org.embulk.output.cassandra

import java.util.concurrent.Executors.newWorkStealingPool

import cats.implicits._
import com.datastax.driver.core.{Cluster, PlainTextAuthProvider}
import com.datastax.driver.core.policies.{DowngradingConsistencyRetryPolicy, ExponentialReconnectionPolicy}
import org.embulk.config.{TaskReport, TaskSource}
import org.embulk.output.cassandra.CassandraOutputPlugin.PluginTask
import org.embulk.spi._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object CassandraPageOutput {
  final val logger = Exec.getLogger(classOf[CassandraPageOutput])
  final val pool = newWorkStealingPool()
}

final case class CassandraPageOutput(taskSource: TaskSource, schema: Schema, taskIndex: Int)
    extends TransactionalPageOutput {
  import CassandraPageOutput._

  private[this] val task: PluginTask = taskSource.loadTask(classOf[PluginTask])
  private[this] implicit val reader: PageReader = new PageReader(schema)

  private[this] val builder = Cluster.builder()
    .addContactPoints(task.getConcatPoints.asScala: _*)
    .withPort(task.getPort)
    .withReconnectionPolicy(new ExponentialReconnectionPolicy(50, 5000))
    .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
    .withoutJMXReporting()
    .withoutMetrics()

  private[this] val user = task.getUser
  private[this] val passwd = task.getPasswd

  if (user.isPresent && passwd.isPresent)
    builder.withCredentials(user.get, passwd.get)

  private[this] val cluster = builder.build()
  private[this] val session = task.getKeyspace.transform(connect(cluster)).or(cluster.connect())
  private[this] val prepared = session.prepare(task.getQuery)

  override def finish(): Unit = logger.info("Finish")

  override def close(): Unit = cluster.close

  override def abort(): Unit = logger.error("Abort")

  override def add(page: Page): Unit = {
    import Col._
    reader.setPage(page)

    @tailrec def loop(acc: List[List[Col]]): List[List[Col]] =
      if (!reader.nextRecord()) acc
      else loop(acc :+ reader.getSchema.getColumns.asScala.map(_.asCol).toList)

    def convert(c: Col): Object = c match {
      case StringColumn(i, n, v)    => v
      case DoubleColumn(i, n, v)    => Double.box(v)
      case LongColumn(i, n, v)      => Long.box(v)
      case BooleanColumn(i, n, v)   => Boolean.box(v)
      case TimestampColumn(i, n, v) => Long.box(v.toEpochMilli)
      case JsonColumn(i, n, v)      => v.asRawValue().asByteBuffer()
      case NullColumn(i, n, _)      => null
    }

    def process(cs: List[Col]): Future[Unit] = Future {
      val stmt = prepared.bind(cs.map(convert): _*)
      session.execute(stmt)
    }

    Await.result(loop(List.empty).traverse(process), task.getTimeoutMillis.millis)
  }

  override def commit(): TaskReport = Exec.newTaskReport
}
