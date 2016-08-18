package org.embulk.output

import com.datastax.driver.core.{Cluster, Session}
import org.embulk.spi.{Column, PageReader}
import org.embulk.spi.`type`.Type
import org.embulk.spi.time.Timestamp
import org.msgpack.value.{ Value => MsgPackValue }

package object cassandra {

  implicit class ColumnConveter(c: Column) {
    import Col._
    def asCol(implicit r: PageReader) =
      if (r isNull c) NullColumn(c.getIndex, c.getName, c.getType) else
        c.getType.getName match {
          case "string"    => StringColumn(c.getIndex, c.getName, r.getString(c))
          case "double"    => DoubleColumn(c.getIndex, c.getName, r.getDouble(c))
          case "long"      => LongColumn(c.getIndex, c.getName, r.getLong(c))
          case "boolean"   => BooleanColumn(c.getIndex, c.getName, r.getBoolean(c))
          case "timestamp" => TimestampColumn(c.getIndex, c.getName, r.getTimestamp(c))
          case "json"      => JsonColumn(c.getIndex, c.getName, r.getJson(c))
          case x           => throw new Exception(s"Uncaught column type: $x")
        }
  }
}

package cassandra {

  case class connect(cluster: Cluster) extends com.google.common.base.Function[String, Session] {
    override def apply(input: String): Session = cluster.connect(input)
  }

  sealed trait Col
  object Col {
    case class DoubleColumn(index: Int, name: String, value: Double) extends Col
    case class StringColumn(index: Int, name: String, value: String) extends Col
    case class BooleanColumn(index: Int, name: String, value: Boolean) extends Col
    case class TimestampColumn(index: Int, name: String, value: Timestamp) extends Col
    case class JsonColumn(index: Int, name: String, value: MsgPackValue) extends Col
    case class LongColumn(index: Int, name: String, value: Long) extends Col
    case class NullColumn(index: Int, name: String, typ: Type) extends Col
  }
}
