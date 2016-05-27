package org.stsffap

/**
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

import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, Table, TableEnvironment}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingJob {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements(Order(1,1), Shipment(1,2), Delivery(1, 3), Order(2, 2), Order(3, 14))

    val inputTable = input.toTable(tableEnv, 'orderId as 'orderId, 'timestamp as 'time, 'status as 'status)

    // calculate the number of orders per hour
    val ordersPerHour: Table = inputTable
      .where('status === "Received")
      .window(Tumbling every Days(1) on 'time as 'w)
      .count()

    ordersPerHour.toDataStream[Row].print()

    // calculate the processing warnings
    val processingPattern = Pattern.begin[Event]("received").subtype(classOf[Order])
      .followedBy("shipped").subtype(classOf[Shipment])
      .within(Time.hours(1))

    val processingPatternStream = CEP.pattern(input.keyBy("orderId"), processingPattern)

    val processingResult: DataStream[Either[ProcessingWarning, ProcessingSuccess]] = processingPatternStream.select {
      (partialPattern, timestamp) => ProcessingWarning(partialPattern("received").orderId, timestamp)
    } {
      fullPattern =>
        ProcessingSuccess(
          fullPattern("received").orderId,
          fullPattern("shipped").timestamp,
          fullPattern("shipped").timestamp - fullPattern("received").timestamp)
    }

    // calculate the delivery warnings
    val deliveryPattern = Pattern.begin[Event]("shipped").where(_.status == "Shipped")
      .followedBy("delivered").where(_.status == "Delivered")
      .within(Time.days(1))

    val deliveryPatternStream = CEP.pattern(input.keyBy("orderId"), deliveryPattern)

    val deliveryResult: DataStream[Either[DeliveryWarning, DeliverySuccess]] = deliveryPatternStream.select {
      (partialPattern, timestamp) => DeliveryWarning(partialPattern("shipped").orderId, timestamp)
    } {
      fullPattern =>
        DeliverySuccess(
          fullPattern("shipped").orderId,
          fullPattern("delivered").timestamp,
          fullPattern("delivered").timestamp - fullPattern("shipped").timestamp
        )
    }

    val processingWarnings = processingResult.flatMap (_.left.toOption)

    val processingSuccesses = processingResult.flatMap (_.right.toOption)

    val deliveryWarnings = deliveryResult.flatMap (_.left.toOption)

    val processingWarningTable = processingWarnings.toTable(tableEnv)
    val deliveryWarningTable = deliveryWarnings.toTable(tableEnv)
    val processingSuccessTable = processingSuccesses.toTable(tableEnv)

    tableEnv.registerTable("processingWarnings", processingWarningTable)
    tableEnv.registerTable("deliveryWarnings", deliveryWarningTable)
    tableEnv.registerTable("processingSuccesses", processingSuccessTable)

    // calculate the processing warnings per hour
    val processingWarningsPerHour = tableEnv.sql(
      """SELECT STREAM
        |TUMBLE_START(timestamp, INTERVAL ‘1’ HOUR) AS hour,
        |COUNT(*) AS cnt
        |FROM processingWarnings
        |GROUP BY TUMBLE(timestamp, INTERVAL ‘1’ HOUR)""".stripMargin)

    // calculate the delivery warnings per hour
    val deliveryWarningsPerHour = tableEnv.sql(
      """SELECT STREAM
        |TUMBLE_START(timestamp, INTERVAL ‘1’ HOUR) AS hour,
        |COUNT(*) AS cnt
        |FROM deliveryWarnings
        |GROUP BY TUMBLE(timestamp, INTERVAL ‘1’ HOUR)""".stripMargin)

    // calculate the average processing time
    val averageProcessingTime = tableEnv.sql(
      """SELECT STREAM
        |TUMBLE_START(timestamp, INTERVAL '1' DAY) AS day,
        |AVG(duration) AS avgDuration
        |FROM processingSuccesses
        |GROUP BY TUMBLE(timestamp, INTERVAL '1' DAY)
      """.stripMargin
    )

    processingWarningsPerHour.toDataStream[Row].print();
    deliveryWarningsPerHour.toDataStream[Row].print();
    averageProcessingTime.toDataStream[Row].print();

    env.execute("Flink Streaming Scala API Skeleton")
  }
}


abstract class Event(var orderId: Long, var timestamp: Long, var status: String) {
  def this() {
    this(-1L, -1L, "Undefined")
  }
}

class Order(orderId: Long, timestamp: Long) extends Event(orderId, timestamp, "Received") {
  def this() {
    this(-1, -1)
  }
}

object Order {
  def apply(orderId: Long, timestamp: Long) = new Order(orderId, timestamp)
}

class Shipment(orderId: Long, timestamp: Long) extends Event(orderId, timestamp, "Shipped") {
  def this() {
    this(-1, -1)
  }
}

object Shipment {
  def apply(orderId: Long, timestamp: Long) = new Shipment(orderId, timestamp)
}

class Delivery(orderId: Long, timestamp: Long) extends Event(orderId, timestamp, "Delivered")

object Delivery {
  def apply(orderId: Long, timestamp: Long) = new Delivery(orderId, timestamp)
}

case class ProcessingSuccess(orderId: Long, timestamp: Long, duration: Long)

case class ProcessingWarning(orderId: Long, timestamp: Long)

case class DeliverySuccess(orderId: Long, timestamp: Long, duration: Long)

case class DeliveryWarning(orderId: Long, timestamp: Long)



