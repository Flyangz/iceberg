/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.utils

import org.apache.iceberg.SpaceFillingCurve
import org.apache.iceberg.ZOrderCurve
import org.apache.iceberg.spark.ordering.LUTZValueCalculator
import org.apache.iceberg.util.PropertyUtil
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.PartitionPruningRDD
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.sql.{Date, Timestamp}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.hashing.byteswap32

object ZOrderUtils {

  def genZValue(ds: Dataset[Row], curve: SpaceFillingCurve): Dataset[Row] = {
    curve match {
      case curve: ZOrderCurve =>
        if (ZOrderCurve.Strategy.SAMPLE == curve.getStrategy) {
          ZOrderUtils.genZValueBySampleRank(ds, curve)
        } else {
          ZOrderUtils.genZValueByDataValue(ds, curve)
        }
      case _: SpaceFillingCurve =>
        throw new IllegalArgumentException("ZOrderUtils only accepts ZOrderCurve but found " + curve.name())
    }
  }

  def genZValueBySampleRank(ds: Dataset[Row], curve: ZOrderCurve): Dataset[Row] = {
    val outputPartitionNum = ds.sqlContext.conf.getConf(SQLConf.SHUFFLE_PARTITIONS)
    val rankSize = PropertyUtil.propertyAsInt(
      curve.properties(), ZOrderCurve.RANK_SIZE, outputPartitionNum)

    if (outputPartitionNum <= 1) {
      return ds.withColumn(ZOrderCurve.Z_VALUE, col(curve.fieldsNames().get(0)))
    }

    import scala.collection.JavaConverters._
    val columns = curve.fieldsNames().asScala.map(col)

    val zFieldsDS = ds.select(columns: _*)
    val fields = curve.schema().columns().asScala

    val zValueCalculator = new LUTZValueCalculator(columns.size, rankSize + 1)
    val typeIds = columns.map(c => fields.find(f => f.name().equals(c.toString())).get.`type`().typeId()).toArray
    val rangeBounds = getRangeBounds(rankSize, zFieldsDS)
    val ranker = new Ranker

    val ranks = new Array[Int](columns.size)

    def getZValue = udf((row: Row) => {
      for (i <- ranks.indices) {
        ranks(i) = ranker.getRank(typeIds(i), rangeBounds(i), row.get(i))
      }
      zValueCalculator.getZValue(ranks)
    })

    ds.withColumn(ZOrderCurve.Z_VALUE, getZValue(functions.struct(columns: _*)))
  }

  def genZValueByDataValue(ds: Dataset[Row], curve: ZOrderCurve): Dataset[Row] = {
    throw new UnsupportedOperationException
  }

  def getRangeBounds(
      rankSize: Int,
      ds: Dataset[Row],
      samplePerPartitionHint: Double = 20.0): IndexedSeq[(AnyRef, Int)] = {
    if (rankSize <= 1) return IndexedSeq.empty[(AnyRef, Int)]

    val rdd = ds.rdd
    // This is the sample size we need to have roughly balanced output partitions, capped at 1M.
    // Cast to double to avoid overflowing ints or longs
    val sampleSize = math.min(samplePerPartitionHint.toDouble * rankSize, 1e6)
    // Assume the input partitions are roughly balanced and over-sample a little bit.
    val sampleSizePerPartition = math.ceil(3.0 * sampleSize / rdd.getNumPartitions).toInt
    val (numItems, sketched) = RangePartitioner.sketch(rdd, sampleSizePerPartition)
    if (numItems == 0L) {
      IndexedSeq.empty[(AnyRef, Int)]
    } else {
      // If a partition contains much more than the average number of items, we re-sample from it
      // to ensure that enough items are collected from that partition.
      val fraction = math.min(sampleSize / math.max(numItems, 1L), 1.0)
      val candidates = ArrayBuffer.empty[(Row, Float)]
      val imbalancedPartitions = mutable.Set.empty[Int]
      sketched.foreach { case (idx, n, sample) =>
        if (fraction * n > sampleSizePerPartition) {
          imbalancedPartitions += idx
        } else {
          // The weight is 1 over the sampling probability.
          val weight = (n.toDouble / sample.length).toFloat
          for (key <- sample) {
            candidates += ((key, weight))
          }
        }
      }
      if (imbalancedPartitions.nonEmpty) {
        // Re-sample imbalanced partitions with the desired sampling probability.
        val imbalanced = new PartitionPruningRDD(rdd, imbalancedPartitions.contains)
        val seed = byteswap32(-rdd.id - 1)
        val reSampled = imbalanced.sample(withReplacement = false, fraction, seed).collect()
        val weight = (1.0 / fraction).toFloat
        candidates ++= reSampled.map(x => (x, weight))
      }

      val fields = ds.schema.fields
      fields.indices.map { i =>
        val colCandidates = candidates.map(e => (e._1(i), e._2))
        // TODO: min(rankSize, outputPartitionNum)
        val bound = (fields(i).dataType match {
          case StringType => RangePartitioner.determineBounds(
            colCandidates.asInstanceOf[ArrayBuffer[(String, Float)]], rankSize)
          case IntegerType => RangePartitioner.determineBounds(
            colCandidates.asInstanceOf[ArrayBuffer[(Int, Float)]], rankSize)
          case LongType => RangePartitioner.determineBounds(
            colCandidates.asInstanceOf[ArrayBuffer[(Long, Float)]], rankSize)
          case FloatType => RangePartitioner.determineBounds(
            colCandidates.asInstanceOf[ArrayBuffer[(Float, Float)]], rankSize)
          case DoubleType => RangePartitioner.determineBounds(
            colCandidates.asInstanceOf[ArrayBuffer[(Double, Float)]], rankSize)
          case ByteType => RangePartitioner.determineBounds(
            colCandidates.asInstanceOf[ArrayBuffer[(Byte, Float)]], rankSize)
          case DateType =>
            implicit def ordered: Ordering[Date] =
              (x: Date, y: Date) => x compareTo y

            RangePartitioner.determineBounds(
              colCandidates.asInstanceOf[ArrayBuffer[(Date, Float)]], rankSize)
          case TimestampType =>
            implicit def ordered: Ordering[Timestamp] =
              (x: Timestamp, y: Timestamp) => x compareTo y

            RangePartitioner.determineBounds(
              colCandidates.asInstanceOf[ArrayBuffer[(Timestamp, Float)]], rankSize)
          case _: DecimalType =>
            implicit def ordered: Ordering[java.math.BigDecimal] =
              (x: java.math.BigDecimal, y: java.math.BigDecimal) => x compareTo y

            RangePartitioner.determineBounds(
              colCandidates.asInstanceOf[ArrayBuffer[(java.math.BigDecimal, Float)]], rankSize)
        })
        (bound.asInstanceOf[AnyRef], bound.length)
      }
    }
  }
}
