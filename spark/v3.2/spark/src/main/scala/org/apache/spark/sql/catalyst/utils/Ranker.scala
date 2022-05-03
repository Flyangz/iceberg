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

import java.util
import org.apache.iceberg.types.Type.TypeID

class Ranker extends Serializable {

  private val navieSearches: Map[TypeID, (AnyRef, Any) => Int] = {
    Map(
      TypeID.INTEGER -> ((b: AnyRef, v: Any) => {
        var rank = 0
        val bound = b.asInstanceOf[Array[Int]]
        val value = v.asInstanceOf[Int]
        while (rank < bound.length && bound(rank) < value) {
          rank += 1
        }
        rank
      }),
      TypeID.LONG -> ((b: AnyRef, v: Any) => {
        var rank = 0
        val bound = b.asInstanceOf[Array[Long]]
        val value = v.asInstanceOf[Long]
        while (rank < bound.length && bound(rank) < value) {
          rank += 1
        }
        rank
      }),
      TypeID.FLOAT -> ((b: AnyRef, v: Any) => {
        var rank = 0
        val bound = b.asInstanceOf[Array[Float]]
        val value = v.asInstanceOf[Float]
        while (rank < bound.length && bound(rank) < value) {
          rank += 1
        }
        rank
      }),
      TypeID.DOUBLE -> ((b: AnyRef, v: Any) => {
        var rank = 0
        val bound = b.asInstanceOf[Array[Double]]
        val value = v.asInstanceOf[Double]
        while (rank < bound.length && bound(rank) < value) {
          rank += 1
        }
        rank
      }),
      TypeID.BINARY -> ((b: AnyRef, v: Any) => {
        var rank = 0
        val bound = b.asInstanceOf[Array[Byte]]
        val value = v.asInstanceOf[Byte]
        while (rank < bound.length && bound(rank) < value) {
          rank += 1
        }
        rank
      })
    )
  }

  private val binarySearches: Map[TypeID, (AnyRef, Any) => Int] = {
    Map(
      TypeID.INTEGER -> ((bounds: AnyRef, v: Any) =>
        util.Arrays.binarySearch(bounds.asInstanceOf[Array[Int]], v.asInstanceOf[Int])),
      TypeID.LONG -> ((bounds: AnyRef, v: Any) =>
        util.Arrays.binarySearch(bounds.asInstanceOf[Array[Long]], v.asInstanceOf[Long])),
      TypeID.FLOAT -> ((bounds: AnyRef, v: Any) =>
        util.Arrays.binarySearch(bounds.asInstanceOf[Array[Float]], v.asInstanceOf[Float])),
      TypeID.DOUBLE -> ((bounds: AnyRef, v: Any) =>
        util.Arrays.binarySearch(bounds.asInstanceOf[Array[Double]], v.asInstanceOf[Double])),
      TypeID.BINARY -> ((bounds: AnyRef, v: Any) =>
        util.Arrays.binarySearch(bounds.asInstanceOf[Array[Byte]], v.asInstanceOf[Byte])),
    )
  }

  def getRank(typeID: TypeID, rangeBounds: (AnyRef, Int), key: Any): Int = {
    if (key == null) return rangeBounds._2 + 1

    if (rangeBounds._2 <= 128) {
      // If we have less than 128 partitions naive search
      navieSearches.getOrElse(typeID, (b: AnyRef, v: Any) => {
        var rank = 0
        val bound = b.asInstanceOf[Array[Comparable[Object]]]
        val value = v.asInstanceOf[Object]
        while (rank < bound.length && bound(rank).compareTo(value) < 0) {
          rank += 1
        }
        rank
      }).apply(rangeBounds._1, key)
    } else {
      // Determine which binary search method to use only once.
      var rank = binarySearches.getOrElse(typeID, (b: AnyRef, v: Any) =>
        util.Arrays.binarySearch(b.asInstanceOf[Array[Object]], v.asInstanceOf[Object]))
        .apply(rangeBounds._1, key)
      // binarySearch either returns the match location or -[insertion point]-1
      if (rank < 0) {
        rank = -rank - 1
      }
      if (rank > rangeBounds._2) {
        rank = rangeBounds._2
      }
      rank
    }
  }
}
