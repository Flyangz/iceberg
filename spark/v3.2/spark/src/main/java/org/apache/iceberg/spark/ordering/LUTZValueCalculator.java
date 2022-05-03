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

package org.apache.iceberg.spark.ordering;


import java.io.Serializable;

public class LUTZValueCalculator implements Serializable {

    private final long[][] LUTs;
    private final int dim;
    private final int chunkSize;
    private final int chunkShiftStep;
    private final int marker;
    private final int initShiftStep;

    public LUTZValueCalculator(int dim, int rankLimit) {
        int maxBitNum = Integer.SIZE - Integer.numberOfLeadingZeros(rankLimit);
        if (dim * maxBitNum >= Long.SIZE) throw new IllegalArgumentException("Exceed 63 bit limit.");

        this.dim = dim;
        int bitCapOfEachDim = Math.min(31, 63 / dim);
        int preCalculateSize = dim < 8 ? 256 : 2 << (bitCapOfEachDim - 1);
        this.LUTs = calculateLUTs(preCalculateSize);
        this.chunkSize = Integer.SIZE - Integer.numberOfLeadingZeros(preCalculateSize - 1);
        this.chunkShiftStep = chunkSize * dim;
        this.marker = (2 << (chunkSize - 1)) - 1;
        this.initShiftStep = chunkSize < 8 ? 0 : bitCapOfEachDim - bitCapOfEachDim % chunkSize;
    }

    public long getZValue(int[] candidates) {
        long answer = 0;
        for (int shiftStep = initShiftStep; shiftStep >= 0; shiftStep -= chunkSize) {
            answer <<= chunkShiftStep;
            for (int j = 0; j < dim; j++) {
                answer |= LUTs[j][candidates[j] >> shiftStep & marker];
            }
        }

        return answer;
    }

    private long[][] calculateLUTs(int size) {
        long[][] res = new long[dim][size];
        for (int i = 0; i < dim; i++) {
            res[i] = genLongLUT(i, size);
        }

        return res;
    }

    private long[] genLongLUT(int dimIdx, int size) {
        long[] tbl = new long[size];
        int pos = 0;
        tbl[pos++] = 0L;
        int times = 0;

        while (pos < size) {
            long newHigh = 1L << (dimIdx + times * dim);
            tbl[pos++] = newHigh;
            if (pos == size) return tbl;

            int limit = pos - 1;
            for (int i = 1; i < limit; i++) {
                tbl[pos++] = newHigh | tbl[i];
                if (pos == size) return tbl;
            }
            times += 1;
        }

        return tbl;
    }
}
