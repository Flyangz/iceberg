package org.apache.iceberg.spark.ordering;

import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;

public class InterleaveTest {

    @Test
    public void testInterleave() {
        int[][] testSet = {
            {2, 221},
            {31, 3},
            {63, 1}, // max dim
            {2, Integer.MAX_VALUE}, // max rank limit
            {1, Integer.MAX_VALUE}
        };

        for (int[] data : testSet) {
            LUTZValueCalculator calculator = new LUTZValueCalculator(data[0], data[1]);
            int[] input = new int[data[0]];
            Arrays.fill(input, data[1]);
            Assert.assertEquals(checkFunc(input), calculator.getZValue(input));
        }
    }
    @Test
    public void testCheckFunc() {
        Assert.assertEquals(45795, checkFunc(new int[]{73, 221}));
    }

    private static long checkFunc(int[] candidates) {
        int limit = getInputLimit(candidates);

        String[] binaryStrings = new String[candidates.length];
        for (int i = 0; i < candidates.length; i++) {
            binaryStrings[i] = paddingZero(Integer.toBinaryString(candidates[i]), limit);
        }

        int pos = Long.SIZE - 1;
        char[] chars = new char[Long.SIZE];
        for (int i = limit - 1; i >= 0; i--) {
            for (int j = 0; j < candidates.length; j++) {
                chars[pos--] = binaryStrings[j].charAt(i);
            }
        }

        while (pos >= 0) {
            chars[pos--] = '0';
        }

        return Long.parseLong(String.valueOf(chars), 2);
    }

    private static int getInputLimit(int[] candidates) {
        int maxInput = 0;
        for (int candidate : candidates) {
            if (candidate > maxInput) maxInput = candidate;
        }
        int maxBitNum = Integer.SIZE - Integer.numberOfLeadingZeros(maxInput);
        if (candidates.length * maxBitNum > 63) throw new IllegalArgumentException("Exceed 64 bit limit.");
        return maxBitNum;
    }

    private static <T> String toBinaryStringWithZeroPadding(T d) {
        if (d instanceof Integer) {
            return paddingZero(Integer.toBinaryString((int) d), Integer.SIZE);
        } else if (d instanceof Long) {
            return paddingZero(Long.toBinaryString((long) d), Long.SIZE);
        } else {
            throw new UnsupportedOperationException("Unsupported type: " + d.getClass());
        }
    }

    private static String paddingZero(String s, int size) {
        if (s.length() < size) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < size - s.length(); i++) {
                sb.append("0");
            }
            return sb.append(s).toString();
        } else {
            return s;
        }
    }
}
