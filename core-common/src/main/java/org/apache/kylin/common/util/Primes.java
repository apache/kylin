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
package org.apache.kylin.common.util;

import java.math.BigInteger;

/**
 * Copied from commons-math3 to avoid big dependency.
 */
public class Primes {

    /**
     * The first 512 prime numbers.
     * <p>
     * It contains all primes smaller or equal to the cubic square of Integer.MAX_VALUE.
     * As a result, <code>int</code> numbers which are not reduced by those primes are guaranteed
     * to be either prime or semi prime.
     */
    public static final int[] PRIMES = { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127, 131, 137, 139, 149, 151, 157, 163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 223, 227, 229, 233, 239, 241, 251, 257, 263, 269, 271, 277, 281, 283, 293, 307, 311, 313, 317, 331, 337, 347, 349, 353, 359, 367, 373, 379, 383, 389, 397, 401, 409, 419, 421, 431, 433, 439, 443, 449, 457, 461, 463, 467, 479, 487, 491, 499, 503, 509, 521, 523, 541, 547, 557, 563, 569, 571, 577, 587, 593, 599, 601, 607, 613, 617, 619, 631, 641, 643, 647, 653, 659, 661, 673, 677, 683, 691, 701, 709, 719, 727, 733, 739, 743, 751, 757, 761, 769, 773, 787, 797, 809, 811, 821, 823, 827, 829, 839, 853, 857, 859, 863, 877, 881, 883, 887, 907, 911, 919, 929, 937, 941, 947, 953, 967, 971, 977, 983, 991, 997, 1009, 1013, 1019, 1021, 1031, 1033, 1039, 1049, 1051, 1061, 1063, 1069, 1087, 1091, 1093, 1097, 1103, 1109, 1117, 1123, 1129, 1151, 1153, 1163,
            1171, 1181, 1187, 1193, 1201, 1213, 1217, 1223, 1229, 1231, 1237, 1249, 1259, 1277, 1279, 1283, 1289, 1291, 1297, 1301, 1303, 1307, 1319, 1321, 1327, 1361, 1367, 1373, 1381, 1399, 1409, 1423, 1427, 1429, 1433, 1439, 1447, 1451, 1453, 1459, 1471, 1481, 1483, 1487, 1489, 1493, 1499, 1511, 1523, 1531, 1543, 1549, 1553, 1559, 1567, 1571, 1579, 1583, 1597, 1601, 1607, 1609, 1613, 1619, 1621, 1627, 1637, 1657, 1663, 1667, 1669, 1693, 1697, 1699, 1709, 1721, 1723, 1733, 1741, 1747, 1753, 1759, 1777, 1783, 1787, 1789, 1801, 1811, 1823, 1831, 1847, 1861, 1867, 1871, 1873, 1877, 1879, 1889, 1901, 1907, 1913, 1931, 1933, 1949, 1951, 1973, 1979, 1987, 1993, 1997, 1999, 2003, 2011, 2017, 2027, 2029, 2039, 2053, 2063, 2069, 2081, 2083, 2087, 2089, 2099, 2111, 2113, 2129, 2131, 2137, 2141, 2143, 2153, 2161, 2179, 2203, 2207, 2213, 2221, 2237, 2239, 2243, 2251, 2267, 2269, 2273, 2281, 2287, 2293, 2297, 2309, 2311, 2333, 2339, 2341, 2347, 2351, 2357, 2371, 2377, 2381, 2383, 2389, 2393,
            2399, 2411, 2417, 2423, 2437, 2441, 2447, 2459, 2467, 2473, 2477, 2503, 2521, 2531, 2539, 2543, 2549, 2551, 2557, 2579, 2591, 2593, 2609, 2617, 2621, 2633, 2647, 2657, 2659, 2663, 2671, 2677, 2683, 2687, 2689, 2693, 2699, 2707, 2711, 2713, 2719, 2729, 2731, 2741, 2749, 2753, 2767, 2777, 2789, 2791, 2797, 2801, 2803, 2819, 2833, 2837, 2843, 2851, 2857, 2861, 2879, 2887, 2897, 2903, 2909, 2917, 2927, 2939, 2953, 2957, 2963, 2969, 2971, 2999, 3001, 3011, 3019, 3023, 3037, 3041, 3049, 3061, 3067, 3079, 3083, 3089, 3109, 3119, 3121, 3137, 3163, 3167, 3169, 3181, 3187, 3191, 3203, 3209, 3217, 3221, 3229, 3251, 3253, 3257, 3259, 3271, 3299, 3301, 3307, 3313, 3319, 3323, 3329, 3331, 3343, 3347, 3359, 3361, 3371, 3373, 3389, 3391, 3407, 3413, 3433, 3449, 3457, 3461, 3463, 3467, 3469, 3491, 3499, 3511, 3517, 3527, 3529, 3533, 3539, 3541, 3547, 3557, 3559, 3571, 3581, 3583, 3593, 3607, 3613, 3617, 3623, 3631, 3637, 3643, 3659, 3671 };

    /**
     * Hide utility class.
     */
    private Primes() {
    }

    /**
     * Primality test: tells if the argument is a (provable) prime or not.
     * <p>
     * It uses the Miller-Rabin probabilistic test in such a way that a result is guaranteed:
     * it uses the firsts prime numbers as successive base (see Handbook of applied cryptography
     * by Menezes, table 4.1).
     *
     * @param n number to test.
     * @return true if n is prime. (All numbers &lt; 2 return false).
     */
    public static boolean isPrime(int n) {
        if (n < 2) {
            return false;
        }

        for (int p : PRIMES) {
            if (0 == (n % p)) {
                return n == p;
            }
        }
        return millerRabinPrimeTest(n);
    }

    /**
     * Return the smallest prime greater than or equal to n.
     *
     * @param n a positive number.
     * @return the smallest prime greater than or equal to n.
     * @throws IllegalArgumentException if n &lt; 0.
     */
    public static int nextPrime(int n) {
        if (n < 0) {
            throw new IllegalArgumentException();
        }
        if (n == 2) {
            return 2;
        }
        n |= 1;//make sure n is odd
        if (n == 1) {
            return 2;
        }

        if (isPrime(n)) {
            return n;
        }

        // prepare entry in the +2, +4 loop:
        // n should not be a multiple of 3
        final int rem = n % 3;
        if (0 == rem) { // if n % 3 == 0
            n += 2; // n % 3 == 2
        } else if (1 == rem) { // if n % 3 == 1
            // if (isPrime(n)) return n;
            n += 4; // n % 3 == 2
        }
        while (true) { // this loop skips all multiple of 3
            if (isPrime(n)) {
                return n;
            }
            n += 2; // n % 3 == 1
            if (isPrime(n)) {
                return n;
            }
            n += 4; // n % 3 == 2
        }
    }

    /**
     * Miller-Rabin probabilistic primality test for int type, used in such a way that a result is always guaranteed.
     * <p>
     * It uses the prime numbers as successive base therefore it is guaranteed to be always correct.
     * (see Handbook of applied cryptography by Menezes, table 4.1)
     *
     * @param n number to test: an odd integer &ge; 3
     * @return true if n is prime. false if n is definitely composite.
     */
    public static boolean millerRabinPrimeTest(final int n) {
        final int nMinus1 = n - 1;
        final int s = Integer.numberOfTrailingZeros(nMinus1);
        final int r = nMinus1 >> s;
        //r must be odd, it is not checked here
        int t = 1;
        if (n >= 2047) {
            t = 2;
        }
        if (n >= 1373653) {
            t = 3;
        }
        if (n >= 25326001) {
            t = 4;
        } // works up to 3.2 billion, int range stops at 2.7 so we are safe :-)
        BigInteger br = BigInteger.valueOf(r);
        BigInteger bn = BigInteger.valueOf(n);

        for (int i = 0; i < t; i++) {
            BigInteger a = BigInteger.valueOf(PRIMES[i]);
            BigInteger bPow = a.modPow(br, bn);
            int y = bPow.intValue();
            if ((1 != y) && (y != nMinus1)) {
                int j = 1;
                while ((j <= s - 1) && (nMinus1 != y)) {
                    long square = ((long) y) * y;
                    y = (int) (square % n);
                    if (1 == y) {
                        return false;
                    } // definitely composite
                    j++;
                }
                if (nMinus1 != y) {
                    return false;
                } // definitely composite
            }
        }
        return true; // definitely prime
    }
}
