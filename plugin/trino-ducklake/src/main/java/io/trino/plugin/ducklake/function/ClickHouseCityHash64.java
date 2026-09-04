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
package io.trino.plugin.ducklake.function;

import io.airlift.slice.Slice;

final class ClickHouseCityHash64
{
    private static final long K0 = 0xC3A5C85C97CB3127L;
    private static final long K1 = 0xB492B66FBE98F273L;
    private static final long K2 = 0x9AE16A3B2F90404FL;
    private static final long K3 = 0xC949D7C7509E6557L;
    private static final long HASH_128_TO_64_MULTIPLIER = 0x9DDFEA08EB382D69L;

    private ClickHouseCityHash64() {}

    public static long hash(Slice value)
    {
        int length = value.length();
        if (length <= 16) {
            return hashLength0To16(value, length);
        }
        if (length <= 32) {
            return hashLength17To32(value, length);
        }
        if (length <= 64) {
            return hashLength33To64(value, length);
        }

        long x = fetch64(value, 0);
        long y = fetch64(value, length - 16) ^ K1;
        long z = fetch64(value, length - 56) ^ K0;
        LongPair v = weakHashLength32WithSeeds(value, length - 64, length, y);
        LongPair w = weakHashLength32WithSeeds(value, length - 32, length * K1, K0);
        z += shiftMix(v.high()) * K1;
        x = rotateRight(z + x, 39) * K1;
        y = rotateRight(y, 33) * K1;

        int offset = 0;
        int remaining = (length - 1) & ~63;
        while (remaining > 0) {
            x = rotateRight(x + y + v.low() + fetch64(value, offset + 16), 37) * K1;
            y = rotateRight(y + v.high() + fetch64(value, offset + 48), 42) * K1;
            x ^= w.high();
            y ^= v.low();
            z = rotateRight(z ^ w.low(), 33);
            v = weakHashLength32WithSeeds(value, offset, v.high() * K1, x + w.low());
            w = weakHashLength32WithSeeds(value, offset + 32, z + w.high(), y);
            long previousX = x;
            x = z;
            z = previousX;
            offset += 64;
            remaining -= 64;
        }

        return hash16(
                hash16(v.low(), w.low()) + shiftMix(y) * K1 + z,
                hash16(v.high(), w.high()) + x);
    }

    private static long hashLength0To16(Slice value, int length)
    {
        if (length > 8) {
            long a = fetch64(value, 0);
            long b = fetch64(value, length - 8);
            return hash16(a, rotateRight(b + length, length)) ^ b;
        }
        if (length >= 4) {
            long a = fetch32(value, 0);
            return hash16(length + (a << 3), fetch32(value, length - 4));
        }
        if (length > 0) {
            int a = value.getUnsignedByte(0);
            int b = value.getUnsignedByte(length >>> 1);
            int c = value.getUnsignedByte(length - 1);
            int y = a + (b << 8);
            int z = length + (c << 2);
            return shiftMix((Integer.toUnsignedLong(y) * K2) ^ (Integer.toUnsignedLong(z) * K3)) * K2;
        }
        return K2;
    }

    private static long hashLength17To32(Slice value, int length)
    {
        long a = fetch64(value, 0) * K1;
        long b = fetch64(value, 8);
        long c = fetch64(value, length - 8) * K2;
        long d = fetch64(value, length - 16) * K0;
        return hash16(
                rotateRight(a - b, 43) + rotateRight(c, 30) + d,
                a + rotateRight(b ^ K3, 20) - c + length);
    }

    private static long hashLength33To64(Slice value, int length)
    {
        long z = fetch64(value, 24);
        long a = fetch64(value, 0) + (length + fetch64(value, length - 16)) * K0;
        long b = rotateRight(a + z, 52);
        long c = rotateRight(a, 37);
        a += fetch64(value, 8);
        c += rotateRight(a, 7);
        a += fetch64(value, 16);

        long vLow = a + z;
        long vHigh = b + rotateRight(a, 31) + c;
        a = fetch64(value, 16) + fetch64(value, length - 32);
        z = fetch64(value, length - 8);
        b = rotateRight(a + z, 52);
        c = rotateRight(a, 37);
        a += fetch64(value, length - 24);
        c += rotateRight(a, 7);
        a += fetch64(value, length - 16);

        long wLow = a + z;
        long wHigh = b + rotateRight(a, 31) + c;
        long r = shiftMix((vLow + wHigh) * K2 + (wLow + vHigh) * K0);
        return shiftMix(r * K0 + vHigh) * K2;
    }

    private static LongPair weakHashLength32WithSeeds(Slice value, int offset, long a, long b)
    {
        long w = fetch64(value, offset);
        long x = fetch64(value, offset + 8);
        long y = fetch64(value, offset + 16);
        long z = fetch64(value, offset + 24);
        a += w;
        b = rotateRight(b + a + z, 21);
        long c = a;
        a += x + y;
        b += rotateRight(a, 44);
        return new LongPair(a + z, b + c);
    }

    private static long hash16(long low, long high)
    {
        long a = (low ^ high) * HASH_128_TO_64_MULTIPLIER;
        a ^= a >>> 47;
        long b = (high ^ a) * HASH_128_TO_64_MULTIPLIER;
        b ^= b >>> 47;
        return b * HASH_128_TO_64_MULTIPLIER;
    }

    private static long shiftMix(long value)
    {
        return value ^ (value >>> 47);
    }

    private static long rotateRight(long value, int shift)
    {
        return Long.rotateRight(value, shift);
    }

    private static long fetch32(Slice value, int offset)
    {
        return value.getUnsignedByte(offset)
                | ((long) value.getUnsignedByte(offset + 1) << 8)
                | ((long) value.getUnsignedByte(offset + 2) << 16)
                | ((long) value.getUnsignedByte(offset + 3) << 24);
    }

    private static long fetch64(Slice value, int offset)
    {
        return fetch32(value, offset) | (fetch32(value, offset + 4) << 32);
    }

    private record LongPair(long low, long high) {}
}
