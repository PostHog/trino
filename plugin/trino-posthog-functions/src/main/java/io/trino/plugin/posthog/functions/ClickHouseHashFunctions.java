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
package io.trino.plugin.posthog.functions;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.StandardTypes;

import static java.lang.Long.rotateRight;

/**
 * ClickHouse's {@code cityHash64}, so that HogQL compiled to Trino returns the same values.
 * <p>
 * ClickHouse bundles CityHash v1.0.2, which differs from later Google CityHash releases.
 * The result is ClickHouse's UInt64, returned as DECIMAL(20,0) so that modulo bucketing
 * and string formatting match ClickHouse for values above 2^63.
 */
public final class ClickHouseHashFunctions
{
    private static final long K0 = 0xC3A5C85C97CB3127L;
    private static final long K1 = 0xB492B66FBE98F273L;
    private static final long K2 = 0x9AE16A3B2F90404FL;
    private static final long K3 = 0xC949D7C7509E6557L;
    private static final long K_MUL = 0x9DDFEA08EB382D69L;

    private ClickHouseHashFunctions() {}

    @Description("ClickHouse cityHash64 of the UTF-8 bytes of a string")
    @ScalarFunction("ch_cityhash64")
    @LiteralParameters("x")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64Varchar(@SqlType("varchar(x)") Slice value)
    {
        return toUnsignedDecimal(cityHash64(value));
    }

    @Description("ClickHouse cityHash64 of a binary value")
    @ScalarFunction("ch_cityhash64")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64Varbinary(@SqlType(StandardTypes.VARBINARY) Slice value)
    {
        return toUnsignedDecimal(cityHash64(value));
    }

    // ClickHouse hashes an integer as its unsigned bit pattern at the integer's own width,
    // so Int8 -1 hashes like UInt8 255 and differs from Int64 -1.
    @Description("ClickHouse cityHash64 of an integer, which is ClickHouse's intHash64")
    @ScalarFunction("ch_cityhash64")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64Tinyint(@SqlType(StandardTypes.TINYINT) long value)
    {
        return toUnsignedDecimal(intHash64(value & 0xFFL));
    }

    @Description("ClickHouse cityHash64 of an integer, which is ClickHouse's intHash64")
    @ScalarFunction("ch_cityhash64")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64Smallint(@SqlType(StandardTypes.SMALLINT) long value)
    {
        return toUnsignedDecimal(intHash64(value & 0xFFFFL));
    }

    @Description("ClickHouse cityHash64 of an integer, which is ClickHouse's intHash64")
    @ScalarFunction("ch_cityhash64")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64Integer(@SqlType(StandardTypes.INTEGER) long value)
    {
        return toUnsignedDecimal(intHash64(value & 0xFFFFFFFFL));
    }

    @Description("ClickHouse cityHash64 of an integer, which is ClickHouse's intHash64")
    @ScalarFunction("ch_cityhash64")
    @SqlType("decimal(20,0)")
    public static Int128 cityHash64Bigint(@SqlType(StandardTypes.BIGINT) long value)
    {
        return toUnsignedDecimal(intHash64(value));
    }

    private static Int128 toUnsignedDecimal(long hash)
    {
        return Int128.valueOf(0, hash);
    }

    static long intHash64(long x)
    {
        x ^= 0x4CF2D2BAAE6DA887L;
        x ^= x >>> 33;
        x *= 0xFF51AFD7ED558CCDL;
        x ^= x >>> 33;
        x *= 0xC4CEB9FE1A85EC53L;
        x ^= x >>> 33;
        return x;
    }

    static long cityHash64(Slice s)
    {
        int n = s.length();
        if (n <= 16) {
            return hashLen0to16(s, n);
        }
        if (n <= 32) {
            return hashLen17to32(s, n);
        }
        if (n <= 64) {
            return hashLen33to64(s, n);
        }

        long x = s.getLong(0);
        long y = s.getLong(n - 16) ^ K1;
        long z = s.getLong(n - 56) ^ K0;
        long[] v = new long[2];
        long[] w = new long[2];
        weakHashLen32WithSeeds(s, n - 64, n, y, v);
        weakHashLen32WithSeeds(s, n - 32, n * K1, K0, w);
        z += shiftMix(v[1]) * K1;
        x = rotateRight(z + x, 39) * K1;
        y = rotateRight(y, 33) * K1;

        int offset = 0;
        for (int remaining = (n - 1) & ~63; remaining > 0; remaining -= 64) {
            x = rotateRight(x + y + v[0] + s.getLong(offset + 16), 37) * K1;
            y = rotateRight(y + v[1] + s.getLong(offset + 48), 42) * K1;
            x ^= w[1];
            y ^= v[0];
            z = rotateRight(z ^ w[0], 33);
            weakHashLen32WithSeeds(s, offset, v[1] * K1, x + w[0], v);
            weakHashLen32WithSeeds(s, offset + 32, z + w[1], y, w);
            long swap = z;
            z = x;
            x = swap;
            offset += 64;
        }
        return hashLen16(hashLen16(v[0], w[0]) + shiftMix(y) * K1 + z, hashLen16(v[1], w[1]) + x);
    }

    private static long hashLen0to16(Slice s, int n)
    {
        if (n > 8) {
            long a = s.getLong(0);
            long b = s.getLong(n - 8);
            return hashLen16(a, rotateRight(b + n, n)) ^ b;
        }
        if (n >= 4) {
            long a = Integer.toUnsignedLong(s.getInt(0));
            return hashLen16(n + (a << 3), Integer.toUnsignedLong(s.getInt(n - 4)));
        }
        if (n > 0) {
            long a = Byte.toUnsignedLong(s.getByte(0));
            long b = Byte.toUnsignedLong(s.getByte(n >> 1));
            long c = Byte.toUnsignedLong(s.getByte(n - 1));
            long y = a + (b << 8);
            long z = n + (c << 2);
            return shiftMix(y * K2 ^ z * K3) * K2;
        }
        return K2;
    }

    private static long hashLen17to32(Slice s, int n)
    {
        long a = s.getLong(0) * K1;
        long b = s.getLong(8);
        long c = s.getLong(n - 8) * K2;
        long d = s.getLong(n - 16) * K0;
        return hashLen16(rotateRight(a - b, 43) + rotateRight(c, 30) + d, a + rotateRight(b ^ K3, 20) - c + n);
    }

    private static long hashLen33to64(Slice s, int n)
    {
        long z = s.getLong(24);
        long a = s.getLong(0) + (n + s.getLong(n - 16)) * K0;
        long b = rotateRight(a + z, 52);
        long c = rotateRight(a, 37);
        a += s.getLong(8);
        c += rotateRight(a, 7);
        a += s.getLong(16);
        long vf = a + z;
        long vs = b + rotateRight(a, 31) + c;

        a = s.getLong(16) + s.getLong(n - 32);
        z = s.getLong(n - 8);
        b = rotateRight(a + z, 52);
        c = rotateRight(a, 37);
        a += s.getLong(n - 24);
        c += rotateRight(a, 7);
        a += s.getLong(n - 16);
        long wf = a + z;
        long ws = b + rotateRight(a, 31) + c;

        long r = shiftMix((vf + ws) * K2 + (wf + vs) * K0);
        return shiftMix(r * K0 + vs) * K2;
    }

    private static void weakHashLen32WithSeeds(Slice s, int offset, long a, long b, long[] out)
    {
        long w = s.getLong(offset);
        long x = s.getLong(offset + 8);
        long y = s.getLong(offset + 16);
        long z = s.getLong(offset + 24);
        a += w;
        b = rotateRight(b + a + z, 21);
        long c = a;
        a += x;
        a += y;
        b += rotateRight(a, 44);
        out[0] = a + z;
        out[1] = b + c;
    }

    private static long hashLen16(long u, long v)
    {
        long a = (u ^ v) * K_MUL;
        a ^= a >>> 47;
        long b = (v ^ a) * K_MUL;
        b ^= b >>> 47;
        return b * K_MUL;
    }

    private static long shiftMix(long v)
    {
        return v ^ (v >>> 47);
    }
}
