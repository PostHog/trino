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
package io.trino.plugin.hogql;

import com.google.common.collect.ImmutableList;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3CoreV3;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.SqlRow;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.RowType;
import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.special.Gamma;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.IPADDRESS;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Math.toIntExact;

public final class HogQLScalarFunctions
{
    private static final H3CoreV3 H3 = loadH3();
    private static final int MAX_ARRAY_SIZE = 1_000_000;
    private static final double DEGREES_PER_METER = 8.99320592271288084e-6;
    private static final RowType DOUBLE_PAIR = RowType.anonymous(ImmutableList.of(
            io.trino.spi.type.DoubleType.DOUBLE,
            io.trino.spi.type.DoubleType.DOUBLE));
    private static final RowType BIGINT_PAIR = RowType.anonymous(ImmutableList.of(
            io.trino.spi.type.BigintType.BIGINT,
            io.trino.spi.type.BigintType.BIGINT));
    private static final RowType IPADDRESS_PAIR = RowType.anonymous(ImmutableList.of(
            io.trino.spi.type.VarcharType.VARCHAR,
            io.trino.spi.type.VarcharType.VARCHAR));
    private static final int ASIN_SQRT_LUT_SIZE = 512;
    private static final int COS_LUT_SIZE = 1024;
    private static final int METRIC_LUT_SIZE = 1024;
    private static final double EARTH_RADIUS = 6371007.180918475;
    private static final double EARTH_DIAMETER = 2 * EARTH_RADIUS;
    private static final double[] COS_LUT = new double[COS_LUT_SIZE + 1];
    private static final double[] ASIN_SQRT_LUT = new double[ASIN_SQRT_LUT_SIZE + 1];
    private static final double[] SPHERE_METRIC_LUT = new double[METRIC_LUT_SIZE + 1];
    private static final double[] SPHERE_METERS_LUT = new double[METRIC_LUT_SIZE + 1];
    private static final double[] WGS84_METERS_LUT = new double[2 * (METRIC_LUT_SIZE + 1)];
    private static final String GEOHASH_ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz";
    private static final String BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    static {
        for (int index = 0; index <= COS_LUT_SIZE; index++) {
            COS_LUT[index] = Math.cos(2 * Math.PI * index / COS_LUT_SIZE);
        }
        for (int index = 0; index <= ASIN_SQRT_LUT_SIZE; index++) {
            ASIN_SQRT_LUT[index] = Math.asin(Math.sqrt((double) index / ASIN_SQRT_LUT_SIZE));
        }
        for (int index = 0; index <= METRIC_LUT_SIZE; index++) {
            double latitude = index * Math.PI / METRIC_LUT_SIZE - Math.PI * 0.5;
            WGS84_METERS_LUT[index * 2] = square(111132.09 - 566.05 * Math.cos(2 * latitude) + 1.20 * Math.cos(4 * latitude));
            WGS84_METERS_LUT[index * 2 + 1] = square(111415.13 * Math.cos(latitude) - 94.55 * Math.cos(3 * latitude) + 0.12 * Math.cos(5 * latitude));
            SPHERE_METERS_LUT[index] = square((EARTH_DIAMETER * Math.PI / 360) * Math.cos(latitude));
            SPHERE_METRIC_LUT[index] = square(Math.cos(latitude));
        }
    }

    private HogQLScalarFunctions() {}

    private static H3CoreV3 loadH3()
    {
        try {
            return H3CoreV3.newInstance();
        }
        catch (IOException exception) {
            throw new ExceptionInInitializerError(exception);
        }
    }

    private static void validateCoordinates(double longitude, double latitude)
    {
        if (!Double.isFinite(longitude) || !Double.isFinite(latitude) || Math.abs(longitude) > 180 || Math.abs(latitude) > 90) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Coordinates must be finite and within geographic bounds");
        }
    }

    private static void checkArraySize(long size)
    {
        if (size < 0 || size > MAX_ARRAY_SIZE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "HogQL array result exceeds the item limit");
        }
    }

    private static void checkRingSize(long distance)
    {
        if (distance < 0 || distance > MAX_ARRAY_SIZE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid H3 ring distance");
        }
        checkArraySize(1 + 3 * distance * (distance + 1));
    }

    private static long bool(boolean value)
    {
        return value ? 1 : 0;
    }

    private static Block longArray(Collection<? extends Number> values)
    {
        BlockBuilder builder = io.trino.spi.type.BigintType.BIGINT.createFixedSizeBlockBuilder(values.size());
        for (Number value : values) {
            io.trino.spi.type.BigintType.BIGINT.writeLong(builder, value.longValue());
        }
        return builder.build();
    }

    private static SqlRow longPair(List<Long> values)
    {
        return io.trino.spi.block.RowValueBuilder.buildRowValue(BIGINT_PAIR, fields -> {
            io.trino.spi.type.BigintType.BIGINT.writeLong(fields.get(0), values.get(0));
            io.trino.spi.type.BigintType.BIGINT.writeLong(fields.get(1), values.get(1));
        });
    }

    private static Block coordinateArray(List<LatLng> coordinates)
    {
        BlockBuilder latitude = io.trino.spi.type.DoubleType.DOUBLE.createFixedSizeBlockBuilder(coordinates.size());
        BlockBuilder longitude = io.trino.spi.type.DoubleType.DOUBLE.createFixedSizeBlockBuilder(coordinates.size());
        for (LatLng coordinate : coordinates) {
            io.trino.spi.type.DoubleType.DOUBLE.writeDouble(latitude, coordinate.lat);
            io.trino.spi.type.DoubleType.DOUBLE.writeDouble(longitude, coordinate.lng);
        }
        return RowBlock.fromFieldBlocks(coordinates.size(), new Block[] {latitude.build(), longitude.build()});
    }

    private static double square(double value)
    {
        return value * value;
    }

    private static double degreeDifference(double value)
    {
        value = Math.abs(value);
        return value > 180 ? 360 - value : value;
    }

    private static double fastCos(double value)
    {
        double position = Math.abs(value) * COS_LUT_SIZE / Math.PI / 2;
        int index = (int) position;
        position -= index;
        index &= COS_LUT_SIZE - 1;
        return COS_LUT[index] + (COS_LUT[index + 1] - COS_LUT[index]) * position;
    }

    private static double fastSin(double value)
    {
        double position = Math.abs(value) * COS_LUT_SIZE / Math.PI / 2;
        int index = (int) position;
        position -= index;
        index = (index - COS_LUT_SIZE / 4) & (COS_LUT_SIZE - 1);
        return COS_LUT[index] + (COS_LUT[index + 1] - COS_LUT[index]) * position;
    }

    private static double fastAsinSqrt(double value)
    {
        if (value < 0.122) {
            double root = Math.sqrt(value);
            return root + value * root * 0.166666666666666 + value * value * root * 0.075 + value * value * value * root * 0.044642857142857;
        }
        if (value < 0.948) {
            double position = value * ASIN_SQRT_LUT_SIZE;
            int index = (int) position;
            return ASIN_SQRT_LUT[index] + (ASIN_SQRT_LUT[index + 1] - ASIN_SQRT_LUT[index]) * (position - index);
        }
        return Math.asin(Math.sqrt(value));
    }

    private static double geographicDistance(
            double longitude1,
            double latitude1,
            double longitude2,
            double latitude2,
            int method)
    {
        validateCoordinates(longitude1, latitude1);
        validateCoordinates(longitude2, latitude2);
        double latitudeDifference = degreeDifference(latitude1 - latitude2);
        double longitudeDifference = degreeDifference(longitude1 - longitude2);
        if (longitudeDifference < 13) {
            double latitudeMidpoint = (latitude1 + latitude2 + 180) * METRIC_LUT_SIZE / 360;
            int index = ((int) latitudeMidpoint) & (METRIC_LUT_SIZE - 1);
            double fraction = latitudeMidpoint - (int) latitudeMidpoint;
            double latitudeScale;
            double longitudeScale;
            if (method == 0) {
                latitudeScale = 1;
                longitudeScale = SPHERE_METRIC_LUT[index]
                        + (SPHERE_METRIC_LUT[index + 1] - SPHERE_METRIC_LUT[index]) * fraction;
            }
            else if (method == 1) {
                latitudeScale = square(EARTH_DIAMETER * Math.PI / 360);
                longitudeScale = SPHERE_METERS_LUT[index]
                        + (SPHERE_METERS_LUT[index + 1] - SPHERE_METERS_LUT[index]) * fraction;
            }
            else {
                latitudeScale = WGS84_METERS_LUT[index * 2]
                        + (WGS84_METERS_LUT[(index + 1) * 2] - WGS84_METERS_LUT[index * 2]) * fraction;
                longitudeScale = WGS84_METERS_LUT[index * 2 + 1]
                        + (WGS84_METERS_LUT[(index + 1) * 2 + 1] - WGS84_METERS_LUT[index * 2 + 1]) * fraction;
            }
            return Math.sqrt(latitudeScale * square(latitudeDifference) + longitudeScale * square(longitudeDifference));
        }
        double radiansPerDegree = Math.PI / 180;
        double halfRadiansPerDegree = Math.PI / 360;
        double haversine = square(fastSin(latitudeDifference * halfRadiansPerDegree))
                + fastCos(latitude1 * radiansPerDegree)
                * fastCos(latitude2 * radiansPerDegree)
                * square(fastSin(longitudeDifference * halfRadiansPerDegree));
        if (method == 0) {
            return 360 / Math.PI * fastAsinSqrt(haversine);
        }
        return EARTH_DIAMETER * fastAsinSqrt(haversine);
    }

    private static int geohashPrecision(long precision)
    {
        return precision < 1 || precision > 12 ? 12 : toIntExact(precision);
    }

    private static String geohashEncodeValue(double longitude, double latitude, int precision)
    {
        double longitudeMin = -180;
        double longitudeMax = 180;
        double latitudeMin = -90;
        double latitudeMax = 90;
        StringBuilder result = new StringBuilder(precision);
        int character = 0;
        int bit = 0;
        boolean longitudeBit = true;
        while (result.length() < precision) {
            double midpoint;
            if (longitudeBit) {
                midpoint = (longitudeMin + longitudeMax) / 2;
                if (longitude >= midpoint) {
                    character |= 1 << (4 - bit);
                    longitudeMin = midpoint;
                }
                else {
                    longitudeMax = midpoint;
                }
            }
            else {
                midpoint = (latitudeMin + latitudeMax) / 2;
                if (latitude >= midpoint) {
                    character |= 1 << (4 - bit);
                    latitudeMin = midpoint;
                }
                else {
                    latitudeMax = midpoint;
                }
            }
            longitudeBit = !longitudeBit;
            if (++bit == 5) {
                result.append(GEOHASH_ALPHABET.charAt(character));
                character = 0;
                bit = 0;
            }
        }
        return result.toString();
    }

    private static SqlRow geohashDecodeValue(String encoded)
    {
        int precision = Math.min(encoded.length(), 12);
        if (precision == 0) {
            return doublePair(0, 0);
        }
        double longitudeMin = -180;
        double longitudeMax = 180;
        double latitudeMin = -90;
        double latitudeMax = 90;
        boolean longitudeBit = true;
        for (int index = 0; index < precision; index++) {
            int value = GEOHASH_ALPHABET.indexOf(encoded.charAt(index)) & 31;
            for (int mask = 16; mask != 0; mask >>= 1) {
                if (longitudeBit) {
                    double midpoint = (longitudeMin + longitudeMax) / 2;
                    if ((value & mask) != 0) {
                        longitudeMin = midpoint;
                    }
                    else {
                        longitudeMax = midpoint;
                    }
                }
                else {
                    double midpoint = (latitudeMin + latitudeMax) / 2;
                    if ((value & mask) != 0) {
                        latitudeMin = midpoint;
                    }
                    else {
                        latitudeMax = midpoint;
                    }
                }
                longitudeBit = !longitudeBit;
            }
        }
        return doublePair((longitudeMin + longitudeMax) / 2, (latitudeMin + latitudeMax) / 2);
    }

    private static SqlRow doublePair(double first, double second)
    {
        return io.trino.spi.block.RowValueBuilder.buildRowValue(DOUBLE_PAIR, fields -> {
            io.trino.spi.type.DoubleType.DOUBLE.writeDouble(fields.get(0), first);
            io.trino.spi.type.DoubleType.DOUBLE.writeDouble(fields.get(1), second);
        });
    }

    static Set<Long> decodeBitmap(Slice bitmap)
    {
        if (bitmap.length() % Long.BYTES != 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "A bitmap must contain complete 64-bit values");
        }
        Set<Long> values = new TreeSet<>(Long::compareUnsigned);
        for (int offset = 0; offset < bitmap.length(); offset += Long.BYTES) {
            values.add(bitmap.getLong(offset));
        }
        return values;
    }

    static Slice encodeBitmap(Collection<Long> values)
    {
        Set<Long> sorted = new TreeSet<>(Long::compareUnsigned);
        sorted.addAll(values);
        Slice bitmap = Slices.allocate(Math.multiplyExact(sorted.size(), Long.BYTES));
        int offset = 0;
        for (long value : sorted) {
            bitmap.setLong(offset, value);
            offset += Long.BYTES;
        }
        return bitmap;
    }

    private static Set<Long> bitmapFromArray(Block values)
    {
        Set<Long> bitmap = new LinkedHashSet<>();
        for (int position = 0; position < values.getPositionCount(); position++) {
            if (!values.isNull(position)) {
                bitmap.add(io.trino.spi.type.BigintType.BIGINT.getLong(values, position));
            }
        }
        return bitmap;
    }

    private static Block bitmapToBlock(Collection<Long> values)
    {
        return longArray(values);
    }

    private static Slice base58DecodeValue(Slice encoded)
    {
        String text = encoded.toStringUtf8();
        BigInteger value = BigInteger.ZERO;
        for (int index = 0; index < text.length(); index++) {
            int digit = BASE58_ALPHABET.indexOf(text.charAt(index));
            if (digit < 0) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid Base58 character");
            }
            value = value.multiply(BigInteger.valueOf(58)).add(BigInteger.valueOf(digit));
        }
        byte[] decoded = value.signum() == 0 ? new byte[0] : value.toByteArray();
        int sourceOffset = decoded.length > 0 && decoded[0] == 0 ? 1 : 0;
        int leadingZeros = 0;
        while (leadingZeros < text.length() && text.charAt(leadingZeros) == '1') {
            leadingZeros++;
        }
        byte[] result = new byte[leadingZeros + decoded.length - sourceOffset];
        System.arraycopy(decoded, sourceOffset, result, leadingZeros, decoded.length - sourceOffset);
        return Slices.wrappedBuffer(result);
    }

    private static Slice parseIpAddress(Slice input)
    {
        String text = input.toStringUtf8();
        try {
            byte[] address = InetAddress.ofLiteral(text).getAddress();
            if (address.length == 16) {
                return Slices.wrappedBuffer(address);
            }
            byte[] mapped = new byte[16];
            mapped[10] = (byte) 0xFF;
            mapped[11] = (byte) 0xFF;
            System.arraycopy(address, 0, mapped, 12, 4);
            return Slices.wrappedBuffer(mapped);
        }
        catch (IllegalArgumentException exception) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid IP address", exception);
        }
    }

    private static String formatIpAddress(Slice address)
    {
        if (address.length() != 16) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "An IPv6 binary value must contain 16 bytes");
        }
        boolean mapped = true;
        for (int index = 0; index < 10; index++) {
            mapped &= address.getByte(index) == 0;
        }
        mapped &= (address.getByte(10) & 0xFF) == 0xFF && (address.getByte(11) & 0xFF) == 0xFF;
        if (mapped) {
            return "::ffff:" + (address.getByte(12) & 0xFF) + "." + (address.getByte(13) & 0xFF) + "."
                    + (address.getByte(14) & 0xFF) + "." + (address.getByte(15) & 0xFF);
        }
        int[] groups = new int[8];
        for (int index = 0; index < groups.length; index++) {
            groups[index] = ((address.getByte(index * 2) & 0xFF) << 8) | (address.getByte(index * 2 + 1) & 0xFF);
        }
        int bestStart = -1;
        int bestLength = 0;
        for (int start = 0; start < groups.length; ) {
            if (groups[start] != 0) {
                start++;
                continue;
            }
            int end = start;
            while (end < groups.length && groups[end] == 0) {
                end++;
            }
            if (end - start > bestLength && end - start >= 2) {
                bestStart = start;
                bestLength = end - start;
            }
            start = end;
        }
        StringBuilder result = new StringBuilder();
        for (int index = 0; index < groups.length; index++) {
            if (index == bestStart) {
                result.append("::");
                index += bestLength - 1;
                continue;
            }
            if (!result.isEmpty() && result.charAt(result.length() - 1) != ':') {
                result.append(':');
            }
            result.append(Integer.toHexString(groups[index]));
        }
        return result.toString();
    }

    private static SqlRow ipAddressPair(Slice first, Slice second)
    {
        return io.trino.spi.block.RowValueBuilder.buildRowValue(IPADDRESS_PAIR, fields -> {
            io.trino.spi.type.VarcharType.VARCHAR.writeSlice(fields.get(0), Slices.utf8Slice(formatIpAddress(first)));
            io.trino.spi.type.VarcharType.VARCHAR.writeSlice(fields.get(1), Slices.utf8Slice(formatIpAddress(second)));
        });
    }

    @ScalarFunction("hogql_plugin_version")
    @Description("Returns the HogQL UDF plugin version")
    @SqlType(VARCHAR)
    public static Slice pluginVersion()
    {
        return Slices.utf8Slice("0.1.0");
    }

    @ScalarFunction("hogql_erf")
    @SqlType(DOUBLE)
    public static double erf(@SqlType(DOUBLE) double value)
    {
        return Erf.erf(value);
    }

    @ScalarFunction("hogql_erfc")
    @SqlType(DOUBLE)
    public static double erfc(@SqlType(DOUBLE) double value)
    {
        return Erf.erfc(value);
    }

    @ScalarFunction("hogql_lgamma")
    @SqlType(DOUBLE)
    public static double lgamma(@SqlType(DOUBLE) double value)
    {
        if (value == 1 || value == 2) {
            return 0;
        }
        return Gamma.logGamma(value);
    }

    @ScalarFunction("hogql_tgamma")
    @SqlType(DOUBLE)
    public static double tgamma(@SqlType(DOUBLE) double value)
    {
        return Gamma.gamma(value);
    }

    @ScalarFunction("hogql_great_circle_angle")
    @SqlType(DOUBLE)
    public static double greatCircleAngle(
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude2,
            @SqlType(DOUBLE) double latitude2)
    {
        return geographicDistance(longitude1, latitude1, longitude2, latitude2, 0);
    }

    @ScalarFunction("hogql_great_circle_distance")
    @SqlType(DOUBLE)
    public static double greatCircleDistance(
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude2,
            @SqlType(DOUBLE) double latitude2)
    {
        return geographicDistance(longitude1, latitude1, longitude2, latitude2, 1);
    }

    @ScalarFunction("hogql_geo_distance")
    @SqlType(DOUBLE)
    public static double geoDistance(
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude2,
            @SqlType(DOUBLE) double latitude2)
    {
        return geographicDistance(longitude1, latitude1, longitude2, latitude2, 2);
    }

    @ScalarFunction("hogql_geohash_encode")
    @SqlType(VARCHAR)
    public static Slice geohashEncode(@SqlType(DOUBLE) double longitude, @SqlType(DOUBLE) double latitude)
    {
        return Slices.utf8Slice(geohashEncodeValue(longitude, latitude, 12));
    }

    @ScalarFunction("hogql_geohash_encode")
    @SqlType(VARCHAR)
    public static Slice geohashEncode(
            @SqlType(DOUBLE) double longitude,
            @SqlType(DOUBLE) double latitude,
            @SqlType(BIGINT) long precision)
    {
        return Slices.utf8Slice(geohashEncodeValue(longitude, latitude, geohashPrecision(precision)));
    }

    @ScalarFunction("hogql_geohash_decode")
    @SqlType("row(double, double)")
    public static SqlRow geohashDecode(@SqlType(VARCHAR) Slice encoded)
    {
        return geohashDecodeValue(encoded.toStringUtf8());
    }

    @ScalarFunction("hogql_base58_decode")
    @SqlType(VARCHAR)
    public static Slice base58Decode(@SqlType(VARCHAR) Slice encoded)
    {
        return base58DecodeValue(encoded);
    }

    @ScalarFunction(value = "hogql_try_base58_decode", neverFails = true)
    @SqlType(VARCHAR)
    public static Slice tryBase58Decode(@SqlType(VARCHAR) Slice encoded)
    {
        try {
            return base58DecodeValue(encoded);
        }
        catch (RuntimeException exception) {
            return Slices.EMPTY_SLICE;
        }
    }

    @ScalarFunction("hogql_unhex")
    @SqlType(VARCHAR)
    public static Slice unhex(@SqlType(VARCHAR) Slice encoded)
    {
        String text = encoded.toStringUtf8();
        byte[] result = new byte[(text.length() + 1) / 2];
        int source = 0;
        int target = 0;
        if ((text.length() & 1) == 1) {
            result[target++] = (byte) Character.digit(text.charAt(source++), 16);
        }
        while (source < text.length()) {
            int high = Character.digit(text.charAt(source++), 16);
            int low = Character.digit(text.charAt(source++), 16);
            result[target++] = (byte) ((high << 4) | low);
        }
        return Slices.wrappedBuffer(result);
    }

    @ScalarFunction("hogql_convert_charset")
    @SqlType(VARCHAR)
    public static Slice convertCharset(
            @SqlType(VARCHAR) Slice value,
            @SqlType(VARCHAR) Slice sourceCharset,
            @SqlType(VARCHAR) Slice targetCharset)
    {
        Charset source = Charset.forName(sourceCharset.toStringUtf8());
        Charset target = Charset.forName(targetCharset.toStringUtf8());
        ByteBuffer encoded = target.encode(source.decode(value.toByteBuffer()));
        byte[] result = new byte[encoded.remaining()];
        encoded.get(result);
        return Slices.wrappedBuffer(result);
    }

    @ScalarFunction("hogql_extract_text_from_html")
    @SqlType(VARCHAR)
    public static Slice extractTextFromHtml(@SqlType(VARCHAR) Slice value)
    {
        String text = value.toStringUtf8();
        text = text.replaceAll("(?s)<!--.*?(?:-->|$)", " ");
        text = text.replaceAll("(?s)<script(?:\\s|>).*?(?:</script\\s*>|$)", " ");
        text = text.replaceAll("(?s)<style(?:\\s|>).*?(?:</style\\s*>|$)", " ");
        text = text.replaceAll("(?s)<!\\[CDATA\\[(.*?)]]>", "$1");
        text = text.replaceAll("(?s)<[^>]*(?:>|$)", " ");
        text = text.replaceAll("[ \\t\\n\\r\\f\\u000B]+", " ").trim();
        return Slices.utf8Slice(text);
    }

    @ScalarFunction("hogql_ipv6_string_to_num")
    @SqlType("varbinary")
    public static Slice ipv6StringToNum(@SqlType(VARCHAR) Slice value)
    {
        return parseIpAddress(value);
    }

    @ScalarFunction(value = "hogql_ipv6_string_to_num_or_default", neverFails = true)
    @SqlType("varbinary")
    public static Slice ipv6StringToNumOrDefault(@SqlType(VARCHAR) Slice value)
    {
        try {
            return parseIpAddress(value);
        }
        catch (RuntimeException exception) {
            return Slices.allocate(16);
        }
    }

    @ScalarFunction(value = "hogql_ipv6_string_to_num_or_null", neverFails = true)
    @SqlNullable
    @SqlType("varbinary")
    public static Slice ipv6StringToNumOrNull(@SqlType(VARCHAR) Slice value)
    {
        try {
            return parseIpAddress(value);
        }
        catch (RuntimeException exception) {
            return null;
        }
    }

    @ScalarFunction("hogql_ipv6_num_to_string")
    @SqlType(VARCHAR)
    public static Slice ipv6NumToString(@SqlType("varbinary") Slice value)
    {
        return Slices.utf8Slice(formatIpAddress(value));
    }

    @ScalarFunction("hogql_to_ipv6")
    @SqlType(IPADDRESS)
    public static Slice toIpv6(@SqlType(VARCHAR) Slice value)
    {
        return parseIpAddress(value);
    }

    @ScalarFunction(value = "hogql_to_ipv6_or_zero", neverFails = true)
    @SqlType(IPADDRESS)
    public static Slice toIpv6OrZero(@SqlType(VARCHAR) Slice value)
    {
        try {
            return parseIpAddress(value);
        }
        catch (RuntimeException exception) {
            return Slices.allocate(16);
        }
    }

    @ScalarFunction(value = "hogql_to_ipv6_or_null", neverFails = true)
    @SqlNullable
    @SqlType(IPADDRESS)
    public static Slice toIpv6OrNull(@SqlType(VARCHAR) Slice value)
    {
        try {
            return parseIpAddress(value);
        }
        catch (RuntimeException exception) {
            return null;
        }
    }

    @ScalarFunction(value = "hogql_to_ipv6_or_default", neverFails = true)
    @SqlType(IPADDRESS)
    public static Slice toIpv6OrDefault(@SqlType(VARCHAR) Slice value)
    {
        return toIpv6OrZero(value);
    }

    @ScalarFunction(value = "hogql_to_ipv6_or_default", neverFails = true)
    @SqlType(IPADDRESS)
    public static Slice toIpv6OrDefault(@SqlType(VARCHAR) Slice value, @SqlType(IPADDRESS) Slice defaultValue)
    {
        try {
            return parseIpAddress(value);
        }
        catch (RuntimeException exception) {
            return defaultValue;
        }
    }

    @ScalarFunction("hogql_ipv4_to_ipv6")
    @SqlType(VARCHAR)
    public static Slice ipv4ToIpv6(@SqlType(IPADDRESS) Slice value)
    {
        return Slices.utf8Slice(formatIpAddress(value));
    }

    @ScalarFunction("hogql_ipv6_cidr_to_range")
    @SqlType("row(varchar, varchar)")
    public static SqlRow ipv6CidrToRange(@SqlType(IPADDRESS) Slice value, @SqlType(BIGINT) long prefixLength)
    {
        if (prefixLength < 0 || prefixLength > 128) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "IPv6 CIDR prefix must be between 0 and 128");
        }
        byte[] lower = value.getBytes();
        byte[] upper = value.getBytes();
        for (int bit = toIntExact(prefixLength); bit < 128; bit++) {
            int byteIndex = bit / 8;
            int mask = 1 << (7 - bit % 8);
            lower[byteIndex] &= (byte) ~mask;
            upper[byteIndex] |= (byte) mask;
        }
        return ipAddressPair(Slices.wrappedBuffer(lower), Slices.wrappedBuffer(upper));
    }

    @ScalarFunction("hogql_cut_ipv6")
    @SqlType(VARCHAR)
    public static Slice cutIpv6(
            @SqlType("varbinary") Slice value,
            @SqlType(BIGINT) long bytesToCutForIpv6,
            @SqlType(BIGINT) long bytesToCutForIpv4)
    {
        if (value.length() != 16) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "An IPv6 binary value must contain 16 bytes");
        }
        byte[] address = value.getBytes();
        boolean mapped = true;
        for (int index = 0; index < 10; index++) {
            mapped &= address[index] == 0;
        }
        mapped &= (address[10] & 0xFF) == 0xFF && (address[11] & 0xFF) == 0xFF;
        int bytesToCut = toIntExact(mapped ? bytesToCutForIpv4 : bytesToCutForIpv6);
        if (bytesToCut < 0 || bytesToCut > 16) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "cutIPv6 byte count must be between 0 and 16");
        }
        for (int index = address.length - bytesToCut; index < address.length; index++) {
            address[index] = 0;
        }
        return Slices.utf8Slice(formatIpAddress(Slices.wrappedBuffer(address)));
    }

    @ScalarFunction("hogql_geohashes_in_box")
    @SqlType("array(varchar)")
    public static Block geohashesInBox(
            @SqlType(DOUBLE) double longitudeMin,
            @SqlType(DOUBLE) double latitudeMin,
            @SqlType(DOUBLE) double longitudeMax,
            @SqlType(DOUBLE) double latitudeMax,
            @SqlType(BIGINT) long requestedPrecision)
    {
        if (longitudeMax < longitudeMin
                || latitudeMax < latitudeMin
                || Double.isNaN(longitudeMin)
                || Double.isNaN(longitudeMax)
                || Double.isNaN(latitudeMin)
                || Double.isNaN(latitudeMax)) {
            return io.trino.spi.type.VarcharType.VARCHAR.createBlockBuilder(null, 0).build();
        }
        longitudeMin = Math.max(-180, Math.min(180, longitudeMin));
        longitudeMax = Math.max(-180, Math.min(180, longitudeMax));
        latitudeMin = Math.max(-90, Math.min(90, latitudeMin));
        latitudeMax = Math.max(-90, Math.min(90, latitudeMax));
        int precision = geohashPrecision(requestedPrecision);
        int longitudeBits = (precision * 5) / 2 + (precision % 2);
        int latitudeBits = (precision * 5) / 2;
        double longitudeStep = Math.scalb(360, -longitudeBits);
        double latitudeStep = Math.scalb(180, -latitudeBits);
        double alignedLongitudeMin = Math.floor(longitudeMin / longitudeStep) * longitudeStep;
        double alignedLatitudeMin = Math.floor(latitudeMin / latitudeStep) * latitudeStep;
        double alignedLongitudeMax = Math.ceil(longitudeMax / longitudeStep) * longitudeStep;
        double alignedLatitudeMax = Math.ceil(latitudeMax / latitudeStep) * latitudeStep;
        long longitudeItems = (long) ((alignedLongitudeMax - alignedLongitudeMin) / longitudeStep);
        long latitudeItems = (long) ((alignedLatitudeMax - alignedLatitudeMin) / latitudeStep);
        long itemCount = Math.max(1, longitudeItems * latitudeItems);
        checkArraySize(itemCount);
        BlockBuilder builder = io.trino.spi.type.VarcharType.VARCHAR.createBlockBuilder(null, toIntExact(itemCount));
        long written = 0;
        if (longitudeItems != 0 && latitudeItems != 0) {
            for (long longitudeIndex = 0; longitudeIndex < longitudeItems; longitudeIndex++) {
                for (long latitudeIndex = 0; latitudeIndex < latitudeItems; latitudeIndex++) {
                    String value = geohashEncodeValue(
                            alignedLongitudeMin + longitudeStep * longitudeIndex,
                            alignedLatitudeMin + latitudeStep * latitudeIndex,
                            precision);
                    io.trino.spi.type.VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value));
                    written++;
                }
            }
        }
        if (written == 0) {
            String value = geohashEncodeValue(alignedLongitudeMin, alignedLatitudeMin, precision);
            io.trino.spi.type.VarcharType.VARCHAR.writeSlice(builder, Slices.utf8Slice(value));
        }
        return builder.build();
    }

    @ScalarFunction("hogql_bitmap_build")
    @SqlType("varbinary")
    public static Slice bitmapBuild(@SqlType("array(bigint)") Block values)
    {
        return encodeBitmap(bitmapFromArray(values));
    }

    @ScalarFunction("hogql_bitmap_to_array")
    @SqlType("array(bigint)")
    public static Block bitmapToArray(@SqlType("varbinary") Slice bitmap)
    {
        return bitmapToBlock(decodeBitmap(bitmap));
    }

    @ScalarFunction("hogql_bitmap_cardinality")
    @SqlType(BIGINT)
    public static long bitmapCardinality(@SqlType("varbinary") Slice bitmap)
    {
        return decodeBitmap(bitmap).size();
    }

    @ScalarFunction("hogql_bitmap_contains")
    @SqlType(BIGINT)
    public static long bitmapContains(@SqlType("varbinary") Slice bitmap, @SqlType(BIGINT) long value)
    {
        return bool(decodeBitmap(bitmap).contains(value));
    }

    @ScalarFunction("hogql_bitmap_has_any")
    @SqlType(BIGINT)
    public static long bitmapHasAny(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        Set<Long> values = decodeBitmap(left);
        values.retainAll(decodeBitmap(right));
        return bool(!values.isEmpty());
    }

    @ScalarFunction("hogql_bitmap_has_all")
    @SqlType(BIGINT)
    public static long bitmapHasAll(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        return bool(decodeBitmap(left).containsAll(decodeBitmap(right)));
    }

    @ScalarFunction("hogql_bitmap_min")
    @SqlType(BIGINT)
    public static long bitmapMin(@SqlType("varbinary") Slice bitmap)
    {
        Set<Long> values = decodeBitmap(bitmap);
        long minimum = -1;
        for (long value : values) {
            if (Long.compareUnsigned(value, minimum) < 0) {
                minimum = value;
            }
        }
        return minimum;
    }

    @ScalarFunction("hogql_bitmap_max")
    @SqlType(BIGINT)
    public static long bitmapMax(@SqlType("varbinary") Slice bitmap)
    {
        Set<Long> values = decodeBitmap(bitmap);
        long maximum = 0;
        for (long value : values) {
            if (Long.compareUnsigned(value, maximum) > 0) {
                maximum = value;
            }
        }
        return maximum;
    }

    @ScalarFunction("hogql_bitmap_and")
    @SqlType("varbinary")
    public static Slice bitmapAnd(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        Set<Long> values = decodeBitmap(left);
        values.retainAll(decodeBitmap(right));
        return encodeBitmap(values);
    }

    @ScalarFunction("hogql_bitmap_or")
    @SqlType("varbinary")
    public static Slice bitmapOr(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        Set<Long> values = decodeBitmap(left);
        values.addAll(decodeBitmap(right));
        return encodeBitmap(values);
    }

    @ScalarFunction("hogql_bitmap_xor")
    @SqlType("varbinary")
    public static Slice bitmapXor(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        Set<Long> leftValues = decodeBitmap(left);
        Set<Long> rightValues = decodeBitmap(right);
        Set<Long> intersection = new LinkedHashSet<>();
        intersection.addAll(leftValues);
        intersection.retainAll(rightValues);
        leftValues.addAll(rightValues);
        leftValues.removeAll(intersection);
        return encodeBitmap(leftValues);
    }

    @ScalarFunction("hogql_bitmap_andnot")
    @SqlType("varbinary")
    public static Slice bitmapAndNot(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        Set<Long> values = decodeBitmap(left);
        values.removeAll(decodeBitmap(right));
        return encodeBitmap(values);
    }

    @ScalarFunction("hogql_bitmap_and_cardinality")
    @SqlType(BIGINT)
    public static long bitmapAndCardinality(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        return bitmapCardinality(bitmapAnd(left, right));
    }

    @ScalarFunction("hogql_bitmap_or_cardinality")
    @SqlType(BIGINT)
    public static long bitmapOrCardinality(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        return bitmapCardinality(bitmapOr(left, right));
    }

    @ScalarFunction("hogql_bitmap_xor_cardinality")
    @SqlType(BIGINT)
    public static long bitmapXorCardinality(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        return bitmapCardinality(bitmapXor(left, right));
    }

    @ScalarFunction("hogql_bitmap_andnot_cardinality")
    @SqlType(BIGINT)
    public static long bitmapAndNotCardinality(@SqlType("varbinary") Slice left, @SqlType("varbinary") Slice right)
    {
        return bitmapCardinality(bitmapAndNot(left, right));
    }

    @ScalarFunction("hogql_bitmap_subset_in_range")
    @SqlType("varbinary")
    public static Slice bitmapSubsetInRange(
            @SqlType("varbinary") Slice bitmap,
            @SqlType(BIGINT) long start,
            @SqlType(BIGINT) long end)
    {
        Set<Long> result = new LinkedHashSet<>();
        for (long value : decodeBitmap(bitmap)) {
            if (Long.compareUnsigned(value, start) >= 0 && Long.compareUnsigned(value, end) < 0) {
                result.add(value);
            }
        }
        return encodeBitmap(result);
    }

    @ScalarFunction("hogql_bitmap_subset_limit")
    @SqlType("varbinary")
    public static Slice bitmapSubsetLimit(
            @SqlType("varbinary") Slice bitmap,
            @SqlType(BIGINT) long start,
            @SqlType(BIGINT) long limit)
    {
        Set<Long> result = new LinkedHashSet<>();
        for (long value : decodeBitmap(bitmap)) {
            if (Long.compareUnsigned(value, start) >= 0 && result.size() < limit) {
                result.add(value);
            }
        }
        return encodeBitmap(result);
    }

    @ScalarFunction("hogql_sub_bitmap")
    @SqlType("varbinary")
    public static Slice subBitmap(
            @SqlType("varbinary") Slice bitmap,
            @SqlType(BIGINT) long offset,
            @SqlType(BIGINT) long limit)
    {
        Set<Long> result = new LinkedHashSet<>();
        long position = 0;
        for (long value : decodeBitmap(bitmap)) {
            if (position >= offset && result.size() < limit) {
                result.add(value);
            }
            position++;
        }
        return encodeBitmap(result);
    }

    @ScalarFunction("hogql_bitmap_transform")
    @SqlType("varbinary")
    public static Slice bitmapTransform(
            @SqlType("varbinary") Slice bitmap,
            @SqlType("array(bigint)") Block from,
            @SqlType("array(bigint)") Block to)
    {
        if (from.getPositionCount() != to.getPositionCount()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "bitmapTransform arrays must have equal lengths");
        }
        Set<Long> original = decodeBitmap(bitmap);
        Set<Long> values = new LinkedHashSet<>(original);
        Set<Long> replacements = new LinkedHashSet<>();
        for (int position = 0; position < from.getPositionCount(); position++) {
            if (from.isNull(position) || to.isNull(position)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "bitmapTransform arrays must not contain null values");
            }
            long source = io.trino.spi.type.BigintType.BIGINT.getLong(from, position);
            long target = io.trino.spi.type.BigintType.BIGINT.getLong(to, position);
            if (original.contains(source)) {
                values.remove(source);
                replacements.add(target);
            }
        }
        values.addAll(replacements);
        return encodeBitmap(values);
    }

    @ScalarFunction("hogql_geo_to_h3")
    @SqlType(BIGINT)
    public static long geoToH3(
            @SqlType(DOUBLE) double latitude,
            @SqlType(DOUBLE) double longitude,
            @SqlType(BIGINT) long resolution)
    {
        return H3.geoToH3(latitude, longitude, toIntExact(resolution));
    }

    @ScalarFunction("hogql_h3_is_valid")
    @SqlType(BIGINT)
    public static long h3IsValid(@SqlType(BIGINT) long index)
    {
        return bool(H3.h3IsValid(index));
    }

    @ScalarFunction("hogql_h3_get_resolution")
    @SqlType(BIGINT)
    public static long h3GetResolution(@SqlType(BIGINT) long index)
    {
        return H3.h3GetResolution(index);
    }

    @ScalarFunction("hogql_h3_get_base_cell")
    @SqlType(BIGINT)
    public static long h3GetBaseCell(@SqlType(BIGINT) long index)
    {
        return H3.h3GetBaseCell(index);
    }

    @ScalarFunction("hogql_h3_edge_angle")
    @SqlType(DOUBLE)
    public static double h3EdgeAngle(@SqlType(BIGINT) long resolution)
    {
        return H3.edgeLength(toIntExact(resolution), LengthUnit.m) * DEGREES_PER_METER;
    }

    @ScalarFunction("hogql_h3_edge_length_m")
    @SqlType(DOUBLE)
    public static double h3EdgeLengthM(@SqlType(BIGINT) long resolution)
    {
        return H3.edgeLength(toIntExact(resolution), LengthUnit.m);
    }

    @ScalarFunction("hogql_h3_edge_length_km")
    @SqlType(DOUBLE)
    public static double h3EdgeLengthKm(@SqlType(BIGINT) long resolution)
    {
        return H3.edgeLength(toIntExact(resolution), LengthUnit.km);
    }

    @ScalarFunction("hogql_h3_hex_area_m2")
    @SqlType(DOUBLE)
    public static double h3HexAreaM2(@SqlType(BIGINT) long resolution)
    {
        return H3.hexArea(toIntExact(resolution), AreaUnit.m2);
    }

    @ScalarFunction("hogql_h3_hex_area_km2")
    @SqlType(DOUBLE)
    public static double h3HexAreaKm2(@SqlType(BIGINT) long resolution)
    {
        return H3.hexArea(toIntExact(resolution), AreaUnit.km2);
    }

    @ScalarFunction("hogql_h3_indexes_are_neighbors")
    @SqlType(BIGINT)
    public static long h3IndexesAreNeighbors(@SqlType(BIGINT) long left, @SqlType(BIGINT) long right)
    {
        return bool(H3.h3IndexesAreNeighbors(left, right));
    }

    @ScalarFunction("hogql_h3_to_parent")
    @SqlType(BIGINT)
    public static long h3ToParent(@SqlType(BIGINT) long index, @SqlType(BIGINT) long resolution)
    {
        return H3.h3ToParent(index, toIntExact(resolution));
    }

    @ScalarFunction("hogql_h3_to_geo")
    @SqlType("row(double, double)")
    public static SqlRow h3ToGeo(@SqlType(BIGINT) long index)
    {
        LatLng coordinate = H3.h3ToGeo(index);
        return io.trino.spi.block.RowValueBuilder.buildRowValue(DOUBLE_PAIR, fields -> {
            io.trino.spi.type.DoubleType.DOUBLE.writeDouble(fields.get(0), coordinate.lat);
            io.trino.spi.type.DoubleType.DOUBLE.writeDouble(fields.get(1), coordinate.lng);
        });
    }

    @ScalarFunction("hogql_h3_to_geo_boundary")
    @SqlType("array(row(double, double))")
    public static Block h3ToGeoBoundary(@SqlType(BIGINT) long index)
    {
        return coordinateArray(H3.h3ToGeoBoundary(index));
    }

    @ScalarFunction("hogql_h3_to_string")
    @SqlType(VARCHAR)
    public static Slice h3ToString(@SqlType(BIGINT) long index)
    {
        return Slices.utf8Slice(H3.h3ToString(index));
    }

    @ScalarFunction("hogql_string_to_h3")
    @SqlType(BIGINT)
    public static long stringToH3(@SqlType(VARCHAR) Slice index)
    {
        return H3.stringToH3(index.toStringUtf8());
    }

    @ScalarFunction("hogql_h3_is_res_class_iii")
    @SqlType(BIGINT)
    public static long h3IsResClassIII(@SqlType(BIGINT) long index)
    {
        return bool(H3.h3IsResClassIII(index));
    }

    @ScalarFunction("hogql_h3_is_pentagon")
    @SqlType(BIGINT)
    public static long h3IsPentagon(@SqlType(BIGINT) long index)
    {
        return bool(H3.h3IsPentagon(index));
    }

    @ScalarFunction("hogql_h3_cell_area_m2")
    @SqlType(DOUBLE)
    public static double h3CellAreaM2(@SqlType(BIGINT) long index)
    {
        return H3.cellArea(index, AreaUnit.m2);
    }

    @ScalarFunction("hogql_h3_cell_area_rads2")
    @SqlType(DOUBLE)
    public static double h3CellAreaRads2(@SqlType(BIGINT) long index)
    {
        return H3.cellArea(index, AreaUnit.rads2);
    }

    @ScalarFunction("hogql_h3_to_center_child")
    @SqlType(BIGINT)
    public static long h3ToCenterChild(@SqlType(BIGINT) long index, @SqlType(BIGINT) long resolution)
    {
        return H3.h3ToCenterChild(index, toIntExact(resolution));
    }

    @ScalarFunction("hogql_h3_exact_edge_length_m")
    @SqlType(DOUBLE)
    public static double h3ExactEdgeLengthM(@SqlType(BIGINT) long edge)
    {
        return H3.exactEdgeLength(edge, LengthUnit.m);
    }

    @ScalarFunction("hogql_h3_exact_edge_length_km")
    @SqlType(DOUBLE)
    public static double h3ExactEdgeLengthKm(@SqlType(BIGINT) long edge)
    {
        return H3.exactEdgeLength(edge, LengthUnit.km);
    }

    @ScalarFunction("hogql_h3_exact_edge_length_rads")
    @SqlType(DOUBLE)
    public static double h3ExactEdgeLengthRads(@SqlType(BIGINT) long edge)
    {
        return H3.exactEdgeLength(edge, LengthUnit.rads);
    }

    @ScalarFunction("hogql_h3_num_hexagons")
    @SqlType(BIGINT)
    public static long h3NumHexagons(@SqlType(BIGINT) long resolution)
    {
        return H3.numHexagons(toIntExact(resolution));
    }

    @ScalarFunction("hogql_h3_point_dist_m")
    @SqlType(DOUBLE)
    public static double h3PointDistM(
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude2,
            @SqlType(DOUBLE) double longitude2)
    {
        return H3.pointDist(new LatLng(latitude1, longitude1), new LatLng(latitude2, longitude2), LengthUnit.m);
    }

    @ScalarFunction("hogql_h3_point_dist_km")
    @SqlType(DOUBLE)
    public static double h3PointDistKm(
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude2,
            @SqlType(DOUBLE) double longitude2)
    {
        return H3.pointDist(new LatLng(latitude1, longitude1), new LatLng(latitude2, longitude2), LengthUnit.km);
    }

    @ScalarFunction("hogql_h3_point_dist_rads")
    @SqlType(DOUBLE)
    public static double h3PointDistRads(
            @SqlType(DOUBLE) double latitude1,
            @SqlType(DOUBLE) double longitude1,
            @SqlType(DOUBLE) double latitude2,
            @SqlType(DOUBLE) double longitude2)
    {
        return H3.pointDist(new LatLng(latitude1, longitude1), new LatLng(latitude2, longitude2), LengthUnit.rads);
    }

    @ScalarFunction("hogql_h3_distance")
    @SqlType(BIGINT)
    public static long h3Distance(@SqlType(BIGINT) long left, @SqlType(BIGINT) long right)
    {
        return H3.h3Distance(left, right);
    }

    @ScalarFunction("hogql_h3_get_unidirectional_edge")
    @SqlType(BIGINT)
    public static long h3GetUnidirectionalEdge(@SqlType(BIGINT) long origin, @SqlType(BIGINT) long destination)
    {
        return H3.getH3UnidirectionalEdge(origin, destination);
    }

    @ScalarFunction("hogql_h3_unidirectional_edge_is_valid")
    @SqlType(BIGINT)
    public static long h3UnidirectionalEdgeIsValid(@SqlType(BIGINT) long edge)
    {
        return bool(H3.h3UnidirectionalEdgeIsValid(edge));
    }

    @ScalarFunction("hogql_h3_get_origin_index_from_unidirectional_edge")
    @SqlType(BIGINT)
    public static long h3GetOriginIndexFromUnidirectionalEdge(@SqlType(BIGINT) long edge)
    {
        return H3.getOriginH3IndexFromUnidirectionalEdge(edge);
    }

    @ScalarFunction("hogql_h3_get_destination_index_from_unidirectional_edge")
    @SqlType(BIGINT)
    public static long h3GetDestinationIndexFromUnidirectionalEdge(@SqlType(BIGINT) long edge)
    {
        return H3.getDestinationH3IndexFromUnidirectionalEdge(edge);
    }

    @ScalarFunction("hogql_h3_k_ring")
    @SqlType("array(bigint)")
    public static Block h3kRing(@SqlType(BIGINT) long index, @SqlType(BIGINT) long distance)
    {
        checkRingSize(distance);
        return longArray(H3.kRing(index, toIntExact(distance)));
    }

    @ScalarFunction("hogql_h3_to_children")
    @SqlType("array(bigint)")
    public static Block h3ToChildren(@SqlType(BIGINT) long index, @SqlType(BIGINT) long resolution)
    {
        if (!H3.h3IsValid(index) || resolution < H3.h3GetResolution(index) || resolution > 15) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid H3 cell or child resolution");
        }
        long children = 1;
        for (int childResolution = H3.h3GetResolution(index); childResolution < resolution; childResolution++) {
            children *= 7;
            checkArraySize(children);
        }
        return longArray(H3.h3ToChildren(index, toIntExact(resolution)));
    }

    @ScalarFunction("hogql_h3_get_faces")
    @SqlType("array(bigint)")
    public static Block h3GetFaces(@SqlType(BIGINT) long index)
    {
        return longArray(H3.h3GetFaces(index));
    }

    @ScalarFunction("hogql_h3_get_res0_indexes")
    @SqlType("array(bigint)")
    public static Block h3GetRes0Indexes()
    {
        return longArray(H3.getRes0Indexes());
    }

    @ScalarFunction("hogql_h3_get_pentagon_indexes")
    @SqlType("array(bigint)")
    public static Block h3GetPentagonIndexes(@SqlType(BIGINT) long resolution)
    {
        return longArray(H3.getPentagonIndexes(toIntExact(resolution)));
    }

    @ScalarFunction("hogql_h3_line")
    @SqlType("array(bigint)")
    public static Block h3Line(@SqlType(BIGINT) long start, @SqlType(BIGINT) long end)
    {
        checkArraySize(H3.h3Distance(start, end) + 1);
        return longArray(H3.h3Line(start, end));
    }

    @ScalarFunction("hogql_h3_hex_ring")
    @SqlType("array(bigint)")
    public static Block h3HexRing(@SqlType(BIGINT) long index, @SqlType(BIGINT) long distance)
    {
        checkRingSize(distance);
        return longArray(H3.hexRing(index, toIntExact(distance)));
    }

    @ScalarFunction("hogql_h3_get_indexes_from_unidirectional_edge")
    @SqlType("row(bigint, bigint)")
    public static SqlRow h3GetIndexesFromUnidirectionalEdge(@SqlType(BIGINT) long edge)
    {
        return longPair(H3.getH3IndexesFromUnidirectionalEdge(edge));
    }

    @ScalarFunction("hogql_h3_get_unidirectional_edges_from_hexagon")
    @SqlType("array(bigint)")
    public static Block h3GetUnidirectionalEdgesFromHexagon(@SqlType(BIGINT) long index)
    {
        return longArray(H3.getH3UnidirectionalEdgesFromHexagon(index));
    }

    @ScalarFunction("hogql_h3_get_unidirectional_edge_boundary")
    @SqlType("array(row(double, double))")
    public static Block h3GetUnidirectionalEdgeBoundary(@SqlType(BIGINT) long edge)
    {
        return coordinateArray(H3.getH3UnidirectionalEdgeBoundary(edge));
    }
}
