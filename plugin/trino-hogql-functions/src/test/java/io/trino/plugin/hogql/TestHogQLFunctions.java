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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IpAddressType.IPADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestHogQLFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addPlugin(new HogQLPlugin());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testVersion()
    {
        assertThat(assertions.function("hogql_plugin_version")).isEqualTo("0.1.0");
    }

    @Test
    public void testMathematicalFunctions()
    {
        assertThat(assertions.function("hogql_erf", "0.0")).isEqualTo(0.0);
        assertThat(assertions.function("hogql_erfc", "0.0")).isEqualTo(1.0);
        assertThat(assertions.function("hogql_lgamma", "1.0")).isEqualTo(0.0);
        assertThat(assertions.function("hogql_tgamma", "5.0")).isEqualTo(24.0);
        assertThat(assertions.function("hogql_erf", "CAST(NULL AS DOUBLE)")).isNull(DOUBLE);
    }

    @Test
    public void testGeographicFunctions()
    {
        assertThat(assertions.function("hogql_geo_distance", "0.0", "0.0", "0.0", "0.0")).isEqualTo(0.0);
        assertThat(assertions.function("hogql_great_circle_angle", "0.0", "0.0", "1.0", "0.0")).isEqualTo(1.0);
        assertThat(assertions.query("SELECT hogql_great_circle_distance(0.0, 0.0, 1.0, 0.0) BETWEEN 110000 AND 112000"))
                .matches("VALUES true");
        assertTrinoExceptionThrownBy(assertions.function("hogql_geo_distance", "0.0", "91.0", "0.0", "0.0")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testGeohashes()
    {
        assertThat(assertions.function("hogql_geohash_encode", "0.0", "0.0", "1")).isEqualTo("s");
        assertThat(assertions.function("hogql_geohash_decode", "'s'"))
                .matches("ROW(CAST(ROW(22.5, 22.5) AS ROW(DOUBLE, DOUBLE)))");
        assertThat(assertions.function("hogql_geohashes_in_box", "0.0", "0.0", "0.0", "0.0", "1"))
                .matches("CAST(ARRAY['s'] AS ARRAY(VARCHAR))");
        assertTrinoExceptionThrownBy(assertions.function("hogql_geohashes_in_box", "-180.0", "-90.0", "180.0", "90.0", "12")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testEncodingFunctions()
    {
        assertThat(assertions.function("hogql_base58_decode", "'StV1DL6CwTryKyV'")).isEqualTo("hello world");
        assertThat(assertions.function("hogql_try_base58_decode", "'0'")).isEqualTo("");
        assertTrinoExceptionThrownBy(assertions.function("hogql_base58_decode", "'0'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertThat(assertions.function("hogql_unhex", "'48656c6c6f'")).isEqualTo("Hello");
        assertThat(assertions.function("hogql_convert_charset", "'hello'", "'UTF-8'", "'ASCII'")).isEqualTo("hello");
        assertThat(assertions.function("hogql_extract_text_from_html", "'<p>Hello</p><script>hidden</script><b>world</b>'"))
                .isEqualTo("Hello world");
    }

    @Test
    public void testIpv6()
    {
        assertThat(assertions.function("hogql_to_ipv6", "'2001:db8::1'")).matches("IPADDRESS '2001:db8::1'");
        assertThat(assertions.function("hogql_ipv6_num_to_string", "hogql_ipv6_string_to_num('2001:db8::1')"))
                .isEqualTo("2001:db8::1");
        assertThat(assertions.function("hogql_ipv6_num_to_string", "hogql_ipv6_string_to_num('192.0.2.1')"))
                .isEqualTo("::ffff:192.0.2.1");
        assertThat(assertions.function("hogql_ipv6_cidr_to_range", "IPADDRESS '2001:db8::1'", "126"))
                .matches("ROW(CAST(ROW('2001:db8::', '2001:db8::3') AS ROW(VARCHAR, VARCHAR)))");
        assertThat(assertions.function("hogql_cut_ipv6", "hogql_ipv6_string_to_num('2001:db8::1')", "1", "1"))
                .isEqualTo("2001:db8::");
    }

    @Test
    public void testInvalidIpInputs()
    {
        assertTrinoExceptionThrownBy(assertions.function("hogql_to_ipv6", "'example.com'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertTrinoExceptionThrownBy(assertions.function("hogql_to_ipv6", "'dead.beef'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertThat(assertions.function("hogql_to_ipv6_or_null", "'invalid'")).isNull(IPADDRESS);
        assertThat(assertions.function("hogql_ipv6_string_to_num_or_null", "'invalid'")).isNull(VARBINARY);
        assertThat(assertions.function("hogql_to_ipv6_or_zero", "'invalid'")).matches("IPADDRESS '::'");
        assertThat(assertions.function("hogql_to_ipv6_or_default", "'invalid'", "IPADDRESS '2001:db8::1'"))
                .matches("IPADDRESS '2001:db8::1'");
        assertTrinoExceptionThrownBy(assertions.function("hogql_cut_ipv6", "X'01'", "1", "1")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertTrinoExceptionThrownBy(assertions.function("hogql_ipv6_cidr_to_range", "IPADDRESS '::1'", "129")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitmapOrderingAndDuplicates()
    {
        assertThat(assertions.function("hogql_bitmap_to_array", "hogql_bitmap_build(ARRAY[BIGINT '3', 1, 3, 2, NULL])"))
                .matches("ARRAY[BIGINT '1', 2, 3]");
        assertThat(assertions.function("hogql_bitmap_cardinality", "hogql_bitmap_build(ARRAY[BIGINT '3', 1, 3, 2])")).isEqualTo(3L);
        assertThat(assertions.function("hogql_bitmap_to_array", "hogql_bitmap_build(ARRAY[BIGINT '-1', 0, 1])"))
                .matches("ARRAY[BIGINT '0', 1, -1]");
        assertThat(assertions.function("hogql_bitmap_min", "hogql_bitmap_build(CAST(ARRAY[] AS ARRAY(BIGINT)))")).isEqualTo(-1L);
        assertThat(assertions.function("hogql_bitmap_max", "hogql_bitmap_build(CAST(ARRAY[] AS ARRAY(BIGINT)))")).isEqualTo(0L);
    }

    @Test
    public void testInvalidBitmapEncoding()
    {
        assertTrinoExceptionThrownBy(assertions.function("hogql_bitmap_cardinality", "X'01'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertTrinoExceptionThrownBy(assertions.function("hogql_bitmap_to_array", "X'01'")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitmapOperations()
    {
        assertThat(assertions.function("hogql_bitmap_and_cardinality", "hogql_bitmap_build(ARRAY[BIGINT '1', 2])", "hogql_bitmap_build(ARRAY[BIGINT '2', 3])")).isEqualTo(1L);
        assertThat(assertions.function("hogql_bitmap_or_cardinality", "hogql_bitmap_build(ARRAY[BIGINT '1', 2])", "hogql_bitmap_build(ARRAY[BIGINT '2', 3])")).isEqualTo(3L);
        assertThat(assertions.function("hogql_bitmap_xor_cardinality", "hogql_bitmap_build(ARRAY[BIGINT '1', 2])", "hogql_bitmap_build(ARRAY[BIGINT '2', 3])")).isEqualTo(2L);
        assertThat(assertions.function("hogql_bitmap_andnot_cardinality", "hogql_bitmap_build(ARRAY[BIGINT '1', 2])", "hogql_bitmap_build(ARRAY[BIGINT '2', 3])")).isEqualTo(1L);
        assertThat(assertions.function("hogql_bitmap_to_array", "hogql_bitmap_subset_limit(hogql_bitmap_build(ARRAY[BIGINT '5', 1, 3]), 2, 1)"))
                .matches("ARRAY[BIGINT '3']");
        assertThat(assertions.function("hogql_bitmap_to_array", "hogql_sub_bitmap(hogql_bitmap_build(ARRAY[BIGINT '5', 1, 3]), 1, 1)"))
                .matches("ARRAY[BIGINT '3']");
    }

    @Test
    public void testBitmapTransformIsSimultaneous()
    {
        assertThat(assertions.function("hogql_bitmap_to_array", "hogql_bitmap_transform(hogql_bitmap_build(ARRAY[BIGINT '1', 2]), ARRAY[BIGINT '1', 2], ARRAY[BIGINT '2', 3])"))
                .matches("ARRAY[BIGINT '2', 3]");
        assertTrinoExceptionThrownBy(assertions.function("hogql_bitmap_transform", "hogql_bitmap_build(ARRAY[BIGINT '1'])", "ARRAY[BIGINT '1']", "ARRAY[BIGINT '2', 3]")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testBitmapAggregations()
    {
        assertThat(assertions.query("SELECT hogql_group_bitmap(value) FROM (VALUES BIGINT '1', 2, 1, NULL) input(value)"))
                .matches("VALUES BIGINT '2'");
        assertThat(assertions.query("SELECT hogql_bitmap_to_array(hogql_group_bitmap_state(value)) FROM (VALUES BIGINT '3', 1, 3) input(value)"))
                .matches("VALUES ARRAY[BIGINT '1', 3]");
        assertThat(assertions.query("SELECT hogql_group_bitmap(value) FROM (VALUES BIGINT '1') input(value) WHERE false"))
                .matches("VALUES BIGINT '0'");
        assertThat(assertions.query("SELECT hogql_bitmap_to_array(hogql_group_bitmap_state(value)) FROM (VALUES BIGINT '1') input(value) WHERE false"))
                .matches("VALUES CAST(ARRAY[] AS ARRAY(BIGINT))");
        assertThat(assertions.query("SELECT group_key, hogql_group_bitmap(value) FROM (VALUES (1, BIGINT '2'), (1, 2), (2, 3)) input(group_key, value) GROUP BY group_key"))
                .matches("VALUES (1, BIGINT '1'), (2, BIGINT '1')");
    }

    @Test
    public void testBitmapStateAggregations()
    {
        String input = " FROM (VALUES ARRAY[BIGINT '1', 2], ARRAY[BIGINT '2', 3]) input(items)";
        assertThat(assertions.query("SELECT hogql_group_bitmap_and(hogql_bitmap_build(items))" + input)).matches("VALUES BIGINT '1'");
        assertThat(assertions.query("SELECT hogql_group_bitmap_or(hogql_bitmap_build(items))" + input)).matches("VALUES BIGINT '3'");
        assertThat(assertions.query("SELECT hogql_group_bitmap_xor(hogql_bitmap_build(items))" + input)).matches("VALUES BIGINT '2'");
        assertThat(assertions.query("SELECT hogql_bitmap_to_array(hogql_group_bitmap_and_state(hogql_bitmap_build(items)))" + input))
                .matches("VALUES ARRAY[BIGINT '2']");
        assertThat(assertions.query("SELECT hogql_bitmap_to_array(hogql_group_bitmap_or_state(hogql_bitmap_build(items)))" + input))
                .matches("VALUES ARRAY[BIGINT '1', 2, 3]");
    }

    @Test
    public void testH3NativeLibrary()
    {
        assertThat(assertions.function("hogql_h3_is_valid", "hogql_geo_to_h3(37.775, -122.418, 9)")).isEqualTo(1L);
        assertThat(assertions.function("hogql_h3_get_resolution", "hogql_geo_to_h3(37.775, -122.418, 9)")).isEqualTo(9L);
        assertThat(assertions.function("hogql_h3_to_string", "hogql_string_to_h3('8928308280fffff')")).isEqualTo("8928308280fffff");
        assertThat(assertions.query("SELECT cardinality(hogql_h3_k_ring(hogql_geo_to_h3(37.775, -122.418, 9), 1))"))
                .matches("VALUES BIGINT '7'");
        assertThat(assertions.query("SELECT cardinality(hogql_h3_to_children(hogql_geo_to_h3(37.775, -122.418, 9), 10))"))
                .matches("VALUES BIGINT '7'");
        assertThat(assertions.query("SELECT cardinality(hogql_h3_to_geo_boundary(hogql_geo_to_h3(37.775, -122.418, 9)))"))
                .matches("VALUES BIGINT '6'");
    }

    @Test
    public void testH3AllocationLimits()
    {
        assertTrinoExceptionThrownBy(assertions.function("hogql_h3_k_ring", "hogql_geo_to_h3(37.775, -122.418, 9)", "1000000")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertTrinoExceptionThrownBy(assertions.function("hogql_h3_hex_ring", "hogql_geo_to_h3(37.775, -122.418, 9)", "-1")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertTrinoExceptionThrownBy(assertions.function("hogql_h3_to_children", "hogql_geo_to_h3(37.775, -122.418, 0)", "15")::evaluate)
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }
}
