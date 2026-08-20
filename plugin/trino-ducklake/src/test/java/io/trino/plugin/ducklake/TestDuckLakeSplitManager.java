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
package io.trino.plugin.ducklake;

import io.trino.plugin.ducklake.DuckLakeSplitManager.ByteRange;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.ducklake.DuckLakeSplitManager.byteRanges;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDuckLakeSplitManager
{
    @Test
    void testFileNotLargerThanTargetIsReadWhole()
    {
        assertThat(byteRanges(0, 1000)).containsExactly(new ByteRange(0, 0));
        assertThat(byteRanges(1, 1000)).containsExactly(new ByteRange(0, 1));
        assertThat(byteRanges(999, 1000)).containsExactly(new ByteRange(0, 999));
        assertThat(byteRanges(1000, 1000)).containsExactly(new ByteRange(0, 1000));
    }

    @Test
    void testFileSizeIsMultipleOfTarget()
    {
        assertThat(byteRanges(2000, 1000)).containsExactly(
                new ByteRange(0, 1000),
                new ByteRange(1000, 1000));
        assertThat(byteRanges(5000, 1000)).hasSize(5);
    }

    @Test
    void testLastRangeExtendsToEndOfFile()
    {
        assertThat(byteRanges(1001, 1000)).containsExactly(
                new ByteRange(0, 1000),
                new ByteRange(1000, 1));
        assertThat(byteRanges(2500, 1000)).containsExactly(
                new ByteRange(0, 1000),
                new ByteRange(1000, 1000),
                new ByteRange(2000, 500));
    }

    @Test
    void testRangesCoverFileExactlyOnce()
    {
        for (long fileSize : List.of(0L, 1L, 999L, 1000L, 1001L, 4096L, 100_000L)) {
            for (long maxSplitSize : List.of(1L, 7L, 1000L, 1024L, 64L * 1024 * 1024)) {
                List<ByteRange> ranges = byteRanges(fileSize, maxSplitSize);
                String description = "file size %s, max split size %s".formatted(fileSize, maxSplitSize);
                assertThat(ranges).as(description).isNotEmpty();
                long expectedStart = 0;
                for (ByteRange range : ranges) {
                    // contiguous: a gap would drop the rows of a row group, an overlap would duplicate them
                    assertThat(range.start()).as(description).isEqualTo(expectedStart);
                    assertThat(range.length()).as(description).isLessThanOrEqualTo(maxSplitSize);
                    expectedStart += range.length();
                }
                assertThat(expectedStart).as(description).isEqualTo(fileSize);
            }
        }
    }

    @Test
    void testInvalidArguments()
    {
        assertThatThrownBy(() -> byteRanges(-1, 1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fileSizeBytes is negative: -1");
        assertThatThrownBy(() -> byteRanges(1000, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("maxSplitSize is not positive: 0");
    }
}
