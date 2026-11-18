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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.posthog.functions.CodePointSet.MAX_CASED_CODE_POINT;
import static java.lang.Character.MAX_CODE_POINT;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCodePointSet
{
    @Test
    public void testNoCodePointAboveTheCaseScanBoundHasAnotherCase()
    {
        // The case folding table scans code points up to MAX_CASED_CODE_POINT only. Check all the rest
        // against the running JDK, so that a newer Unicode version that adds cased letters up there fails here.
        List<String> above = new ArrayList<>();
        for (int codePoint = MAX_CASED_CODE_POINT + 1; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (Character.toLowerCase(codePoint) != codePoint || Character.toUpperCase(codePoint) != codePoint || Character.toTitleCase(codePoint) != codePoint) {
                above.add("U+%X".formatted(codePoint));
            }
        }
        assertThat(above).isEmpty();
    }

    @Test
    public void testCaseVariants()
    {
        assertThat(CodePointSet.caseVariants('a')).isEqualTo(CodePointSet.ofRanges('A', 'A', 'a', 'a'));
        assertThat(CodePointSet.caseVariants('k')).isEqualTo(CodePointSet.ofRanges('K', 'K', 'k', 'k', 0x212A, 0x212A));
        assertThat(CodePointSet.caseVariants(0x212A)).isEqualTo(CodePointSet.caseVariants('k'));
        assertThat(CodePointSet.caseVariants('s')).isEqualTo(CodePointSet.ofRanges('S', 'S', 's', 's', 0x17F, 0x17F));
        assertThat(CodePointSet.caseVariants('1')).isEqualTo(CodePointSet.of('1'));
        // the Turkish dotted capital I and dotless small i fold only to themselves, as in Hyperscan
        assertThat(CodePointSet.caseVariants(0x130)).isEqualTo(CodePointSet.of(0x130));
        assertThat(CodePointSet.caseVariants(0x131)).isEqualTo(CodePointSet.of(0x131));
        // the highest letter with a case, in Adlam
        assertThat(CodePointSet.caseVariants(0x1E943)).isEqualTo(CodePointSet.ofRanges(0x1E921, 0x1E921, 0x1E943, 0x1E943));
    }

    @Test
    public void testCaseClosure()
    {
        assertThat(CodePointSet.ofRanges('a', 'c').caseClosure()).isEqualTo(CodePointSet.ofRanges('A', 'C', 'a', 'c'));
        assertThat(CodePointSet.ofRanges('0', '9').caseClosure()).isEqualTo(CodePointSet.ofRanges('0', '9'));
        assertThat(CodePointSet.ofRanges('k', 'k').containsCasedCodePoint()).isTrue();
        assertThat(CodePointSet.ofRanges('0', '9').containsCasedCodePoint()).isFalse();
    }
}
