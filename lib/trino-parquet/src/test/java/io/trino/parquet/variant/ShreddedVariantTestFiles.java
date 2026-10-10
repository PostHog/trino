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
package io.trino.parquet.variant;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.ERROR;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.MULTIPLE_ROWS;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.NO_FILES;
import static io.trino.parquet.variant.ShreddedVariantTestFiles.CaseKind.SINGLE_ROW;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/// The shredded VARIANT fixtures under `variant/shredded`. See the README in that directory.
final class ShreddedVariantTestFiles
{
    static final Path PARQUET_TESTING = resourceDirectory("variant/shredded/parquet-testing");
    static final Path DUCKDB = resourceDirectory("variant/shredded/duckdb");

    static final List<String> DUCKDB_FIXTURES = ImmutableList.of("arrays", "case-variant-keys", "nulls", "objects", "wide-object", "wide-object-unshredded");
    // Larger files in the shape of event properties, without a read-back by DuckDB
    static final List<String> DUCKDB_PROPERTIES_FIXTURES = ImmutableList.of("properties-shape", "properties-shape-mixed");

    private ShreddedVariantTestFiles() {}

    static List<ShreddedVariantCase> loadParquetTestingCases()
    {
        List<?> cases = parseJson(PARQUET_TESTING.resolve("cases.json"), List.class);
        return cases.stream()
                .map(json -> ShreddedVariantCase.fromJson((Map<?, ?>) json))
                .collect(toImmutableList());
    }

    private static Path resourceDirectory(String name)
    {
        try {
            return Path.of(Resources.getResource(name).toURI());
        }
        catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    enum CaseKind
    {
        SINGLE_ROW,
        MULTIPLE_ROWS,
        ERROR,
        NO_FILES,
    }

    record ShreddedVariantCase(
            int caseNumber,
            CaseKind kind,
            Optional<String> parquetFile,
            List<Optional<String>> variantFiles,
            boolean invalid)
    {
        ShreddedVariantCase
        {
            requireNonNull(kind, "kind is null");
            requireNonNull(parquetFile, "parquetFile is null");
            variantFiles = ImmutableList.copyOf(variantFiles);
        }

        public static ShreddedVariantCase fromJson(Map<?, ?> json)
        {
            int caseNumber = ((Number) json.get("case_number")).intValue();
            Optional<String> parquetFile = Optional.ofNullable((String) json.get("parquet_file"));
            Set<String> keys = json.keySet().stream()
                    .map(String.class::cast)
                    .collect(toImmutableSet());
            boolean invalid = keys.contains("notes");

            if (keys.contains("error_message")) {
                assertThat(keys).as("case %s", caseNumber).doesNotContain("variant_file", "variant_files");
                assertThat(parquetFile).as("case %s", caseNumber).isPresent();
                return new ShreddedVariantCase(caseNumber, ERROR, parquetFile, ImmutableList.of(), invalid);
            }
            if (keys.contains("variant_file")) {
                assertThat(keys).as("case %s", caseNumber).contains("variant").doesNotContain("variant_files");
                assertThat(parquetFile).as("case %s", caseNumber).isPresent();
                List<Optional<String>> variantFiles = ImmutableList.of(Optional.of((String) json.get("variant_file")));
                return new ShreddedVariantCase(caseNumber, SINGLE_ROW, parquetFile, variantFiles, invalid);
            }
            if (keys.contains("variant_files")) {
                assertThat(keys).as("case %s", caseNumber).contains("variants");
                assertThat(parquetFile).as("case %s", caseNumber).isPresent();
                List<Optional<String>> variantFiles = ((List<?>) json.get("variant_files")).stream()
                        .map(file -> Optional.ofNullable((String) file))
                        .collect(toImmutableList());
                return new ShreddedVariantCase(caseNumber, MULTIPLE_ROWS, parquetFile, variantFiles, invalid);
            }
            assertThat(keys).as("case %s", caseNumber).containsExactly("case_number");
            return new ShreddedVariantCase(caseNumber, NO_FILES, Optional.empty(), ImmutableList.of(), invalid);
        }
    }
}
