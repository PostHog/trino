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
package io.trino.hogql.compiler.catalog;

import io.trino.hogql.compiler.HogQlErrorCode;
import io.trino.spi.TrinoException;

import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_CATALOG_GENERATION_MISMATCH;
import static io.trino.hogql.compiler.HogQlErrorCode.HOGQL_CATALOG_NOT_READY;
import static java.util.Objects.requireNonNull;

public final class HogQlExchangeRateException
        extends TrinoException
{
    private final Failure failure;

    public HogQlExchangeRateException(Failure failure, String message)
    {
        super(errorCode(requireNonNull(failure, "failure is null")), message);
        this.failure = failure;
    }

    public Failure failure()
    {
        return failure;
    }

    private static HogQlErrorCode errorCode(Failure failure)
    {
        return switch (failure) {
            case UNAVAILABLE -> HOGQL_CATALOG_NOT_READY;
            case GENERATION_MISMATCH -> HOGQL_CATALOG_GENERATION_MISMATCH;
        };
    }

    public enum Failure
    {
        UNAVAILABLE,
        GENERATION_MISMATCH,
    }
}
