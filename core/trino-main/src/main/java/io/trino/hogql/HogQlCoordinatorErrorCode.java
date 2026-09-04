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
package io.trino.hogql;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;

public enum HogQlCoordinatorErrorCode
        implements ErrorCodeSupplier
{
    HOGQL_COMPILATION_QUEUE_FULL(0, INSUFFICIENT_RESOURCES),
    HOGQL_COMPILATION_TIMEOUT(1, INSUFFICIENT_RESOURCES),
    ;

    private final ErrorCode errorCode;

    HogQlCoordinatorErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0522_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
