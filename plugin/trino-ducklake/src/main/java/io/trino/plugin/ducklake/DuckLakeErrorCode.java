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

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

public enum DuckLakeErrorCode
        implements ErrorCodeSupplier
{
    DUCKLAKE_METASTORE_ERROR(0, EXTERNAL),
    DUCKLAKE_INVALID_METADATA(1, EXTERNAL),
    DUCKLAKE_UNSUPPORTED_FORMAT_VERSION(2, EXTERNAL),
    DUCKLAKE_UNSUPPORTED_TYPE(3, USER_ERROR),
    DUCKLAKE_UNSUPPORTED_FEATURE(4, USER_ERROR),
    DUCKLAKE_BAD_DATA(5, EXTERNAL),
    DUCKLAKE_FILESYSTEM_ERROR(6, EXTERNAL),
    DUCKLAKE_COMMIT_FAILED(7, EXTERNAL),
    DUCKLAKE_WRITER_ERROR(8, EXTERNAL),
    DUCKLAKE_TOO_MANY_OPEN_PARTITIONS(9, USER_ERROR),
    DUCKLAKE_COMMIT_CONFLICT(10, EXTERNAL),
    DUCKLAKE_UNSUPPORTED_CHANGE_TYPE(11, EXTERNAL),
    /**/;

    private final ErrorCode errorCode;

    DuckLakeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0517_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
