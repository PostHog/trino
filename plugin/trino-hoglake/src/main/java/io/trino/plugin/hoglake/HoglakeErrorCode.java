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
package io.trino.plugin.hoglake;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.USER_ERROR;

/**
 * The connector's error taxonomy. EXTERNAL codes mean the hoglake
 * control plane (or the network to it) failed — not Trino, not the
 * connector — which is what operators and ErrorType-keyed retry
 * policies need to see. USER_ERROR codes mean the catalog
 * configuration or the query is wrong.
 */
public enum HoglakeErrorCode
        implements ErrorCodeSupplier
{
    /**
     * Control plane unreachable (connect/IO failure) or 5xx.
     */
    HOGLAKE_CATALOG_UNAVAILABLE(0, EXTERNAL),
    /**
     * 200 with an unparseable body, or a status outside the contract.
     */
    HOGLAKE_INVALID_RESPONSE(1, EXTERNAL),
    /**
     * 410 Gone: the pinned read snapshot fell below the catalog's
     * expiry floor mid-query. Re-run the query to plan at a retained
     * snapshot.
     */
    HOGLAKE_SNAPSHOT_EXPIRED(2, EXTERNAL),
    /**
     * The configured hoglake.catalog does not exist on the control plane.
     */
    HOGLAKE_CATALOG_NOT_FOUND(3, USER_ERROR);

    // Arbitrary plugin-private range; only uniqueness within this
    // connector matters (external plugins pick their own base).

    private static final int ERROR_CODE_BASE = 0x0512_0000;

    private final ErrorCode errorCode;

    HoglakeErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_BASE, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
