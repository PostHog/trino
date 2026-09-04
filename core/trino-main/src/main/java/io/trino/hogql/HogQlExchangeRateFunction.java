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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.annotation.UsedByGeneratedCode;
import io.trino.metadata.SqlScalarFunction;
import io.trino.operator.scalar.ChoicesSpecializedSqlScalarFunction;
import io.trino.operator.scalar.SpecializedSqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.Int128;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Optional;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.util.Reflection.constructorMethodHandle;
import static io.trino.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public final class HogQlExchangeRateFunction
        extends SqlScalarFunction
{
    public static final String NAME = "hogql_convert_currency";
    private static final int DECIMAL_SCALE = 10;
    private static final java.lang.invoke.MethodHandle METHOD_HANDLE = methodHandle(
            State.class,
            "convert",
            long.class,
            Slice.class,
            Slice.class,
            Int128.class,
            long.class);

    private final HogQlExchangeRateManager manager;

    public HogQlExchangeRateFunction(HogQlExchangeRateManager manager)
    {
        super(FunctionMetadata.scalarBuilder(NAME)
                .signature(Signature.builder()
                        .returnType(createDecimalType(38, DECIMAL_SCALE))
                        .argumentType(BIGINT)
                        .argumentType(VARCHAR)
                        .argumentType(VARCHAR)
                        .argumentType(createDecimalType(38, DECIMAL_SCALE))
                        .argumentType(DATE)
                        .build())
                .hidden()
                .description("Convert a decimal amount using a pinned HogQL exchange-rate generation")
                .build());
        this.manager = requireNonNull(manager, "manager is null");
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        java.lang.invoke.MethodHandle instanceFactory = constructorMethodHandle(State.class, HogQlExchangeRateManager.class).bindTo(manager);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL, NEVER_NULL, NEVER_NULL, NEVER_NULL, NEVER_NULL),
                METHOD_HANDLE,
                Optional.of(instanceFactory));
    }

    public static final class State
    {
        private final HogQlExchangeRateManager manager;
        private long generation = -1;
        private HogQlExchangeRateConversionEngine engine;

        public State(HogQlExchangeRateManager manager)
        {
            this.manager = requireNonNull(manager, "manager is null");
        }

        @UsedByGeneratedCode
        public Int128 convert(long generation, Slice sourceCurrency, Slice targetCurrency, Int128 amount, long effectiveDate)
        {
            if (engine == null || this.generation != generation) {
                engine = manager.engine(generation);
                this.generation = generation;
            }
            BigDecimal decimalAmount = new BigDecimal(amount.toBigInteger(), DECIMAL_SCALE);
            BigDecimal converted = engine.convert(
                    sourceCurrency.toStringUtf8(),
                    targetCurrency.toStringUtf8(),
                    decimalAmount,
                    LocalDate.ofEpochDay(effectiveDate));
            return encodeScaledValue(converted, DECIMAL_SCALE);
        }
    }
}
