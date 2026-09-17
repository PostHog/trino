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

import io.airlift.slice.SizeOf;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Optional;
import java.util.Set;

final class TestingDeletionVectorMemory
{
    private TestingDeletionVectorMemory() {}

    static long retainedBytes(Object object)
            throws ReflectiveOperationException
    {
        return retainedBytes(object, Collections.newSetFromMap(new IdentityHashMap<>()));
    }

    private static long retainedBytes(Object object, Set<Object> seen)
            throws ReflectiveOperationException
    {
        if (object == null || !seen.add(object)) {
            return 0;
        }
        switch (object) {
            case byte[] array -> {
                return SizeOf.sizeOf(array);
            }
            case char[] array -> {
                return SizeOf.sizeOf(array);
            }
            case int[] array -> {
                return SizeOf.sizeOf(array);
            }
            case long[] array -> {
                return SizeOf.sizeOf(array);
            }
            case boolean[] array -> {
                return SizeOf.sizeOf(array);
            }
            case String value -> {
                return SizeOf.estimatedSizeOf(value);
            }
            case Optional<?> value -> {
                return SizeOf.instanceSize(Optional.class) + retainedBytes(value.orElse(null), seen);
            }
            case Object[] array -> {
                long bytes = SizeOf.sizeOf(array);
                for (Object value : array) {
                    bytes += retainedBytes(value, seen);
                }
                return bytes;
            }
            default -> {}
        }
        long bytes = SizeOf.instanceSize(object.getClass());
        for (Class<?> type = object.getClass(); type != Object.class; type = type.getSuperclass()) {
            for (Field field : type.getDeclaredFields()) {
                if (!Modifier.isStatic(field.getModifiers()) && !field.getType().isPrimitive()) {
                    field.setAccessible(true);
                    bytes += retainedBytes(field.get(object), seen);
                }
            }
        }
        return bytes;
    }
}
