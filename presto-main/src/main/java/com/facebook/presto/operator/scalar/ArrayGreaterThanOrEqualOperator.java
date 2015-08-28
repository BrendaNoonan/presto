package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.GREATER_THAN;
import static com.facebook.presto.metadata.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.TypeUtils.readNativeValue;
import static com.facebook.presto.type.ArrayType.ARRAY_NULL_ELEMENT_MSG;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayGreaterThanOrEqualOperator
        extends SqlOperator
{
    public static final ArrayGreaterThanOrEqualOperator ARRAY_GREATER_THAN_OR_EQUAL = new ArrayGreaterThanOrEqualOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayGreaterThanOrEqualOperator.class, "greaterThanOrEqual", MethodHandle.class, Type.class, Block.class, Block.class);

    private ArrayGreaterThanOrEqualOperator()
    {
        super(GREATER_THAN_OR_EQUAL, ImmutableList.of(orderableTypeParameter("T")), StandardTypes.BOOLEAN, ImmutableList.of("array<T>", "array<T>"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, List<TypeSignature> parameterTypes, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = types.get("T");
        MethodHandle greaterThanFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(GREATER_THAN, BOOLEAN, ImmutableList.of(elementType, elementType))).getMethodHandle();
        MethodHandle method = METHOD_HANDLE.bindTo(greaterThanFunction).bindTo(elementType);
        return new ScalarFunctionImplementation(false, ImmutableList.of(false, false), method, isDeterministic());
    }

    public static boolean greaterThanOrEqual(MethodHandle greaterThanFunction, Type type, Block leftArray, Block rightArray)
    {
        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        int index = 0;
        while (index < len) {
            checkElementNotNull(leftArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            Object leftElement = readNativeValue(type, leftArray, index);
            Object rightElement = readNativeValue(type, rightArray, index);
            try {
                if ((boolean) greaterThanFunction.invoke(leftElement, rightElement)) {
                    return true;
                }
                if ((boolean) greaterThanFunction.invoke(rightElement, leftElement)) {
                    return false;
                }
            }
            catch (Throwable t) {
                Throwables.propagateIfInstanceOf(t, Error.class);
                Throwables.propagateIfInstanceOf(t, PrestoException.class);

                throw new PrestoException(INTERNAL_ERROR, t);
            }
            index++;
        }

        return leftArray.getPositionCount() >= rightArray.getPositionCount();
    }
}
