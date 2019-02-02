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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.spi.plan.Symbol.checkArgument;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SymbolAllocator
{
    private final Map<Symbol, Type> symbols;
    private int nextId;

    public SymbolAllocator()
    {
        symbols = new HashMap<>();
    }

    public SymbolAllocator(Map<Symbol, Type> initial)
    {
        symbols = new HashMap<>(initial);
    }

    public Symbol newSymbol(Symbol symbolHint)
    {
        checkArgument(symbols.containsKey(symbolHint), "symbolHint not in symbols map");
        return newSymbol(symbolHint.getName(), symbols.get(symbolHint));
    }

    public Symbol newSymbol(String nameHint, Type type)
    {
        return newSymbol(nameHint, type, null);
    }

    public Symbol newHashSymbol()
    {
        return newSymbol("$hashValue", BigintType.BIGINT);
    }

    public Symbol newSymbol(String nameHint, Type type, String suffix)
    {
        requireNonNull(nameHint, "name is null");
        requireNonNull(type, "type is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (tryParseInt(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        String unique = nameHint;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }

        String attempt = unique;
        while (symbols.containsKey(new Symbol(attempt))) {
            attempt = unique + "_" + nextId();
        }

        Symbol symbol = new Symbol(attempt);
        symbols.put(symbol, type);
        return symbol;
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(symbols);
    }

    private int nextId()
    {
        return nextId++;
    }

    private Integer tryParseInt(String value)
    {
        try {
            return Integer.parseInt(value);
        }
        catch (NumberFormatException e) {
            return null;
        }
    }
}
