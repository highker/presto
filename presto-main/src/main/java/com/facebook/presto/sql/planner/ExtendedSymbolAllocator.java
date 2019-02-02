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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.plan.Symbol;
import com.facebook.presto.spi.plan.SymbolAllocator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Map;

public class ExtendedSymbolAllocator
        extends SymbolAllocator
{
    public ExtendedSymbolAllocator()
    {
        super();
    }

    public ExtendedSymbolAllocator(Map<Symbol, Type> initial)
    {
        super(initial);
    }

    public Symbol newSymbol(Field field)
    {
        String nameHint = field.getName().orElse("field");
        return newSymbol(nameHint, field.getType());
    }

    public Symbol newSymbol(Expression expression, Type type)
    {
        return newSymbol(expression, type, null);
    }

    public Symbol newSymbol(Expression expression, Type type, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof Identifier) {
            nameHint = ((Identifier) expression).getValue();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix();
        }
        else if (expression instanceof SymbolReference) {
            nameHint = ((SymbolReference) expression).getName();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
        }

        return newSymbol(nameHint, type, suffix);
    }
}
