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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExpressionVerifier
{
    private final SqlParser parser = new SqlParser();
    private final Metadata metadata = MetadataManager.createTestMetadataManager();
    private final TestingRowExpressionTranslator translator = new TestingRowExpressionTranslator(metadata);

    @Test
    public void test()
    {
        Expression actual = expression("NOT(orderkey = 3 AND custkey = 3 AND orderkey < 10)");

        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        assertTrue(verifier.process(actual, expression("NOT(X = 3 AND Y = 3 AND X < 10)")));
        assertThrows(() -> verifier.process(actual, expression("NOT(X = 3 AND Y = 3 AND Z < 10)")));
        assertFalse(verifier.process(actual, expression("NOT(X = 3 AND X = 3 AND X < 10)")));

        RowExpressionVerifier rowExpressionVerifier = new RowExpressionVerifier(symbolAliases, metadata, TEST_SESSION);

        assertTrue(rowExpressionVerifier.process(expression("NOT(X = 3 AND Y = 3 AND X < 10)"), translate(actual)));
        assertThrows(() -> rowExpressionVerifier.process(expression("NOT(X = 3 AND Y = 3 AND Z < 10)"), translate(actual)));
        assertFalse(rowExpressionVerifier.process(expression("NOT(X = 3 AND X = 3 AND X < 10)"), translate(actual)));
    }

    @Test
    public void testCast()
    {
        SymbolAliases aliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(aliases);
        assertTrue(verifier.process(expression("CAST('2' AS varchar)"), expression("CAST('2' AS varchar)")));
        assertFalse(verifier.process(expression("CAST('2' AS varchar)"), expression("CAST('2' AS bigint)")));
        assertTrue(verifier.process(expression("CAST(orderkey AS varchar)"), expression("CAST(X AS varchar)")));

        RowExpressionVerifier rowExpressionVerifier = new RowExpressionVerifier(aliases, metadata, TEST_SESSION);
        assertTrue(rowExpressionVerifier.process(expression("CAST('2' AS varchar)"), translate(expression("CAST('2' AS varchar)"))));
        assertFalse(rowExpressionVerifier.process(expression("CAST('2' AS bigint)"), translate(expression("CAST('2' AS varchar)"))));
        assertTrue(rowExpressionVerifier.process(expression("CAST(X AS varchar)"), translate(expression("CAST(orderkey AS varchar)"))));
    }

    @Test
    public void testBetween()
    {
        SymbolAliases symbolAliases = SymbolAliases.builder()
                .put("X", new SymbolReference("orderkey"))
                .put("Y", new SymbolReference("custkey"))
                .build();

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
        // Complete match
        assertTrue(verifier.process(expression("orderkey BETWEEN 1 AND 2"), expression("X BETWEEN 1 AND 2")));
        // Different value
        assertFalse(verifier.process(expression("orderkey BETWEEN 1 AND 2"), expression("Y BETWEEN 1 AND 2")));
        assertFalse(verifier.process(expression("custkey BETWEEN 1 AND 2"), expression("X BETWEEN 1 AND 2")));
        // Different min or max
        assertFalse(verifier.process(expression("orderkey BETWEEN 2 AND 4"), expression("X BETWEEN 1 AND 2")));
        assertFalse(verifier.process(expression("orderkey BETWEEN 1 AND 2"), expression("X BETWEEN '1' AND '2'")));
        assertFalse(verifier.process(expression("orderkey BETWEEN 1 AND 2"), expression("X BETWEEN 4 AND 7")));

        RowExpressionVerifier rowExpressionVerifier = new RowExpressionVerifier(symbolAliases, metadata, TEST_SESSION);
        assertTrue(rowExpressionVerifier.process(expression("X BETWEEN 1 AND 2"), translate(expression("orderkey BETWEEN 1 AND 2"))));
        // Different value
        assertFalse(rowExpressionVerifier.process(expression("Y BETWEEN 1 AND 2"), translate(expression("orderkey BETWEEN 1 AND 2"))));
        assertFalse(rowExpressionVerifier.process(expression("X BETWEEN 1 AND 2"), translate(expression("custkey BETWEEN 1 AND 2"))));
        // Different min or max
        assertFalse(rowExpressionVerifier.process(expression("X BETWEEN 1 AND 2"), translate(expression("orderkey BETWEEN 2 AND 4"))));
        assertFalse(rowExpressionVerifier.process(expression("X BETWEEN '1' AND '2'"), translate(expression("orderkey BETWEEN 1 AND 2"))));
        assertFalse(rowExpressionVerifier.process(expression("X BETWEEN 4 AND 7"), translate(expression("orderkey BETWEEN 1 AND 2"))));
    }

    private Expression expression(String sql)
    {
        return rewriteIdentifiersToSymbolReferences(parser.createExpression(sql));
    }

    private static void assertThrows(Runnable runnable)
    {
        try {
            runnable.run();
            throw new AssertionError("Method didn't throw exception as expected");
        }
        catch (Exception expected) {
        }
    }

    private RowExpression translate(Expression expression)
    {
        return translator.translate(expression, TypeProvider.copyOf(ImmutableMap.of(new Symbol("orderkey"), BIGINT, new Symbol("custkey"), BIGINT)));
    }
}
