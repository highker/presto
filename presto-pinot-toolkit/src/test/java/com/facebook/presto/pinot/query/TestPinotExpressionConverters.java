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
package com.facebook.presto.pinot.query;

import com.facebook.presto.pinot.PinotException;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.testng.annotations.Test;

import java.util.function.Function;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_EXPRESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPinotExpressionConverters
        extends TestPinotQueryBase
{
    private final Function<VariableReferenceExpression, PinotQueryGeneratorContext.Selection> testInputFunction = (var) -> testInput.get(var);

    @Test
    public void testProjectExpressionConverter()
    {
        SessionHolder sessionHolder = new SessionHolder(false);
        // functions
        testProject("date_trunc('hour', from_unixtime(secondssinceepoch))",
                "dateTimeConvert(secondsSinceEpoch, '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')", sessionHolder);

        // arithmetic
        testProject("regionid + 1", "ADD(regionId, 1)", sessionHolder);
        testProject("regionid - 1", "SUB(regionId, 1)", sessionHolder);
        testProject("1 * regionid", "MULT(1, regionId)", sessionHolder);
        testProject("1 / regionid", "DIV(1, regionId)", sessionHolder);

        // TODO ... this one is failing
        testProject("secondssinceepoch + 1559978258.674", "ADD(secondsSinceEpoch, 1559978258.674)", sessionHolder);

        testProject("secondssinceepoch + 1559978258", "ADD(secondsSinceEpoch, 1559978258)", sessionHolder);

        testProjectUnsupported("secondssinceepoch > 0", sessionHolder);

        testProject("date_trunc('hour', from_unixtime(secondssinceepoch + 2))",
                "dateTimeConvert(ADD(secondsSinceEpoch, 2), '1:SECONDS:EPOCH', '1:MILLISECONDS:EPOCH', '1:HOURS')", sessionHolder);
    }

    @Test
    public void testAdhoc()
    {
        SessionHolder sessionHolder = new SessionHolder(false);

        testProject("secondssinceepoch + 1559978258.674", "ADD(secondsSinceEpoch, 1559978258.674)", sessionHolder);
    }

    @Test
    public void testPrestoDateTruncConversion()
    {
        SessionHolder sessionHolder = new SessionHolder(true);
        testProject("date_trunc('hour', from_unixtime(secondssinceepoch + 2))",
                "prestoDateTrunc(ADD(secondsSinceEpoch, 2),seconds, UTC, hour)", sessionHolder);

        testProject("date_trunc('hour', from_unixtime(secondssinceepoch + 2, 'America/New_York'))",
                "prestoDateTrunc(ADD(secondsSinceEpoch, 2),seconds, America/New_York, hour)", sessionHolder);
    }

    @Test
    public void testFilterExpressionConverter()
    {
        SessionHolder sessionHolder = new SessionHolder(false);

        // Simple comparisons
        testFilter("regionid = 20", "(regionId = 20)", sessionHolder);
        testFilter("regionid >= 20", "(regionId >= 20)", sessionHolder);
        testFilter("city = 'Campbell'", "(city = 'Campbell')", sessionHolder);

        // between
        testFilter("totalfare between 20 and 30", "((fare + trip) BETWEEN 20 AND 30)", sessionHolder);

        // in, not in
        testFilter("regionid in (20, 30, 40)", "(regionId IN (20, 30, 40))", sessionHolder);
        testFilter("regionid not in (20, 30, 40)", "(regionId NOT IN (20, 30, 40))", sessionHolder);
        testFilter("city in ('San Jose', 'Campbell', 'Union City')", "(city IN ('San Jose', 'Campbell', 'Union City'))", sessionHolder);
        testFilter("city not in ('San Jose', 'Campbell', 'Union City')", "(city NOT IN ('San Jose', 'Campbell', 'Union City'))", sessionHolder);
        testFilterUnsupported("secondssinceepoch + 1 in (234, 24324)", sessionHolder);
        testFilterUnsupported("NOT (secondssinceepoch = 2323)", sessionHolder);

        // combinations
        testFilter("totalfare between 20 and 30 AND regionid > 20 OR city = 'Campbell'",
                "((((fare + trip) BETWEEN 20 AND 30) AND (regionId > 20)) OR (city = 'Campbell'))", sessionHolder);

        testFilter("secondssinceepoch > 1559978258", "(secondsSinceEpoch > 1559978258)", sessionHolder);
    }

    private void testProject(String sqlExpression, String expectedPinotExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualPinotExpression = pushDownExpression.accept(new PinotProjectExpressionConverter(
                        typeManager,
                        functionMetadataManager,
                        standardFunctionResolution,
                        sessionHolder.getConnectorSession()),
                testInput).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    private void testProjectUnsupported(String sqlExpression, SessionHolder sessionHolder)
    {
        try {
            RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
            String actualPinotExpression = pushDownExpression.accept(new PinotProjectExpressionConverter(
                            typeManager,
                            functionMetadataManager,
                            standardFunctionResolution,
                            sessionHolder.getConnectorSession()),
                    testInput).getDefinition();
            fail("expected to not reach here: Generated " + actualPinotExpression);
        }
        catch (PinotException e) {
            assertEquals(e.getErrorCode(), PINOT_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }

    private void testFilter(String sqlExpression, String expectedPinotExpression, SessionHolder sessionHolder)
    {
        RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
        String actualPinotExpression = pushDownExpression.accept(new PinotFilterExpressionConverter(
                        typeManager,
                        functionMetadataManager,
                        standardFunctionResolution),
                testInputFunction).getDefinition();
        assertEquals(actualPinotExpression, expectedPinotExpression);
    }

    private void testFilterUnsupported(String sqlExpression, SessionHolder sessionHolder)
    {
        try {
            RowExpression pushDownExpression = getRowExpression(sqlExpression, sessionHolder);
            String actualPinotExpression = pushDownExpression.accept(new PinotFilterExpressionConverter(
                            typeManager,
                            functionMetadataManager,
                            standardFunctionResolution),
                    testInputFunction).getDefinition();
            fail("expected to not reach here: Generated " + actualPinotExpression);
        }
        catch (PinotException e) {
            assertEquals(e.getErrorCode(), PINOT_UNSUPPORTED_EXPRESSION.toErrorCode());
        }
    }
}
