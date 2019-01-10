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
package com.facebook.presto;

public class DeoptimizationStorm
{
    private DeoptimizationStorm() {}

    static int someComplicatedFunction(double x)
    {
        return (int) (Math.pow(x, x) + Math.log(x) + Math.sqrt(x) * Math.tanh(x));
    }

    static void hotMethod(int iteration)
    {
        if (iteration < 20) {
            someComplicatedFunction(1.1);
        }
        else if (iteration < 40) {
            // uncommon trap will be triggered if PerMethodRecompilationCutoff=1
            someComplicatedFunction(1.1);
        }
        else if (iteration < 60) {
            // uncommon trap will be triggered if PerMethodRecompilationCutoff=2
            someComplicatedFunction(1.1);
        }
        else {
            // uncommon trap will be triggered if PerMethodRecompilationCutoff=3
            someComplicatedFunction(1.1);
        }
    }

    static void hotMethodWrapper(int iteration)
    {
        int count = 100_000;
        for (int i = 0; i < count; i++) {
            hotMethod(iteration);
        }
    }

    public static void main(String[] args)
    {
        // -ea -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation -XX:+PrintCompilation -XX:PerMethodRecompilationCutoff=1
        for (int k = 0; k < 100; k++) {
            long start = System.nanoTime();
            hotMethodWrapper(k + 1);
            System.out.println(k + " total:\t" + (System.nanoTime() - start));
        }
    }
}
