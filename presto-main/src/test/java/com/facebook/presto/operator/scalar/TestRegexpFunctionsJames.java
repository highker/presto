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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.type.JoniRegexp;
import com.facebook.presto.type.Re2JRegexp;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRegexpFunctionsJames
        extends AbstractTestFunctions
{
    private SecureRandom random = new SecureRandom();

    @Test
    public void testest()
    {
        List<String> patterns = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader("/Users/jamessun/Desktop/pattern.txt"));
            String line = br.readLine();

            while (line != null) {
                patterns.add(line);
                line = br.readLine();
            }
        }
        catch (Exception e) {
        }

        for (String pattern : patterns) {
            try {
                new Re2JRegexp(100000000, 2, Slices.utf8Slice(pattern));
            }
            catch (Exception e1) {
                System.out.println("RE2J: " + pattern);
                e1.printStackTrace();
                try {
                    new JoniRegexp(Slices.utf8Slice(pattern));
                }
                catch (Exception e2) {
                    System.out.println("Joni: " + pattern);
                }
            }
        }
    }

    @Test
    public void testest1()
    {
        try {
            BufferedReader br = new BufferedReader(new FileReader("/Users/jamessun/Desktop/query.txt"));
            String query;
            do {
                String ns = br.readLine();
                String id = br.readLine();
                query = br.readLine();
                if (ns == null) {
                    continue;
                }
                System.out.println("echo '" + id + "' >> time.txt");
                System.out.println();
                System.out.println(
                        "/usr/bin/time -f '%E real, %U user, %S sys' -a -o time.txt java -jar /usr/local/fbprojects/packages/presto.cli/latest/bin/presto-executable.jar --server $(smcc ls presto.stability.coordinator.http) --catalog prism --schema " + ns +
                        " --execute \"" + query.replace("\"", "") + "\"");
                System.out.println("echo '' >> time.txt");
                //System.out.println(query);
                System.out.println();
            } while (query != null);
        }
        catch (Exception e) {
        }
    }

    @Test
    public void testest2()
    {
        try {
            BufferedReader br1 = new BufferedReader(new FileReader("/Users/jamessun/Desktop/time_joni.txt"));
            String time;
            Map<String, int[]> map = new HashMap<>();
            do {
                String id = br1.readLine();
                time = br1.readLine();
                br1.readLine();
                if (time == null) {
                    continue;
                }
                String[] t = time.split("real")[0].trim().split(":");
                map.put(id, new int[2]);
                map.get(id)[0] = (int) (Double.parseDouble(t[0]) * 60 + Double.parseDouble(t[1]));
            } while (time != null);

            br1 = new BufferedReader(new FileReader("/Users/jamessun/Desktop/re2j_time.txt"));
            do {
                String id = br1.readLine();
                time = br1.readLine();
                br1.readLine();
                if (time == null) {
                    continue;
                }
                String[] t = time.split("real")[0].trim().split(":");
                map.get(id)[1] = (int) (Double.parseDouble(t[0]) * 60 + Double.parseDouble(t[1]));
            } while (time != null);

            for (String key : map.keySet()) {
                double l = (double) map.get(key)[0];
                double r = (double) map.get(key)[1];
                if (r == 0) {
                    continue;
                }

                System.out.print(key + ",");
                System.out.println(map.get(key)[0] + "," + map.get(key)[1]);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
