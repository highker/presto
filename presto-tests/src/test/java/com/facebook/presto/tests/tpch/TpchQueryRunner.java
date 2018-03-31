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
package com.facebook.presto.tests.tpch;

import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;

import static com.google.common.base.Verify.verify;

public final class TpchQueryRunner
{
    private TpchQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        boolean dispatcherEnabled = true;

        Logging.initialize();
        DistributedQueryRunner queryRunner;
        if (dispatcherEnabled) {
            queryRunner = TpchQueryRunnerBuilder.builder()
                    .setExtraProperties(ImmutableMap.of("http-server.http.port", "8081", "dispatcher.http.port", "8080"))
                    .build();
        }
        else {
            queryRunner = TpchQueryRunnerBuilder.builder()
                    .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                    .build();
        }
        Thread.sleep(10);
        Logger log = Logger.get(TpchQueryRunner.class);
        log.info("======== SERVER STARTED ========");

        if (dispatcherEnabled) {
            verify(queryRunner.getDispatcher().isPresent());
            log.info("\n====\ndispatcher:\t\t%s\ncoordinator:\t%s\n====", queryRunner.getDispatcher().get().getBaseUrl(), queryRunner.getCoordinator().getBaseUrl());
        }
        else {
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
