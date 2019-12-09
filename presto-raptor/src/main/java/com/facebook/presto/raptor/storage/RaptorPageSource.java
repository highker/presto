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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.OrcBatchRecordReader;
import com.facebook.presto.spi.ConnectorPageSource;

// TODO: implement delta read + normal read logic in this file
public class RaptorPageSource
        implements ConnectorPageSource
{
    private final OrcBatchRecordReader recordReader;

    /**
     * delegate to orc reader
     */
    public long getFileRowCount();

    /**
     * delegate to orc reader
     */
    public long getFilePosition();
}
