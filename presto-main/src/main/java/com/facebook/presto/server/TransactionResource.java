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
package com.facebook.presto.server;

import com.facebook.presto.transaction.TransactionId;
import com.facebook.presto.transaction.TransactionInfo;
import com.facebook.presto.transaction.TransactionManager;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import java.util.List;

import static com.facebook.presto.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Path("/v1/transaction")
public class TransactionResource
{
    private final TransactionManager transactionManager;

    @Inject
    public TransactionResource(TransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @GET
    public Response getAllTransactions()
    {
        List<TransactionId> result = transactionManager.getAllTransactionInfos().stream().map(TransactionInfo::getTransactionId).collect(toList());
        return Response.ok(result).build();
    }

    @GET
    @Path("{transactionId}")
    public Response setActive(@PathParam("transactionId") String transactionId)
    {
        try {
            transactionManager.checkAndSetActive(TransactionId.valueOf(transactionId));
            return Response.noContent().build();
        }
        catch (Exception e) {
            return Response.ok(toFailure(e)).build();
        }
    }

    @DELETE
    @Path("{transactionId}")
    public Response setInactive(@PathParam("transactionId") String transactionId)
    {
        try {
            transactionManager.trySetInactive(TransactionId.valueOf(transactionId));
            return Response.noContent().build();
        }
        catch (Exception e) {
            return Response.ok(toFailure(e)).build();
        }
    }
}
