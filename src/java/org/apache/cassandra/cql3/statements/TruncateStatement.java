/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.util.concurrent.TimeoutException;

import io.reactivex.Single;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;

public class TruncateStatement extends CFStatement implements CQLStatement
{
    public TruncateStatement(CFName name)
    {
        super(name);
    }

    public int getBoundTerms()
    {
        return 0;
    }

    public Prepared prepare() throws InvalidRequestException
    {
        return new Prepared(this);
    }

    public void checkAccess(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), CorePermission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        Schema.instance.validateTable(keyspace(), columnFamily());
    }

    public Single<? extends ResultMessage> execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), columnFamily());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            StorageProxy.truncateBlocking(keyspace(), columnFamily());
        }
        catch (UnavailableException | TimeoutException e)
        {
            throw new TruncateException(e);
        }

        return Single.just(new ResultMessage.Void());
    }

    public Single<? extends ResultMessage> executeInternal(QueryState state, QueryOptions options)
    {
        try
        {
            // TODO rx-ify
            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(columnFamily());
            cfs.truncateBlocking();
            return Single.just(new ResultMessage.Void());
        }
        catch (Exception e)
        {
            return Single.error(new TruncateException(e));
        }
    }
}
