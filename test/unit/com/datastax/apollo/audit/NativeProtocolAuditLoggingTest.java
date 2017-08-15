/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.audit;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.junit.Test;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

import static com.datastax.apollo.audit.AuditLoggingTestSupport.assertAllEventsInSameBatch;
import static com.datastax.apollo.audit.AuditLoggingTestSupport.assertEventProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NativeProtocolAuditLoggingTest extends AbstractCql3AuditLoggingTest
{
    Cluster clusterCompressed = null;
    Cluster clusterNotCompressed = null;

    public Session initTestClient(boolean withCompression) throws Exception
    {
        Session session = sessionNet();

        if (withCompression)
        {
            if (clusterCompressed == null)
            {
                // must use Snappy compression as the LZ4 versions
                // from C* & java-driver are not compatible
                session.getCluster().getConfiguration().getProtocolOptions()
                       .setCompression(ProtocolOptions.Compression.SNAPPY);
            }
        }
        else
        {
            if (clusterNotCompressed == null)
            {
                // must use Snappy compression as the LZ4 versions
                // from C* & java-driver are not compatible
                session.getCluster().getConfiguration().getProtocolOptions()
                       .setCompression(ProtocolOptions.Compression.NONE);
            }
        }
        // This is extremely hacky, but we need some way to
        // make sure that all the connections in the session's
        // pool are initialized (i.e. have been used with the
        // keyspace in question). Otherwise, under the hood the
        // driver issues USE statements to set ks & these then
        // show up in the audit log
        for (int i = 0; i < 10; i++)
        {
            session.execute("USE " + ks);
        }

        InProcTestAuditWriter.reset();

        return session;
    }

    @Override
    void closeConnection()
    {
        clusterCompressed = null;
        clusterNotCompressed = null;
    }

    @Override
    void executeCql3Query(String cql, boolean withCompression)
    {
        try
        {
            Session session = initTestClient(withCompression);
            session.execute(cql);
        }
        catch(Exception e)
        {
            throw new RuntimeException("Error executing cql3 statement", e);
        }
    }

    @Override
    PreparedStmtHandle prepareCql3Query(String cql, boolean withCompression)
    {
        try
        {
            Session session = initTestClient(withCompression);
            return new NativeProtocolPreparedStmtHandle(session.prepare(cql));
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error preparing cql3 statement", e);
        }
    }

    @Override
    void executePreparedCql3Query(PreparedStmtHandle handle, List<Object> variables)
    {
        assertTrue(handle instanceof NativeProtocolPreparedStmtHandle);
        try
        {
            Session session = initTestClient(false);
            BoundStatement bound = ((NativeProtocolPreparedStmtHandle)handle).stmt.bind(variables.toArray());
            session.execute(bound);
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error executing prepared cql3 statement", e);
        }
    }

    @Test
    public void regularStatementWithVariablesIsLogged() throws Exception
    {
        Session session = initTestClient(false);
        session.execute("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;", 3, 72);
        Stack<AuditableEvent> events = getEvents();
        assertEquals(1, events.size());
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ; [k=3,v=72]");
    }

    @Test
    public void batchOfPreparedStatementsIsLogged() throws Exception
    {
        Session session = initTestClient(false);
        PreparedStatement ps1 = session.prepare("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;");
        PreparedStatement ps2 = session.prepare("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;");
        BatchStatement batch = new BatchStatement();
        batch.add(ps1.bind(1, 24)).add(ps2.bind(2, 48));

        session.execute(batch);

        Stack<AuditableEvent> events = getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ; [k=2,v=48]");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ; [k=1,v=24]");
    }

    @Test
    public void queryBuilderBatchIsLogged() throws Exception
    {
        Session session = initTestClient(false);
        Cluster cluster = clusterNotCompressed;

        List<Update> queries = new ArrayList<Update>();

        for (int i = 0; i < 2; i++)
        {
            Assignment inc = QueryBuilder.incr("count");
            Update countUpdate = QueryBuilder.update(QueryBuilder.quote(ccf));
            Update.Assignments incs = countUpdate.with(inc);
            Update.Where where = incs.where(QueryBuilder.eq(QueryBuilder.quote("column1"), "data1"));
            where.and(QueryBuilder.eq(QueryBuilder.quote("column2"), "data2"));
            queries.add(countUpdate);
        }

        Batch b = QueryBuilder.batch(queries.toArray(new Update[queries.size()]));
        session.execute(b);
        Stack<AuditableEvent> events = getEvents();
        assertEquals(2, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, ccf,
           "UPDATE Counters SET count = count + 1 WHERE column1 = ? AND column2 = ? ; [column1=data1,column2=data2]");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, ccf,
           "UPDATE Counters SET count = count + 1 WHERE column1 = ? AND column2 = ? ; [column1=data1,column2=data2]");
    }

    @Test
    public void mixedBatchIsLogged() throws Exception
    {
        Session session = initTestClient(false);
        PreparedStatement ps1 = session.prepare("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;");
        PreparedStatement ps2 = session.prepare("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;");
        BatchStatement batch = new BatchStatement();
        batch.add(ps1.bind(1, 24)).add(ps2.bind(2, 48));
        batch.add(new SimpleStatement("INSERT INTO \"Standard1\" (k, v) VALUES (3, 72) ;"));
        batch.add(new SimpleStatement("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;", 4, 96));

        session.execute(batch);

        Stack<AuditableEvent> events = getEvents();
        assertEquals(4, events.size());
        assertAllEventsInSameBatch(events);
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ; [bind variable values unavailable]");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (3, 72) ;");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ; [k=2,v=48]");
        assertEventProperties(events.pop(), AuditableEventType.CQL_UPDATE, ks, cf, "INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ; [k=1,v=24]");
    }

    @Test
    public void onlyFirstPageOfSelectQueryIsLogged() throws Exception
    {
        Session session = initTestClient(false);
        BatchStatement batch = new BatchStatement();
        PreparedStatement prepared = session.prepare("INSERT INTO \"Standard1\" (k, v) VALUES (?, ?) ;");
        for (int i=0;i<10;i++)
        {
            batch.add(prepared.bind(i,i));
        }
        session.execute(batch);

        InProcTestAuditWriter.reset();

        Statement paged = new SimpleStatement("SELECT * FROM \"Standard1\"");
        paged.setFetchSize(5);
        // iterate over the results to force the driver 
        // into executing additional queries
        session.execute(paged).all();
        
        Stack<AuditableEvent> events = getEvents();
        assertEquals(1, events.size());
        assertEventProperties(events.pop(), AuditableEventType.CQL_SELECT, ks, cf, "SELECT * FROM \"Standard1\"");
    }

    class NativeProtocolPreparedStmtHandle implements PreparedStmtHandle
    {
        public final PreparedStatement stmt;
        public NativeProtocolPreparedStmtHandle(PreparedStatement stmt)
        {
            this.stmt = stmt;
        }
    }
}
