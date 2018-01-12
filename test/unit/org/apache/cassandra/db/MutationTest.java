package org.apache.cassandra.db;

import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MutationTest
{
    private static final String KEYSPACE1 = "MutationTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static final CFMetaData FAKE_CFM = SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1);


    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1));
    }

    @Test
    public void testMutationOnUnknownTable() throws Exception
    {
        try
        {
            getMutationOnUnknownTable().apply();
            fail("Should have thrown exception");
        } catch (RuntimeException e)
        {
            assertEquals(String.format("Attempting to mutate non-existant table %s (%s.%s)",
                                       FAKE_CFM.cfId, FAKE_CFM.ksName, FAKE_CFM.cfName), e.getMessage());
        }
    }

    @Test
    public void testMutationOnUnknownTableAsync() throws Exception
    {
        try
        {
            getMutationOnUnknownTable().applyFuture().get();
            fail("Should have thrown exception");
        } catch (RuntimeException e)
        {
            assertEquals(String.format("Attempting to mutate non-existant table %s (%s.%s)",
                                       FAKE_CFM.cfId, FAKE_CFM.ksName, FAKE_CFM.cfName), e.getMessage());
        }
    }

    private Mutation getMutationOnUnknownTable()
    {
        Mutation mutation = new Mutation(KEYSPACE1, Util.dk("key1"));
        mutation.add(new RowUpdateBuilder(FAKE_CFM, System.currentTimeMillis(), bytes(0))
                             .clustering(bytes(0))
                             .add("val", bytes(0)).buildUpdate());
        return mutation;
    }
}
