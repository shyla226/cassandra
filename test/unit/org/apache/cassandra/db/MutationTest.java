package org.apache.cassandra.db;

import java.util.concurrent.ExecutionException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InternalRequestExecutionException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class MutationTest
{
    private static final String KEYSPACE1 = "MutationTest";
    private static final String CF_STANDARD1 = "Standard1";
    private static TableMetadata FAKE_CFM;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1,
                                    KeyspaceParams.simple(1));
        FAKE_CFM = SchemaLoader.standardCFMD(KEYSPACE1, CF_STANDARD1).build();
    }

    @Test
    public void testMutationOnUnknownTable() throws Exception
    {
        try
        {
            getMutationOnUnknownTable().apply();
            fail("Should have thrown exception");
        }
        catch (InternalRequestExecutionException e)
        {
            assertEquals(RequestFailureReason.UNKNOWN_TABLE, e.reason);
            assertEquals(String.format("Attempting to mutate non-existant table %s (%s.%s)",
                                       FAKE_CFM.id, FAKE_CFM.keyspace, FAKE_CFM.name), e.getMessage());
        }
    }

    @Test
    public void testMutationOnUnknownTableAsync() throws Exception
    {
        try
        {
            getMutationOnUnknownTable().applyFuture().get();
            fail("Should have thrown exception");
        }
        catch (ExecutionException e)
        {
            assertTrue(e.getCause() instanceof InternalRequestExecutionException);
            InternalRequestExecutionException iree = (InternalRequestExecutionException)e.getCause();
            assertEquals(RequestFailureReason.UNKNOWN_TABLE, iree.reason);
            assertEquals(String.format("Attempting to mutate non-existant table %s (%s.%s)",
                                       FAKE_CFM.id, FAKE_CFM.keyspace, FAKE_CFM.name), iree.getMessage());
        }
    }



    private static Mutation getMutationOnUnknownTable()
    {
        return new RowUpdateBuilder(FAKE_CFM, System.currentTimeMillis(), bytes(0))
               .clustering(bytes(0))
               .add("val", bytes(0)).build();
    }
}
