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
package org.apache.cassandra.cql3;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class BatchTests
{
    private static EmbeddedCassandraService cassandra;

    private static Cluster cluster;
    private static Session session;

    private static PreparedStatement counter;
    private static PreparedStatement noncounter;

    @BeforeClass()
    public static void setup() throws ConfigurationException, IOException
    {
        cassandra = new EmbeddedCassandraService();
        cassandra.start();

        cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute("drop keyspace if exists junit;");
        session.execute("create keyspace junit WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE junit.noncounter (\n" +
                "  id int PRIMARY KEY,\n" +
                "  val text\n" +
                ");");
        session.execute("CREATE TABLE junit.counter (\n" +
                "  id int PRIMARY KEY,\n" +
                "  val counter,\n" +
                ");");


        noncounter = session.prepare("insert into junit.noncounter(id, val)values(?,?)");
        counter = session.prepare("update junit.counter set val = val + ? where id = ?");
    }


    static Object[] data = new Object[] {
     1501172613354001L, true, 1785505920, 0, -1, UUID.fromString("ffffffff-ffff-ffff-0000-000000000000"),
     1501172613354001L, "1", 1785505920, 0, 32771, UUID.fromString("00000000-0000-8003-0000-000000000000"),
     1501172613354001L, 1, 1785505920, 0, 32772, UUID.fromString("00000000-0000-8004-0000-000000000000"),
     1501172613354001L, "string1_1", 1785505920, 0, 32773, UUID.fromString("00000000-0000-8005-0000-000000000000"),
     1501172613354001L, "string1_1", 1785505920, 0, 32774, UUID.fromString("00000000-0000-8006-0000-000000000000"),
     1501172613354001L, 1, 1785505920, 0, 32775, UUID.fromString("00000000-0000-8007-0000-000000000000") ,
     1501172613354001L, 1, 1785505920, 0, 32776, UUID.fromString("00000000-0000-8008-0000-000000000000") ,
     1501172613354001L, 1, 1785505920, 0, 32777, UUID.fromString("00000000-0000-8009-0000-000000000000") ,
     1501172613354001L, 1, 1785505920, 0, 32778, UUID.fromString("00000000-0000-800a-0000-000000000000") ,
     1501172613354001L, 1.0, 1785505920, 0, 32779, UUID.fromString("00000000-0000-800b-0000-000000000000") ,
     1501172613354001L, 1.0, 1785505920, 0, 32780, UUID.fromString("00000000-0000-800c-0000-000000000000") ,
     1501172613354001L, false, 1785505920, 0, 32781, UUID.fromString("00000000-0000-800d-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32782, UUID.fromString("00000000-0000-800e-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), 1785505920, 0, 32783, UUID.fromString("00000000-0000-800f-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32784, UUID.fromString("00000000-0000-8010-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32785, UUID.fromString("00000000-0000-8011-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-02"), 1785505920, 0, 32786, UUID.fromString("00000000-0000-8012-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-02-02"), 1785505920, 0, 32787, UUID.fromString("00000000-0000-8013-0000-000000000000") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.1"), 1785505920, 0, 32788, UUID.fromString("00000000-0000-8014-0000-000000000000") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000001"), 1785505920, 0, 32789, UUID.fromString("00000000-0000-8015-0000-000000000000") ,
     1501172613354001L, 1, 1785505920, 0, 32790, UUID.fromString("00000000-0000-8016-0000-000000000000") ,
     1501172613354001L, 1, 1785505920, 0, 32791, UUID.fromString("00000000-0000-8017-0000-000000000000") ,
     1501172613354001L, "test101 full test", 1785505920, 0, 32794, UUID.fromString("00000000-0000-801a-0000-000000000000") ,
     1501172613354001L,
                  "test101 full test", 1785505920, 0, 32795, UUID.fromString("00000000-0000-801b-0000-000000000000") ,
     1501172613354001L, "string1_1", 1785505920, 0, 32796, UUID.fromString("e352b670-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "string1_1", 1785505920, 0, 32797, UUID.fromString("e352b671-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32798, UUID.fromString("e352b672-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32799, UUID.fromString("e352b673-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32800, UUID.fromString("e352b674-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32801, UUID.fromString("e352dd80-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1.0, 1785505920, 0, 32802, UUID.fromString("e352dd81-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1.0, 1785505920, 0, 32803, UUID.fromString("e352dd82-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, false, 1785505920, 0, 32804, UUID.fromString("e352dd83-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32805, UUID.fromString("e352dd84-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), 1785505920, 0, 32806, UUID.fromString("e352dd85-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32807, UUID.fromString("e352dd86-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32808, UUID.fromString("e352dd87-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-02"), 1785505920, 0, 32809, UUID.fromString("e352dd88-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-02-02"), 1785505920, 0, 32810, UUID.fromString("e352dd89-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.1"), 1785505920, 0, 32811, UUID.fromString("e352dd8a-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000001"), 1785505920, 0, 32812, UUID.fromString("e3530490-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32813, UUID.fromString("e3530491-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32814, UUID.fromString("e3530492-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "test101 full test", 1785505920, 0, 32817, UUID.fromString("e353eef0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "test101 full test", 1785505920, 0, 32818, UUID.fromString("e353eef1-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "string1_1", 1785505920, 0, 32796, UUID.fromString("e353eef2-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "string1_1", 1785505920, 0, 32797, UUID.fromString("e353eef3-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32798, UUID.fromString("e3541600-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32799, UUID.fromString("e3541601-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32800, UUID.fromString("e3541602-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32801, UUID.fromString("e3541603-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1.0, 1785505920, 0, 32802, UUID.fromString("e3541604-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1.0, 1785505920, 0, 32803, UUID.fromString("e3541605-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, false, 1785505920, 0, 32804, UUID.fromString("e3541606-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32805, UUID.fromString("e3541607-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), 1785505920, 0, 32806, UUID.fromString("e3541608-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32807, UUID.fromString("e3552770-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), 1785505920, 0, 32808, UUID.fromString("e3552771-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-02"), 1785505920, 0, 32809, UUID.fromString("e3552772-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-02-02"), 1785505920, 0, 32810, UUID.fromString("e3552773-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.1"), 1785505920, 0, 32811, UUID.fromString("e3552774-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000001"), 1785505920, 0, 32812, UUID.fromString("e3552775-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32813, UUID.fromString("e3552776-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, 1785505920, 0, 32814, UUID.fromString("e3552777-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "test101 full test", 1785505920, 0, 32817, UUID.fromString("e3554e80-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "test101 full test", 1785505920, 0, 32818, UUID.fromString("e3554e81-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 1, "string1_1",
                  "string1_1", 1, 1, 1, 1, 1.0, 1.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-02"), TimestampType.instance.fromTimeInMillis(1), InetAddressType.instance.fromString("192.168.0.1"), UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, 1, "test101 full test",
                  "test101 full test", 1785505920, 0, 32768, UUID.fromString("00000000-0000-8000-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-02"), TimestampType.instance.fromTimeInMillis(1), InetAddressType.instance.fromString("192.168.0.1"), UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, 1, "test101 full test",
                  "test101 full test", 1, "string1_1",
                  "string1_1", 1, 1, 1, 1, 1.0, 1.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), true, 1785505920, 0, 65689, ByteBuffer.wrap("test101".getBytes()), 1, UUID.fromString("e3580da0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, true, 1785505920, 1, -1, UUID.fromString("ffffffff-ffff-ffff-0000-000000000000") ,
     1501172613354001L, "2", 1785505920, 1, 32771, UUID.fromString("00000000-0000-8003-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32772, UUID.fromString("00000000-0000-8004-0000-000000000000") ,
     1501172613354001L, "string2_2", 1785505920, 1, 32773, UUID.fromString("00000000-0000-8005-0000-000000000000") ,
     1501172613354001L,
                  "string2_2", 1785505920, 1, 32774, UUID.fromString("00000000-0000-8006-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32775, UUID.fromString("00000000-0000-8007-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32776, UUID.fromString("00000000-0000-8008-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32777, UUID.fromString("00000000-0000-8009-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32778, UUID.fromString("00000000-0000-800a-0000-000000000000") ,
     1501172613354001L, 2.0, 1785505920, 1, 32779, UUID.fromString("00000000-0000-800b-0000-000000000000") ,
     1501172613354001L, 2.0, 1785505920, 1, 32780, UUID.fromString("00000000-0000-800c-0000-000000000000") ,
     1501172613354001L, true, 1785505920, 1, 32781, UUID.fromString("00000000-0000-800d-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32782, UUID.fromString("00000000-0000-800e-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), 1785505920, 1, 32783, UUID.fromString("00000000-0000-800f-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32784, UUID.fromString("00000000-0000-8010-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32785, UUID.fromString("00000000-0000-8011-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-03"), 1785505920, 1, 32786, UUID.fromString("00000000-0000-8012-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-03-03"), 1785505920, 1, 32787, UUID.fromString("00000000-0000-8013-0000-000000000000") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.2"), 1785505920, 1, 32788, UUID.fromString("00000000-0000-8014-0000-000000000000") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000002"), 1785505920, 1, 32789, UUID.fromString("00000000-0000-8015-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32790, UUID.fromString("00000000-0000-8016-0000-000000000000") ,
     1501172613354001L, 2, 1785505920, 1, 32791, UUID.fromString("00000000-0000-8017-0000-000000000000") ,
     1501172613354001L, "test202 full test", 1785505920, 1, 32794, UUID.fromString("00000000-0000-801a-0000-000000000000") ,
     1501172613354001L,
                  "test202 full test", 1785505920, 1, 32795, UUID.fromString("00000000-0000-801b-0000-000000000000") ,
     1501172613354001L, "string2_2", 1785505920, 1, 32796, UUID.fromString("e35611d0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "string2_2", 1785505920, 1, 32797, UUID.fromString("e35611d1-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32798, UUID.fromString("e35611d2-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32799, UUID.fromString("e35611d3-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32800, UUID.fromString("e35611d4-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32801, UUID.fromString("e35611d5-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2.0, 1785505920, 1, 32802, UUID.fromString("e35611d6-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2.0, 1785505920, 1, 32803, UUID.fromString("e35611d7-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, true, 1785505920, 1, 32804, UUID.fromString("e35611d8-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32805, UUID.fromString("e35611d9-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), 1785505920, 1, 32806, UUID.fromString("e35611da-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32807, UUID.fromString("e35611db-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32808, UUID.fromString("e35611dc-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-03"), 1785505920, 1, 32809, UUID.fromString("e35638e0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromTimeInMillis(2), 1785505920, 1, 32810, UUID.fromString("e35638e1-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.2"), 1785505920, 1, 32811, UUID.fromString("e35638e2-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000002"), 1785505920, 1, 32812, UUID.fromString("e35638e3-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32813, UUID.fromString("e35638e4-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32814, UUID.fromString("e35638e5-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "test202 full test", 1785505920, 1, 32817, UUID.fromString("e35638e8-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "test202 full test", 1785505920, 1, 32818, UUID.fromString("e35638e9-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "string2_2", 1785505920, 1, 32796, UUID.fromString("e3565ff0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "string2_2", 1785505920, 1, 32797, UUID.fromString("e3565ff1-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32798, UUID.fromString("e3565ff2-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32799, UUID.fromString("e3565ff3-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32800, UUID.fromString("e3565ff4-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32801, UUID.fromString("e3565ff5-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2.0, 1785505920, 1, 32802, UUID.fromString("e3565ff6-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2.0, 1785505920, 1, 32803, UUID.fromString("e3565ff7-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, true, 1785505920, 1, 32804, UUID.fromString("e3565ff8-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32805, UUID.fromString("e3565ff9-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), 1785505920, 1, 32806, UUID.fromString("e3565ffa-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32807, UUID.fromString("e3565ffb-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), 1785505920, 1, 32808, UUID.fromString("e3565ffc-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-03"), 1785505920, 1, 32809, UUID.fromString("e3565ffd-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-03-03"), 1785505920, 1, 32810, UUID.fromString("e3568700-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.2"), 1785505920, 1, 32811, UUID.fromString("e3568701-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000002"), 1785505920, 1, 32812, UUID.fromString("e3568702-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32813, UUID.fromString("e3568703-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, 1785505920, 1, 32814, UUID.fromString("e3568704-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "test202 full test", 1785505920, 1, 32817, UUID.fromString("e356ae12-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "test202 full test", 1785505920, 1, 32818, UUID.fromString("e356ae13-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, "string2_2",
                  "string2_2", 2, 2, 2, 2, 2.0, 2.0, true, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-03"), TimestampType.instance.fromTimeInMillis(2), InetAddressType.instance.fromString("192.168.0.2"), UUID.fromString("00000000-0000-0000-0000-000000000002"), 2, 2, "test202 full test",
                  "test202 full test", 1785505920, 1, 32768, UUID.fromString("00000000-0000-8000-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-03"), TimestampType.instance.fromTimeInMillis(2), InetAddressType.instance.fromString("192.168.0.2"), UUID.fromString("00000000-0000-0000-0000-000000000002"), 2, 2, "test202 full test",
                  "test202 full test", 2, "string2_2",
                  "string2_2", 2, 2, 2, 2, 2.0, 2.0, true, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), true, 1785505920, 1, 65689, ByteBuffer.wrap("test202".getBytes()), 1, UUID.fromString("e35882d0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 6, true, 1785505920, 1, 65688, ByteBuffer.wrap("test101".getBytes()), 1, UUID.fromString("e358f800-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, true, 1785505920, 2, -1, UUID.fromString("ffffffff-ffff-ffff-0000-000000000000") ,
     1501172613354001L, "3", 1785505920, 2, 32771, UUID.fromString("00000000-0000-8003-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32772, UUID.fromString("00000000-0000-8004-0000-000000000000") ,
     1501172613354001L, "string3_3", 1785505920, 2, 32773, UUID.fromString("00000000-0000-8005-0000-000000000000") ,
     1501172613354001L,
                  "string3_3", 1785505920, 2, 32774, UUID.fromString("00000000-0000-8006-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32775, UUID.fromString("00000000-0000-8007-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32776, UUID.fromString("00000000-0000-8008-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32777, UUID.fromString("00000000-0000-8009-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32778, UUID.fromString("00000000-0000-800a-0000-000000000000") ,
     1501172613354001L, 3.0, 1785505920, 2, 32779, UUID.fromString("00000000-0000-800b-0000-000000000000") ,
     1501172613354001L, 3.0, 1785505920, 2, 32780, UUID.fromString("00000000-0000-800c-0000-000000000000") ,
     1501172613354001L, false, 1785505920, 2, 32781, UUID.fromString("00000000-0000-800d-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32782, UUID.fromString("00000000-0000-800e-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), 1785505920, 2, 32783, UUID.fromString("00000000-0000-800f-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32784, UUID.fromString("00000000-0000-8010-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32785, UUID.fromString("00000000-0000-8011-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-04"), 1785505920, 2, 32786, UUID.fromString("00000000-0000-8012-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromTimeInMillis(3), 1785505920, 2, 32787, UUID.fromString("00000000-0000-8013-0000-000000000000") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.3"), 1785505920, 2, 32788, UUID.fromString("00000000-0000-8014-0000-000000000000") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000003"), 1785505920, 2, 32789, UUID.fromString("00000000-0000-8015-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32790, UUID.fromString("00000000-0000-8016-0000-000000000000") ,
     1501172613354001L, 3, 1785505920, 2, 32791, UUID.fromString("00000000-0000-8017-0000-000000000000") ,
     1501172613354001L, "test303 full test", 1785505920, 2, 32794, UUID.fromString("00000000-0000-801a-0000-000000000000") ,
     1501172613354001L,
                  "test303 full test", 1785505920, 2, 32795, UUID.fromString("00000000-0000-801b-0000-000000000000") ,
     1501172613354001L, "string3_3", 1785505920, 2, 32796, UUID.fromString("e3577160-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "string3_3", 1785505920, 2, 32797, UUID.fromString("e3577161-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32798, UUID.fromString("e3577162-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32799, UUID.fromString("e3577163-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32800, UUID.fromString("e3577164-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32801, UUID.fromString("e3577165-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3.0, 1785505920, 2, 32802, UUID.fromString("e3577166-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3.0, 1785505920, 2, 32803, UUID.fromString("e3577167-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, false, 1785505920, 2, 32804, UUID.fromString("e3577168-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32805, UUID.fromString("e3577169-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), 1785505920, 2, 32806, UUID.fromString("e357716a-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32807, UUID.fromString("e357716b-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32808, UUID.fromString("e357716c-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-04"), 1785505920, 2, 32809, UUID.fromString("e357716d-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromTimeInMillis(3), 1785505920, 2, 32810, UUID.fromString("e3579870-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.3"), 1785505920, 2, 32811, UUID.fromString("e3579871-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000003"), 1785505920, 2, 32812, UUID.fromString("e3579872-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32813, UUID.fromString("e3579873-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32814, UUID.fromString("e3579874-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "test303 full test", 1785505920, 2, 32817, UUID.fromString("e3579877-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "test303 full test", 1785505920, 2, 32818, UUID.fromString("e3579878-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "string3_3", 1785505920, 2, 32796, UUID.fromString("e357bf80-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "string3_3", 1785505920, 2, 32797, UUID.fromString("e357bf81-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32798, UUID.fromString("e357bf82-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32799, UUID.fromString("e357bf83-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32800, UUID.fromString("e357bf84-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32801, UUID.fromString("e357bf85-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3.0, 1785505920, 2, 32802, UUID.fromString("e357bf86-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3.0, 1785505920, 2, 32803, UUID.fromString("e357bf87-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, false, 1785505920, 2, 32804, UUID.fromString("e357bf88-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32805, UUID.fromString("e357bf89-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), 1785505920, 2, 32806, UUID.fromString("e357bf8a-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32807, UUID.fromString("e357bf8b-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), 1785505920, 2, 32808, UUID.fromString("e357bf8c-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-04"), 1785505920, 2, 32809, UUID.fromString("e357bf8d-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromTimeInMillis(3), 1785505920, 2, 32810, UUID.fromString("e357bf8e-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, InetAddressType.instance.fromString("192.168.0.3"), 1785505920, 2, 32811, UUID.fromString("e357bf8f-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, UUID.fromString("00000000-0000-0000-0000-000000000003"), 1785505920, 2, 32812, UUID.fromString("e357bf90-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32813, UUID.fromString("e357e690-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, 1785505920, 2, 32814, UUID.fromString("e357e691-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, "test303 full test", 1785505920, 2, 32817, UUID.fromString("e357e694-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L,
                  "test303 full test", 1785505920, 2, 32818, UUID.fromString("e357e695-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, "string3_3",
                  "string3_3", 3, 3, 3, 3, 3.0, 3.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-04"), TimestampType.instance.fromTimeInMillis(3), InetAddressType.instance.fromString("192.168.0.3"), UUID.fromString("00000000-0000-0000-0000-000000000003"), 3, 3, "test303 full test",
                  "test303 full test", 1785505920, 2, 32768, UUID.fromString("00000000-0000-8000-0000-000000000000") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-04"), TimestampType.instance.fromTimeInMillis(3), InetAddressType.instance.fromString("192.168.0.3"), UUID.fromString("00000000-0000-0000-0000-000000000003"), 3, 3, "test303 full test",
                  "test303 full test", 3, "string3_3",
                  "string3_3", 3, 3, 3, 3, 3.0, 3.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), true, 1785505920, 2, 65689, ByteBuffer.wrap("test303".getBytes()), 1, UUID.fromString("e358a9e0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.004Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.004Z"), TimestampType.instance.fromString("1970-01-05"), TimestampType.instance.fromTimeInMillis(4), InetAddressType.instance.fromString("192.168.0.4"), UUID.fromString("00000000-0000-0000-0000-000000000004"), 4, 4, "test404 full test",
                  "test404 full test", 4, "string4_4",
                  "string4_4", 4, 4, 4, 4, 4.0, 4.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.004Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.004Z"), true, 1785505920, 2, 65688, ByteBuffer.wrap("test404".getBytes()), 1, UUID.fromString("e358d0f0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, true, 1785505920, 3, -1, UUID.fromString("ffffffff-ffff-ffff-0000-000000000000") ,
     1501172613354001L, "0", 1785505920, 3, 32771, UUID.fromString("00000000-0000-8003-0000-000000000000") ,
     1501172613354001L, 0, 1785505920, 3, 32772, UUID.fromString("00000000-0000-8004-0000-000000000000") ,
     1501172613354001L, 0, 1785505920, 3, 32768, UUID.fromString("00000000-0000-8000-0000-000000000000") ,
     1501172613354001L, 1, "string1_1",
                  "string1_1", 1, 1, 1, 1, 1.0, 1.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-02"), TimestampType.instance.fromTimeInMillis(1), InetAddressType.instance.fromString("192.168.0.1"), UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, 1, "test101 full test",
                  "test101 full test", 1785505920, 3, 32770, UUID.fromString("e34f0cf0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 2, "string2_2",
                  "string2_2", 2, 2, 2, 2, 2.0, 2.0, true, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-03"), TimestampType.instance.fromTimeInMillis(2), InetAddressType.instance.fromString("192.168.0.2"), UUID.fromString("00000000-0000-0000-0000-000000000002"), 2, 2, "test202 full test",
                  "test202 full test", 1785505920, 3, 32770, UUID.fromString("e34fd040-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 3, "string3_3",
                  "string3_3", 3, 3, 3, 3, 3.0, 3.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-04"), TimestampType.instance.fromTimeInMillis(3), InetAddressType.instance.fromString("192.168.0.3"), UUID.fromString("00000000-0000-0000-0000-000000000003"), 3, 3, "test303 full test",
                  "test303 full test", 1785505920, 3, 32770, UUID.fromString("e34fd041-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("1970-01-02"), TimestampType.instance.fromTimeInMillis(1), InetAddressType.instance.fromString("192.168.0.1"), UUID.fromString("00000000-0000-0000-0000-000000000001"), 1, 1, "test101 full test",
                  "test101 full test", 1, "string1_1",
                  "string1_1", 1, 1, 1, 1, 1.0, 1.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.001Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.001Z"), true, 1785505920, 3, 65688, ByteBuffer.wrap("test101".getBytes()), 1, UUID.fromString("e3580da0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("1970-01-03"), TimestampType.instance.fromTimeInMillis(2), InetAddressType.instance.fromString("192.168.0.2"), UUID.fromString("00000000-0000-0000-0000-000000000002"), 2, 2, "test202 full test",
                  "test202 full test", 2, "string2_2",
                  "string2_2", 2, 2, 2, 2, 2.0, 2.0, true, TimestampType.instance.fromString("1970-01-01T00:00:00.002Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.002Z"), true, 1785505920, 3, 65688, ByteBuffer.wrap("test102".getBytes()), 1, UUID.fromString("e35882d0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("1970-01-04"), TimestampType.instance.fromTimeInMillis(3), InetAddressType.instance.fromString("192.168.0.3"), UUID.fromString("00000000-0000-0000-0000-000000000003"), 3, 3, "test303 full test",
                  "test303 full test", 3, "string3_3",
                  "string3_3", 3, 3, 3, 3, 3.0, 3.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.003Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.003Z"), true, 1785505920, 3, 65688, ByteBuffer.wrap("test303".getBytes()), 1, UUID.fromString("e358a9e0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.004Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.004Z"), TimestampType.instance.fromString("1970-01-05"), TimestampType.instance.fromTimeInMillis(4), InetAddressType.instance.fromString("192.168.0.4"), UUID.fromString("00000000-0000-0000-0000-000000000004"), 4, 4, "test404 full test",
                  "test404 full test", 4, "string4_4",
                  "string4_4", 4, 4, 4, 4, 4.0, 4.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.004Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.004Z"), true, 1785505920, 3, 65689, ByteBuffer.wrap("test404".getBytes()), 1, UUID.fromString("e358d0f0-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.005Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.005Z"), TimestampType.instance.fromString("1970-01-06"), TimestampType.instance.fromTimeInMillis(5), InetAddressType.instance.fromString("192.168.0.5"), UUID.fromString("00000000-0000-0000-0000-000000000005"), 5, 5, "test505 full test",
                  "test505 full test", 5, "string5_5",
                  "string5_5", 5, 5, 5, 5, 5.0, 5.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.005Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.005Z"), true, 1785505920, 3, 65688, ByteBuffer.wrap("test505".getBytes()), 1, UUID.fromString("e358d0f1-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, TimestampType.instance.fromString("1970-01-01T00:00:00.005Z"), TimestampType.instance.fromString("1970-01-01T00:00:00.005Z"), TimestampType.instance.fromString("1970-01-06"), TimestampType.instance.fromTimeInMillis(5), InetAddressType.instance.fromString("192.168.0.5"), UUID.fromString("00000000-0000-0000-0000-000000000005"), 5, 5, "test505 full test",
                  "test505 full test", 5, "string5_5",
                  "string5_5", 5, 5, 5, 5, 5.0, 5.0, false, TimestampType.instance.fromString("1970-01-01T00:00:00.005Z"), TimestampType.instance.fromString("28011-12-02T00:00:00.005Z"), true, 1785505920, 3, 65689, ByteBuffer.wrap("test606".getBytes()), 1, UUID.fromString("e358d0f1-72e7-11e7-ac86-5734dcb5b823") ,
     1501172613354001L, 6, true, 1785505920, 3, 65689, ByteBuffer.wrap("test606".getBytes()), 1, UUID.fromString("e358f800-72e7-11e7-ac86-5734dcb5b823")
    };

    // Taken from a DSE Graph test
    void createLargeBatchWith2is()
    {
        session.execute("drop keyspace if exists \"TestGraph\";");
        session.execute("CREATE KEYSPACE \"TestGraph\" WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");
        session.execute("CREATE TABLE \"TestGraph\".\"SecondaryIndex_p\" (\n" +
                        "    community_id int,\n" +
                        "    member_id int,\n" +
                        "    \"~~property_key_id\" int,\n" +
                        "    \"~~property_id\" uuid,\n" +
                        "    \"dot.\" int,\n" +
                        "    \"multiple_bigDecimal\" int,\n" +
                        "    \"multiple_bigInteger\" int,\n" +
                        "    multiple_boolean boolean,\n" +
                        "    multiple_calendar blob,\n" +
                        "    multiple_date blob,\n" +
                        "    \"multiple_dot.\" int,\n" +
                        "    multiple_double double,\n" +
                        "    multiple_float double,\n" +
                        "    \"multiple_fullText\" text,\n" +
                        "    \"multiple_fullText_m\" text,\n" +
                        "    multiple_inet blob,\n" +
                        "    multiple_instant blob,\n" +
                        "    multiple_int int,\n" +
                        "    \"multiple_localDate\" blob,\n" +
                        "    \"multiple_localTime\" blob,\n" +
                        "    multiple_long int,\n" +
                        "    \"multiple_negInstant\" blob,\n" +
                        "    multiple_short int,\n" +
                        "    multiple_string text,\n" +
                        "    multiple_string_m text,\n" +
                        "    multiple_uuid uuid,\n" +
                        "    name int,\n" +
                        "    pk1 text,\n" +
                        "    pk2 int,\n" +
                        "    simple_string text,\n" +
                        "    \"single_bigDecimal\" int,\n" +
                        "    \"single_bigInteger\" int,\n" +
                        "    single_boolean boolean,\n" +
                        "    single_calendar blob,\n" +
                        "    single_date blob,\n" +
                        "    \"single_dot.\" int,\n" +
                        "    single_double double,\n" +
                        "    single_float double,\n" +
                        "    \"single_fullText\" text,\n" +
                        "    \"single_fullText_m\" text,\n" +
                        "    single_inet blob,\n" +
                        "    single_instant blob,\n" +
                        "    single_int int,\n" +
                        "    \"single_localDate\" blob,\n" +
                        "    \"single_localTime\" blob,\n" +
                        "    single_long int,\n" +
                        "    \"single_negInstant\" blob,\n" +
                        "    single_short int,\n" +
                        "    single_string text,\n" +
                        "    single_string_m text,\n" +
                        "    single_uuid uuid,\n" +
                        "    vp int,\n" +
                        "    \"~meta_bigDecimal\" int,\n" +
                        "    \"~meta_bigInteger\" int,\n" +
                        "    \"~meta_boolean\" boolean,\n" +
                        "    \"~meta_calendar\" blob,\n" +
                        "    \"~meta_date\" blob,\n" +
                        "    \"~meta_dot.\" int,\n" +
                        "    \"~meta_double\" double,\n" +
                        "    \"~meta_float\" double,\n" +
                        "    \"~meta_fullText\" text,\n" +
                        "    \"~meta_fullText_m\" text,\n" +
                        "    \"~meta_inet\" blob,\n" +
                        "    \"~meta_instant\" blob,\n" +
                        "    \"~meta_int\" int,\n" +
                        "    \"~meta_localDate\" blob,\n" +
                        "    \"~meta_localTime\" blob,\n" +
                        "    \"~meta_long\" int,\n" +
                        "    \"~meta_negInstant\" blob,\n" +
                        "    \"~meta_short\" int,\n" +
                        "    \"~meta_string\" text,\n" +
                        "    \"~meta_string_m\" text,\n" +
                        "    \"~meta_uuid\" uuid,\n" +
                        "    \"~~vertex_exists\" boolean,\n" +
                        "    PRIMARY KEY (community_id, member_id, \"~~property_key_id\", \"~~property_id\")\n" +
                        ") WITH CLUSTERING ORDER BY (member_id ASC, \"~~property_key_id\" ASC, \"~~property_id\" ASC)\n" +
                        "    AND bloom_filter_fp_chance = 0.01\n" +
                        "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                        "    AND comment = ''\n" +
                        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                        "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                        "    AND crc_check_chance = 1.0\n" +
                        "    AND dclocal_read_repair_chance = 0.1\n" +
                        "    AND default_time_to_live = 0\n" +
                        "    AND gc_grace_seconds = 864000\n" +
                        "    AND max_index_interval = 2048\n" +
                        "    AND memtable_flush_period_in_ms = 0\n" +
                        "    AND min_index_interval = 128\n" +
                        "    AND read_repair_chance = 0.0\n" +
                        "    AND speculative_retry = '99PERCENTILE';\n");

        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_inet\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_inet);");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_long\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_long);");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_dot_\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_dot.\");");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_negInstant\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_negInstant\");");

        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_date\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_date);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_bigDecimal\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_bigDecimal\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_string\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_string);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_date\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_date);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_string_m\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_string_m);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_float\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_float);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_instant\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_instant);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_fullText\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_fullText\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_uuid\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_uuid);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_short\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_short);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_float\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_float);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_bigInteger\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_bigInteger\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_fullText_m\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_fullText_m\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_string\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_string);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_uuid\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_uuid);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_calendar\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_calendar);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_instant\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_instant);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_dot_\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_dot.\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_double\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_double);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_short\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_short);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_int\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_int);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_bigInteger\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_bigInteger\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_boolean\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_boolean);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_fullText_m\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_fullText_m\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_localTime\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_localTime\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_long\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_long);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_string_m\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_string_m);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_localTime\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_localTime\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_localDate\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_localDate\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_localDate\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_localDate\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_bigDecimal\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_bigDecimal\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_double\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_double);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_fullText\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"multiple_fullText\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_negInstant\" ON \"TestGraph\".\"SecondaryIndex_p\" (\"single_negInstant\");\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_boolean\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_boolean);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_int\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_int);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_multiple_calendar\" ON \"TestGraph\".\"SecondaryIndex_p\" (multiple_calendar);\n");
        session.execute("CREATE INDEX \"SecondaryIndex_2i_2i_single_inet\" ON \"TestGraph\".\"SecondaryIndex_p\" (single_inet);\n");

        session.execute("CREATE TABLE \"TestGraph\".\"SecondaryIndex_e\" (\n" +
                        "    community_id int,\n" +
                        "    member_id int,\n" +
                        "    \"~~edge_label_id\" int,\n" +
                        "    \"~~adjacent_vertex_id\" blob,\n" +
                        "    \"~~adjacent_label_id\" int,\n" +
                        "    \"~~edge_id\" uuid,\n" +
                        "    \"~name\" int,\n" +
                        "    \"~simple_string\" text,\n" +
                        "    \"~single_bigDecimal\" int,\n" +
                        "    \"~single_bigInteger\" int,\n" +
                        "    \"~single_boolean\" boolean,\n" +
                        "    \"~single_calendar\" blob,\n" +
                        "    \"~single_date\" blob,\n" +
                        "    \"~single_dot.\" int,\n" +
                        "    \"~single_double\" double,\n" +
                        "    \"~single_float\" double,\n" +
                        "    \"~single_fullText\" text,\n" +
                        "    \"~single_fullText_m\" text,\n" +
                        "    \"~single_inet\" blob,\n" +
                        "    \"~single_instant\" blob,\n" +
                        "    \"~single_int\" int,\n" +
                        "    \"~single_localDate\" blob,\n" +
                        "    \"~single_localTime\" blob,\n" +
                        "    \"~single_long\" int,\n" +
                        "    \"~single_negInstant\" blob,\n" +
                        "    \"~single_short\" int,\n" +
                        "    \"~single_string\" text,\n" +
                        "    \"~single_string_m\" text,\n" +
                        "    \"~single_uuid\" uuid,\n" +
                        "    \"~~edge_exists\" boolean,\n" +
                        "    \"~~simple_edge_id\" uuid,\n" +
                        "    PRIMARY KEY (community_id, member_id, \"~~edge_label_id\", \"~~adjacent_vertex_id\", \"~~adjacent_label_id\", \"~~edge_id\")\n" +
                        ") WITH CLUSTERING ORDER BY (member_id ASC, \"~~edge_label_id\" ASC, \"~~adjacent_vertex_id\" ASC, \"~~adjacent_label_id\" ASC, \"~~edge_id\" ASC)\n" +
                        "    AND bloom_filter_fp_chance = 0.01\n" +
                        "    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}\n" +
                        "    AND comment = ''\n" +
                        "    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}\n" +
                        "    AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}\n" +
                        "    AND crc_check_chance = 1.0\n" +
                        "    AND dclocal_read_repair_chance = 0.1\n" +
                        "    AND default_time_to_live = 0\n" +
                        "    AND gc_grace_seconds = 864000\n" +
                        "    AND max_index_interval = 2048\n" +
                        "    AND memtable_flush_period_in_ms = 0\n" +
                        "    AND min_index_interval = 128\n" +
                        "    AND read_repair_chance = 0.0\n" +
                        "    AND speculative_retry = '99PERCENTILE';");
    }

    void insertLargeBatchWith2is()
    {
        PreparedStatement p = session.prepare("BEGIN BATCH\n UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"~~vertex_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk1\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk2\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"name\" = ?, \"~meta_string\" = ?, \"~meta_string_m\" = ?, \"~meta_dot.\" = ?, \"~meta_int\" = ?, \"~meta_long\" = ?, \"~meta_short\" = ?, \"~meta_double\" = ?, \"~meta_float\" = ?, \"~meta_boolean\" = ?, \"~meta_instant\" = ?, \"~meta_negInstant\" = ?, \"~meta_date\" = ?, \"~meta_calendar\" = ?, \"~meta_localDate\" = ?, \"~meta_localTime\" = ?, \"~meta_inet\" = ?, \"~meta_uuid\" = ?, \"~meta_bigInteger\" = ?, \"~meta_bigDecimal\" = ?, \"~meta_fullText\" = ?, \"~meta_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"~~vertex_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk1\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk2\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"name\" = ?, \"~meta_string\" = ?, \"~meta_string_m\" = ?, \"~meta_dot.\" = ?, \"~meta_int\" = ?, \"~meta_long\" = ?, \"~meta_short\" = ?, \"~meta_double\" = ?, \"~meta_float\" = ?, \"~meta_boolean\" = ?, \"~meta_instant\" = ?, \"~meta_negInstant\" = ?, \"~meta_date\" = ?, \"~meta_calendar\" = ?, \"~meta_localDate\" = ?, \"~meta_localTime\" = ?, \"~meta_inet\" = ?, \"~meta_uuid\" = ?, \"~meta_bigInteger\" = ?, \"~meta_bigDecimal\" = ?, \"~meta_fullText\" = ?, \"~meta_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~name\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"~~vertex_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk1\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk2\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"single_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_string_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_dot.\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_int\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_long\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_short\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_double\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_float\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_boolean\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_instant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_negInstant\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_date\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_calendar\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localDate\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_localTime\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_inet\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_uuid\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigInteger\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_bigDecimal\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"multiple_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"name\" = ?, \"~meta_string\" = ?, \"~meta_string_m\" = ?, \"~meta_dot.\" = ?, \"~meta_int\" = ?, \"~meta_long\" = ?, \"~meta_short\" = ?, \"~meta_double\" = ?, \"~meta_float\" = ?, \"~meta_boolean\" = ?, \"~meta_instant\" = ?, \"~meta_negInstant\" = ?, \"~meta_date\" = ?, \"~meta_calendar\" = ?, \"~meta_localDate\" = ?, \"~meta_localTime\" = ?, \"~meta_inet\" = ?, \"~meta_uuid\" = ?, \"~meta_bigInteger\" = ?, \"~meta_bigDecimal\" = ?, \"~meta_fullText\" = ?, \"~meta_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"~~vertex_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk1\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"pk2\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"name\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"vp\" = ?, \"~meta_string\" = ?, \"~meta_string_m\" = ?, \"~meta_dot.\" = ?, \"~meta_int\" = ?, \"~meta_long\" = ?, \"~meta_short\" = ?, \"~meta_double\" = ?, \"~meta_float\" = ?, \"~meta_boolean\" = ?, \"~meta_instant\" = ?, \"~meta_negInstant\" = ?, \"~meta_date\" = ?, \"~meta_calendar\" = ?, \"~meta_localDate\" = ?, \"~meta_localTime\" = ?, \"~meta_inet\" = ?, \"~meta_uuid\" = ?, \"~meta_bigInteger\" = ?, \"~meta_bigDecimal\" = ?, \"~meta_fullText\" = ?, \"~meta_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"vp\" = ?, \"~meta_string\" = ?, \"~meta_string_m\" = ?, \"~meta_dot.\" = ?, \"~meta_int\" = ?, \"~meta_long\" = ?, \"~meta_short\" = ?, \"~meta_double\" = ?, \"~meta_float\" = ?, \"~meta_boolean\" = ?, \"~meta_instant\" = ?, \"~meta_negInstant\" = ?, \"~meta_date\" = ?, \"~meta_calendar\" = ?, \"~meta_localDate\" = ?, \"~meta_localTime\" = ?, \"~meta_inet\" = ?, \"~meta_uuid\" = ?, \"~meta_bigInteger\" = ?, \"~meta_bigDecimal\" = ?, \"~meta_fullText\" = ?, \"~meta_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_p\" USING TIMESTAMP ? SET \"vp\" = ?, \"~meta_string\" = ?, \"~meta_string_m\" = ?, \"~meta_dot.\" = ?, \"~meta_int\" = ?, \"~meta_long\" = ?, \"~meta_short\" = ?, \"~meta_double\" = ?, \"~meta_float\" = ?, \"~meta_boolean\" = ?, \"~meta_instant\" = ?, \"~meta_negInstant\" = ?, \"~meta_date\" = ?, \"~meta_calendar\" = ?, \"~meta_localDate\" = ?, \"~meta_localTime\" = ?, \"~meta_inet\" = ?, \"~meta_uuid\" = ?, \"~meta_bigInteger\" = ?, \"~meta_bigDecimal\" = ?, \"~meta_fullText\" = ?, \"~meta_fullText_m\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~property_key_id\" = ? AND \"~~property_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~single_date\" = ?, \"~single_calendar\" = ?, \"~single_localDate\" = ?, \"~single_localTime\" = ?, \"~single_inet\" = ?, \"~single_uuid\" = ?, \"~single_bigInteger\" = ?, \"~single_bigDecimal\" = ?, \"~single_fullText\" = ?, \"~single_fullText_m\" = ?, \"~name\" = ?, \"~single_string\" = ?, \"~single_string_m\" = ?, \"~single_dot.\" = ?, \"~single_int\" = ?, \"~single_long\" = ?, \"~single_short\" = ?, \"~single_double\" = ?, \"~single_float\" = ?, \"~single_boolean\" = ?, \"~single_instant\" = ?, \"~single_negInstant\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    UPDATE \"TestGraph\".\"SecondaryIndex_e\" USING TIMESTAMP ? SET \"~name\" = ?, \"~~edge_exists\" = ? WHERE \"community_id\" = ? AND \"member_id\" = ? AND \"~~edge_label_id\" = ? AND \"~~adjacent_vertex_id\" = ? AND \"~~adjacent_label_id\" = ? AND \"~~edge_id\" = ?;\n" +
                        "    APPLY BATCH\n");

        session.execute(p.bind(data));
    }

    @Test
    public void testLargeBatchWith2is()
    {
        createLargeBatchWith2is();
        insertLargeBatchWith2is();
    }

    @Test(expected = InvalidQueryException.class)
    public void testMixedInCounterBatch()
    {
       sendBatch(BatchStatement.Type.COUNTER, true, true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testMixedInLoggedBatch()
    {
        sendBatch(BatchStatement.Type.LOGGED, true, true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testMixedInUnLoggedBatch()
    {
        sendBatch(BatchStatement.Type.UNLOGGED, true, true);
    }

    @Test(expected = InvalidQueryException.class)
    public void testNonCounterInCounterBatch()
    {
        sendBatch(BatchStatement.Type.COUNTER, false, true);
    }

    @Test
    public void testNonCounterInLoggedBatch()
    {
        sendBatch(BatchStatement.Type.LOGGED, false, true);
    }

    @Test
    public void testNonCounterInUnLoggedBatch()
    {
        sendBatch(BatchStatement.Type.UNLOGGED, false, true);
    }

    @Test
    public void testCounterInCounterBatch()
    {
        sendBatch(BatchStatement.Type.COUNTER, true, false);
    }

    @Test
    public void testCounterInUnLoggedBatch()
    {
        sendBatch(BatchStatement.Type.UNLOGGED, true, false);
    }

    @Test
    public void testEmptyBatch()
    {
        session.execute("BEGIN BATCH APPLY BATCH");
        session.execute("BEGIN UNLOGGED BATCH APPLY BATCH");
    }

    @Test(expected = InvalidQueryException.class)
    public void testCounterInLoggedBatch()
    {
        sendBatch(BatchStatement.Type.LOGGED, true, false);
    }

    @Test(expected = InvalidQueryException.class)
    public void testOversizedBatch()
    {
        int SIZE_FOR_FAILURE = 2500;
        BatchStatement b = new BatchStatement(BatchStatement.Type.UNLOGGED);
        for (int i = 0; i < SIZE_FOR_FAILURE; i++)
        {
            b.add(noncounter.bind(i, "foobar"));
        }
        session.execute(b);
    }

    public void sendBatch(BatchStatement.Type type, boolean addCounter, boolean addNonCounter)
    {

        assert addCounter || addNonCounter;
        BatchStatement b = new BatchStatement(type);

        for (int i = 0; i < 10; i++)
        {
            if (addNonCounter)
                b.add(noncounter.bind(i, "foo"));

            if (addCounter)
                b.add(counter.bind((long)i, i));
        }

        session.execute(b);
    }

}
