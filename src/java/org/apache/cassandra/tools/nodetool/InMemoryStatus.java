/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package org.apache.cassandra.tools.nodetool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.mos.MemoryOnlyStatusMXBean;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;

@Command(name = "inmemorystatus", description = "Returns a list of the in-memory tables for this node and the amount of memory each table is using, or information about a single table if the keyspace and columnfamily are given.")
public class InMemoryStatus extends NodeTool.NodeToolCmd
{
    @Arguments(usage = "[<keyspace> <table>...]", description = "The keyspace followed by one or many tables")
    private List<String> args = new ArrayList<>();

    protected void execute(NodeProbe probe)
    {
        if (args.size() == 0)
        {
            printInmemoryInfo(probe.getMemoryOnlyStatusProxy());
        }
        else if (args.size() == 2)
        {
            printInmemoryInfo(probe.getMemoryOnlyStatusProxy(), args.get(0), args.get(1));
        }
        else
        {
            throw new IllegalArgumentException("inmemorystatus can be called with either no arguments (print all tables) or "
                                               + "a keyspace/columnfamily pair.");
        }
    }

    private void printInmemoryInfo(MemoryOnlyStatusMXBean proxy)
    {
        printInmemoryInfo(proxy.getMemoryOnlyTableInformation(), proxy.getMemoryOnlyTotals());
    }

    private void printInmemoryInfo(MemoryOnlyStatusMXBean proxy, String ks, String cf)
    {
        printInmemoryInfo(
        Collections.singletonList(proxy.getMemoryOnlyTableInformation(ks, cf)),
        proxy.getMemoryOnlyTotals()
        );
    }

    private void printInmemoryInfo(List<MemoryOnlyStatusMXBean.TableInfo> infos, MemoryOnlyStatusMXBean.TotalInfo totals)
    {
        System.out.format("Max Memory to Lock:                    %10dMB\n", totals.getMaxMemoryToLock() / 0x100000);
        System.out.format("Current Total Memory Locked:           %10dMB\n", totals.getUsed() / 0x100000);
        System.out.format("Current Total Memory Not Able To Lock: %10dMB\n", totals.getNotAbleToLock() / 0x100000);
        if (infos.size() > 0)
        {
            System.out.format("%-30s %-30s %12s %17s %7s\n", "Keyspace", "ColumnFamily", "Size", "Couldn't Lock", "Usage");
            for (MemoryOnlyStatusMXBean.TableInfo mi : infos)
            {
                System.out.format("%-30s %-30s %10dMB %15dMB %6.0f%%\n", mi.getKs(), mi.getCf(), mi.getUsed() / 0x100000,
                                  mi.getNotAbleToLock() / 0x100000, (100.0 * mi.getUsed()) / mi.getMaxMemoryToLock());
            }
        }
        else
        {
            System.out.format("No MemoryOnlyStrategy tables found.\n");
        }
    }
}
