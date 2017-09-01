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
package org.apache.cassandra.tools.nodetool.nodesync;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;

import com.datastax.apollo.nodesync.RateSimulator;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.units.TimeValue;

@Command(name = "ratesimulator", description = "Simulate rates necessary to achieve NodeSync deadline based on configurable assumptions ")
public class RateSimulatorCmd extends NodeTool.NodeToolCmd
{
    private static final Splitter SPLIT_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
    private static final Splitter SPLIT_ON_COLON = Splitter.on(':').trimResults().omitEmptyStrings();
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("((?<k>(\\w+|\"\\w+\"))\\.)?(?<t>(\\w+|\"\\w+\"))");

    private enum SubCommand
    {
        HELP,
        SIMULATE,
        THEORETICAL_MINIMUM,
        RECOMMENDED_MINIMUM,
        RECOMMENDED;

        @Override
        public String toString()
        {
            return super.toString().toLowerCase();
        }
    }

    @Arguments(title = "sub-command", usage = "<sub-command>",
               description = "Simulator sub-command: use 'help' (the default if unset) for a listing of all available sub-commands.")
    private String subCommand = null;

    @Option(title = "factor", name = { "-sg", "--size-growth-factor"},
            description = "by how much to increase data sizes to account for data grow; only for the 'simulate' sub-command.")
    private Float sizeGrowthFactor = null;

    @Option(title = "factor", name = { "-ds", "--deadline-safety-factor"},
            description = "by how much to decrease table deadlines to account for imperfect conditions; only for the 'simulate' sub-command.")
    private Float deadlineSafetyFactor = null;

    @Option(title = "factor", name = { "-rs", "--rate-safety-factor"},
            description = "By how much to increase the final rate to account for imperfect conditions; only for the 'simulate' sub-command.")
    private Float rateSafetyFactor = null;

    @Option(title = "overrides", name = { "--deadline-overrides"},
            description = "Allow override the configure deadline for some/all of the tables in the simulation.")
    private String deadlineOverrides = null;

    @Option(title = "ignore replication factor", name = { "--ignore-replication-factor"},
            description = "Don't take the replication factor in the simulation.")
    private boolean ignoreReplicationFactor = false;

    @Option(title = "includes", name = { "-i", "--includes"},
    description = "A comma-separated list of tables to include in the simulation even if NodeSync is not enabled server-side; " +
                  "this allow to simulate the impact on the rate of enabling NodeSync on those tables.")
    private String includes = null;

    @Option(title = "excludes", name = { "-e", "--excludes"},
    description = "A comma-separated list of tables to exclude tables from the simulation even if NodeSync is enabled server-side; " +
                  "this allow to simulate the impact on the rate of disabling NodeSync on those tables.")
    private String excludes = null;

    @Option(title = "verbose output", name = { "-v", "--verbose"},
            description = "Turn on verbose output, giving details on how the simulation is carried out.")
    private boolean verbose = false;

    @Override
    public void execute(NodeProbe probe)
    {
        SubCommand cmd = parseSubCommand();
        if (cmd == SubCommand.HELP)
        {
            printHelp();
            return;
        }

        RateSimulator.Info info = RateSimulator.Info.fromJMX(probe.getNodeSyncRateSimulatorInfo(includes != null));

        try
        {
            info = withIncludes(info);
            info = withExcludes(info);
            info = withOverrides(info);
        }
        catch (IllegalArgumentException e)
        {
            printError(e.getMessage());
            System.exit(1);
            throw new AssertionError();
        }

        if (info.isEmpty())
        {
            // TODO: Should probably gather info on all tables so we can allow including table without NodeSync
            // enabled yet, so people can see the impact on the rate adding them would have.
            print("No tables have NodeSync enabled; nothing to simulate");
            return;
        }

        RateSimulator simulator = new RateSimulator(info, getSimulationParameters(cmd));
        if (ignoreReplicationFactor)
            simulator.ignoreReplicationFactor();
        if (verbose)
        {
            simulator.withLogger(this::print);
            simulator.computeRate();
        }
        else
        {
            print("Computed rate: %s.", simulator.computeRate());
        }
    }

    private SubCommand parseSubCommand()
    {
        if (subCommand == null)
            return SubCommand.HELP;

        try
        {
            return SubCommand.valueOf(subCommand.trim().toUpperCase());
        }
        catch (IllegalArgumentException e)
        {
            printError("Unknown sub-command '%s' for the rate simulator; use 'help' "
                       + "('nodetool nodesyncservice ratesimulator help') for details on available sub-commands",
                       subCommand);
            System.exit(1);
            throw new AssertionError();
        }
    }

    private RateSimulator.Parameters getSimulationParameters(SubCommand cmd)
    {
        if (cmd == SubCommand.SIMULATE)
        {
            checkSet(sizeGrowthFactor, "-sg/--size-growth-factor");
            checkSet(deadlineSafetyFactor, "-ds/--deadline-safety-factor");
            checkSet(rateSafetyFactor, "-rs/--rate-safety-factor");
            return RateSimulator.Parameters.builder()
                                           .sizeGrowingFactor(sizeGrowthFactor)
                                           .deadlineSafetyFactor(deadlineSafetyFactor)
                                           .rateSafetyFactor(rateSafetyFactor)
                                           .build();
        }

        checkUnset(sizeGrowthFactor, cmd,"-sg/--size-growth-factor");
        checkUnset(deadlineSafetyFactor, cmd,"-ds/--deadline-safety-factor");
        checkUnset(rateSafetyFactor, cmd,"-rs/--rate-safety-factor");

        switch (cmd)
        {
            case THEORETICAL_MINIMUM: return RateSimulator.Parameters.THEORETICAL_MINIMUM;
            case RECOMMENDED_MINIMUM: return RateSimulator.Parameters.MINIMUM_RECOMMENDED;
            case RECOMMENDED: return RateSimulator.Parameters.RECOMMENDED;
            default: throw new AssertionError();
        }
    }

    private RateSimulator.Info withOverrides(RateSimulator.Info info)
    {
        if (deadlineOverrides == null || deadlineOverrides.isEmpty())
            return info;

        Set<String> knownTables = Streams.of(info.tables()).map(RateSimulator.TableInfo::tableName).collect(Collectors.toSet());
        long catchAllOverride = -1;
        Map<String, Long> overrides = new HashMap<>();
        for (String s : SPLIT_ON_COMMA.split(deadlineOverrides))
        {
            List<String> l = SPLIT_ON_COLON.splitToList(s);
            if (l.size() != 2)
                throw new IllegalArgumentException(String.format("Invalid deadline override '%s' in '%s'", s, deadlineOverrides));

            String tableName = l.get(0);
            long deadlineValue = parseDeadlineValue(l.get(1), l.get(0));

            if (tableName.equals("*"))
            {
                if (catchAllOverride > 0)
                    throw new IllegalArgumentException(String.format("Duplicate entry for '*' found in '%s'", deadlineOverrides));
                catchAllOverride = deadlineValue;
            }
            else
            {
                if (!TABLE_NAME_PATTERN.matcher(tableName).matches())
                    throw new IllegalArgumentException(String.format("Invalid table name '%s' in '%s'", tableName, deadlineOverrides));
                if (!knownTables.contains(tableName))
                    throw new IllegalArgumentException(String.format("Table %s doesn't appear to be a NodeSync-enabled table", tableName));
                overrides.put(tableName, deadlineValue);
            }
        }

        final long override = catchAllOverride;
        return info.transform(t -> {
            Long v = overrides.get(t.tableName());
            if (v != null && !t.isNodeSyncEnabled)
                throw new IllegalArgumentException(String.format("Deadline override for %s, but it is not included " +
                                                                 "in the rate simulation (doesn't have nodesyn enabled " +
                                                                 "server side, and is not part of -i/--includes)", t.tableName()));
            long tableOverride = v == null ? override : v;
            return tableOverride < 0 ? t : t.withNewDeadline(TimeValue.of(tableOverride, TimeUnit.SECONDS));
        });
    }

    private long parseDeadlineValue(String v, String table)
    {
        TimeUnit unit = TimeUnit.SECONDS;
        switch (v.charAt(v.length() - 1))
        {
            case 'm':
                unit = TimeUnit.MINUTES;
                v = v.substring(0, v.length() - 1);
                break;
            case 'h':
                unit = TimeUnit.HOURS;
                v = v.substring(0, v.length() - 1);
                break;
            case 'd':
                unit = TimeUnit.DAYS;
                v = v.substring(0, v.length() - 1);
                break;
        }
        try
        {
            return unit.toSeconds(Long.parseLong(v));
        }
        catch (NumberFormatException e)
        {
            throw new IllegalArgumentException(String.format("Cannot parse deadline from '%s' for table %s", v, table));
        }
    }

    private RateSimulator.Info withIncludes(RateSimulator.Info info)
    {
        if (includes == null)
            return info;

        Set<String> toInclude = parseTableNames(includes);
        RateSimulator.Info newInfo = info.transform(t -> toInclude.remove(t.tableName()) ? t.withNodeSyncEnabled() : t);
        if (!toInclude.isEmpty())
            throw new IllegalArgumentException("Unknown tables listed in -i/--includes: " + toInclude);
        return newInfo;
    }

    private RateSimulator.Info withExcludes(RateSimulator.Info info)
    {
        if (excludes == null)
            return info;

        Set<String> toExclude = parseTableNames(excludes);
        RateSimulator.Info newInfo = info.transform(t -> toExclude.remove(t.tableName()) ? t.withoutNodeSyncEnabled() : t);
        if (!toExclude.isEmpty())
            throw new IllegalArgumentException("Unknown or not-NodeSync-enabled tables listed in -e/--excludes: " + toExclude);
        return newInfo;
    }

    private Set<String> parseTableNames(String str)
    {
        Set<String> names = new HashSet<>();
        for (String name : SPLIT_ON_COMMA.split(str))
        {
            if (!TABLE_NAME_PATTERN.matcher(name).matches())
                throw new IllegalArgumentException(String.format("Invalid table name '%s' in '%s'", name, str));
            names.add(name);
        }
        return names;
    }

    private void checkUnset(Float factor, SubCommand cmd, String option)
    {
        if (factor == null)
            return;

        printError("Cannot use %s for the %s sub-command; this can only be used with the %s sub-command",
                   option, cmd, SubCommand.SIMULATE);
        System.exit(1);
    }

    private void checkSet(Float factor, String option)
    {
        if (factor != null)
            return;

        printError("Missing mandatory option %s for the %s sub-command", option, SubCommand.SIMULATE);
        System.exit(1);
    }

    private void print(String format, Object... args)
    {
        if (args.length == 0)
            System.out.println(format);
        else
            System.out.println(String.format(format, args));
    }

    private void printError(String format, Object... args)
    {
        if (args.length == 0)
            System.err.println(format);
        else
            System.err.println(String.format(format, args));
    }

    private void printHelp()
    {
        // Yes, that is very very ugly. It would be better to put this in a file somewhere and simply "cat" it
        // here, and we should consider that later. I'm really unsure however how to make sure this get picked up in
        // all the ways we package DSE, and  with the rush for 6.0, figuring that out is a lot more work than just
        // putting it all here, and a lot less risky.

        print("NodeSync Rate Simulator");
        print("=======================");
        print("");
        print("The NodeSync rate simulator helps in the configuration of the validation rate of");
        print("the NodeSync service by computing the rate necessary for NodeSync to validate");
        print("all tables within their allowed deadlines (the NodeSync 'deadline_target_sec'");
        print("table option) taking a number of parameters into account.");
        print("");
        print("There is unfortunately no perfect value for the validation rate because NodeSync");
        print("has to deal with many imponderables. Typically, when a node fail, it won't");
        print("participate in NodeSync validation while it is offline, which impact the overall");
        print("rate, but failures cannot by nature be fully predicted.  Similarly, some node");
        print("may not achieve the configured rate at all time in period of overload, or due to");
        print("various unexpected events. Lastly, the rate required to repair all tables within");
        print("a fixed amount of time directly depends on the size of the data to validate,");
        print("which is generally a moving target. One should thus build safety margins within");
        print("the configured rate and this tool helps with this.");
        print("");
        print("");
        print("Sub-commands");
        print("------------");
        print("");
        print("The simulator supports the following sub-commands:");
        print("");
        print("  help: display this help message.");
        print("  simulate: simulates the rate corresponding to he parameters provided as");
        print("    options (see below for details).");
        print("  recommended: simulates a recommended 'default' rate, one that consider data");
        print("    growing up to double the current size, and has healthy margin to account");
        print("    for failures and other events. ");
        print("  recommended_minimum: simulates the minimum rate that is recommended. Note that");
        print("    this is truly a minimum, which assume barely any increase in data size and");
        print("    a fairly healthy cluster (little failures, mostly sustained rate). If you");
        print("    are new to NodeSync and/or unsure, we advise starting with the 'recommended'");
        print("    sub-command instead.");
        print("  theoretical_minimum: simulates the minimum theoretical rate that could");
        print("    possibly allow to validate all NodeSync-enabled tables within their");
        print("    respective deadlines, assuming no data-grow, no failure and a perfectly");
        print("    sustained rate. This is purely an indicative value: those assumption are");
        print("    unrealistic and one should *not* use the rate this return in practice.");
        print("");
        print("The rate computed by the 'recommended' simulation is a good starting point for");
        print("new comers, but please keep in mind that this is largely indicative and cannot");
        print("be a substitute for monitoring NodeSync and adjusting the rate if necessary.");
        print("");
        print("Further, those recommended value are likely to be too low on new and almost");
        print("empty clusters. Indeed, the size of data on such cluster may initially grow");
        print("with a high multiplicative factor (Loading 100GB of data rapidly in a 1MB");
        print("initial cluster, the 'recommended' value at 1MB will be way below what is");
        print("needed at 100GB). In such cases, consider using the 'simulate' sub-command to");
        print("perform a simulation with parameters tailored to your own needs.");
        print("");
        print("");
        print("Simulation");
        print("----------");
        print("");
        print("To perform a simulation, the simulator retrieves from the connected nodes");
        print("information on all NodeSync-enabled tables, including their current data size");
        print("and the value of their 'deadline_target_sec' property. Then, the minimum viable");
        print("rate is computed using the following parameters:");
        print("");
        print("  'size growth factor' (-sg/--size-growth-factor): by how much to increase the");
        print("    current size to account for data size. For instance, a factor of 0.5 will");
        print("    compute a rate that is suitable up to data growing 50%, 1.0 will be suitable");
        print("    for doubling data, etc.");
        print("  'deadline safety factor' (-ds/--deadline-safety-factor): by how much to");
        print("    decrease each table deadline target in the computation. For example, a");
        print("    factor of 0.25 will compute a rate such that in perfect condition, each");
        print("    table is validated within 75% of their full deadline target.");
        print("  'rate safety factor' (-rs/--rate-safety-factor): a final factor by which the");
        print("    computed rate is increased as a safety margin. For instance, a 10% factor");
        print("    will return a rate that 10% bigger than with a 0% factor.");
        print("");
        print("Note that the parameters above should be provided for the 'simulate' sub-command");
        print("but couldn't/shouldn't for other sub-command, as those provide simulations based");
        print("on pre-defined parameters.");
        print("");
        print("Lastly, all simulations (all sub-commands) can also be influence by the following");
        print("options:");
        print("");
        print("  -v/--verbose: Display all steps taken by the simulation. This is a useful");
        print("    option to understand the simulations, but can be very verbose with many");
        print("    tables.");
        print("  -i/--includes: takes a comma-separated list of table names that doesn't have");
        print("    NodeSync enabled server-side but should be included in the simulation");
        print("    nonetheless. This allow to simulate the impact enabling NodeSync on those");
        print("    tables would have on the rate.");
        print("  -e/--excludes: takes a comma-separated list of table names that have NodeSync");
        print("    enabled server-side but should not be included in the simulation regardless.");
        print("    This allow to simulate the impact disabling NodeSync on those tables would");
        print("    have on the rate.");
        print("  --ignore-replication-factor: ignores the replication factor in the simulation.");
        print("    By default, the simulator assumes NodeSync runs on every node of the cluster");
        print("    (which is highly recommended), and so that validation work is spread amongst");
        print("    replica and as such, each node only has to validate 1/RF of the data it");
        print("    owns. This option remove that assumption, computing a rate that takes the");
        print("    totally of the data the node stores into account.");
        print("  -do/--deadline-overrides=<overrides>: by default, the simulator consider each");
        print("    table must be validated within the table 'deadline_target_sec' option. This");
        print("    option allows to simulate the impact on the rate of changing that option for");
        print("    some (or all) of the tables. When provided, <overrides> must be a comma");
        print("    separated list of <table>:<deadline> pairs, where <table> should be a fully");
        print("    qualified table name and <deadline> the deadline target to use for that");
        print("    table in the simulation (in seconds by default, but can be followed by a");
        print("    single character unit for convenient: 'm' for minutes, 'h' for hours or 'd'");
        print("    for days). Optionally, the special character '*' can be used in lieu of");
        print("    <table> to define a deadline for 'all other' tables (if not present, any");
        print("    table no on the override list will use the deadline configured on the table");
        print("    as usual). So for instance:");
        print("      --deadline-overrides 'ks.foo:20h,*:10d'");
        print("    will simulate using a 20 hours deadline for the 'ks.foo' table, and a 10");
        print("    days deadline for any other tables.");
    }
}
