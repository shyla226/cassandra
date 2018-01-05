/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.tools.nodesync;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.datastax.bdp.db.nodesync.ValidationOutcome;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.TimeValue;
import org.apache.cassandra.utils.units.Units;

import static com.datastax.bdp.db.nodesync.UserValidationProposer.Status;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.reducing;
import static java.util.stream.Collectors.toList;
import static org.apache.cassandra.repair.SystemDistributedKeyspace.NODESYNC_USER_VALIDATIONS;
import static org.apache.cassandra.schema.SchemaConstants.DISTRIBUTED_KEYSPACE_NAME;

/**
 * {@link NodeSyncCommand} to list user validations.
 * <p>
 * By default it only shows the running validations, but the {@code -a/--all} flag allows to show also the finished
 * validations.
 */
@Command(name = "list", description = "List user validations. By default, only running validations are displayed.")
public class ListValidations extends NodeSyncCommand
{
    private static final String ID = "Identifier";
    private static final String TABLE = "Table";
    private static final String STATUS = "Status";
    private static final String OUTCOME = "Outcome";
    private static final String DURATION = "Duration";
    private static final String ETA = "ETA";
    private static final String PROGRESS = "Progress";
    private static final String VALIDATED = "Validated";
    private static final String REPAIRED = "Repaired";

    private static final String FORMAT = "%%-%ds  %%-%ds  %%-%ds  %%-%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds%n";

    @Option(type = OptionType.COMMAND,
            name = { "-a", "--all" },
            description = "List all either running or finished validations since less then 1 day")
    private boolean all = false;

    @Override
    public final void execute(Metadata metadata, Session session, NodeProbes probes)
    {
        // Collect validations from the system_distributed table and aggregate them grouping by id and table.
        // Note that, although the validation id is warranted to be unique in each node, different nodes can have
        // validations with the same id but for different tables.
        Select select = QueryBuilder.select("id",
                                            "keyspace_name",
                                            "table_name",
                                            "status",
                                            "outcomes",
                                            "started_at",
                                            "ended_at",
                                            "segments_to_validate",
                                            "segments_validated",
                                            "metrics")
                                    .from(DISTRIBUTED_KEYSPACE_NAME, NODESYNC_USER_VALIDATIONS);

        Predicate<Row> statusFilter = r -> all || Objects.equals(r.getString("status"), Status.RUNNING.toString());

        List<Validation> validations = Streams.of(session.execute(select))
                                              .filter(statusFilter)
                                              .map(Validation::fromRow)
                                              .collect(groupingBy(v -> v.key, reducing(Validation::combineWith)))
                                              .values()
                                              .stream()
                                              .filter(Optional::isPresent)
                                              .map(Optional::get)
                                              .collect(toList());

        // Build the table to be displayed
        List<Map<String, String>> table = new ArrayList<>(validations.size());
        for (Validation validation : validations)
        {
            Map<String, String> map = new HashMap<>(6);
            map.put(ID, validation.key.id);
            map.put(TABLE, fullyQualifiedTableName(validation.key.keyspace, validation.key.table));
            map.put(STATUS, validation.status.toString());
            map.put(OUTCOME, validation.summarizedOutcome());
            map.put(DURATION, validation.duration().toString());
            map.put(ETA, validation.eta());
            map.put(PROGRESS, validation.progress() + "%");
            map.put(VALIDATED, Units.toString(validation.bytesValidated, SizeUnit.BYTES));
            map.put(REPAIRED, Units.toString(validation.bytesRepaired, SizeUnit.BYTES));
            table.add(map);
        }

        // Prepare the string format line with the widths of the table columns
        String format = String.format(FORMAT,
                                      columnWidth(ID, table),
                                      columnWidth(TABLE, table),
                                      columnWidth(STATUS, table),
                                      columnWidth(OUTCOME, table),
                                      columnWidth(DURATION, table),
                                      columnWidth(ETA, table),
                                      columnWidth(PROGRESS, table),
                                      columnWidth(VALIDATED, table),
                                      columnWidth(REPAIRED, table));

        System.out.println();
        System.out.printf(format, ID, TABLE, STATUS, OUTCOME, DURATION, ETA, PROGRESS, VALIDATED, REPAIRED);
        System.out.println();
        table.forEach(m -> System.out.printf(format,
                                             m.get(ID),
                                             m.get(TABLE),
                                             m.get(STATUS),
                                             m.get(OUTCOME),
                                             m.get(DURATION),
                                             m.get(ETA),
                                             m.get(PROGRESS),
                                             m.get(VALIDATED),
                                             m.get(REPAIRED)));
        System.out.println();
    }

    private static int columnWidth(String columnName, List<Map<String, String>> list)
    {
        int maxValueLength = list.stream().map(m -> m.get(columnName)).mapToInt(String::length).max().orElse(0);
        return Math.max(maxValueLength, columnName.length());
    }

    /**
     * Class representing a cluster-wide user-triggered validation.
     */
    private static class Validation
    {
        private final Key key;
        private final Status status;
        private final Map<String, Long> outcomes;
        private final Date startTime, endTime;
        private final long segmentsToValidate, segmentsValidated;
        private final long bytesValidated, bytesRepaired;

        private Validation(Key key,
                           Status status,
                           Map<String, Long> outcomes,
                           Date startTime,
                           Date endTime,
                           long segmentsToValidate,
                           long segmentsValidated,
                           long bytesValidated,
                           long bytesRepaired)
        {
            this.key = key;
            this.status = status;
            this.outcomes = outcomes;
            this.startTime = startTime;
            this.endTime = endTime;
            this.segmentsToValidate = segmentsToValidate;
            this.segmentsValidated = segmentsValidated;
            this.bytesValidated = bytesValidated;
            this.bytesRepaired = bytesRepaired;
        }

        /**
         * The progress (as a percentage) of this validation.
         *
         * @return a value between 0 and 100 that represents the percentage of validated segments over the total amount
         * of segments to be validated.
         */
        int progress()
        {
            int p = (int) (((double) segmentsValidated / segmentsToValidate) * 100d);
            return Math.max(0, Math.min(100, p));
        }

        /**
         * The amount of time the validation has been running.
         */
        TimeValue duration()
        {
            long endMillis = status == Status.RUNNING ? System.currentTimeMillis() : endTime.getTime();
            long durationMillis = startTime == null ? 0 : endMillis - startTime.getTime();
            return TimeValue.of(durationMillis, TimeUnit.MILLISECONDS);
        }

        /**
         * The estimated amount of time for termination.
         *
         * @return the amount of time for termination, {@code ?} if the estimation is not available, or {@code -} if the
         * validation is not running.
         */
        String eta()
        {
            if (status != Status.RUNNING)
                return "-";

            int progress = progress();
            if (progress == 0)
                return "?";

            long durationMillis = duration().in(TimeUnit.MILLISECONDS);
            long etaMillis = (durationMillis * 100 / progress) - durationMillis;
            return TimeValue.of(etaMillis, TimeUnit.MILLISECONDS).toString();
        }

        private boolean hasOutcome(ValidationOutcome outcome)
        {
            Long value = outcomes.get(outcome.toString());
            return value != null && value > 0;
        }

        /**
         * The summarized outcome of this table, based on the aggregation of the validation outcomes.
         */
        String summarizedOutcome()
        {
            if (hasOutcome(ValidationOutcome.UNCOMPLETED))
                return "uncompleted";

            if (hasOutcome(ValidationOutcome.FAILED))
                return "failed";

            if (hasOutcome(ValidationOutcome.PARTIAL_IN_SYNC)
                || hasOutcome(ValidationOutcome.PARTIAL_REPAIRED))
                return "partial";

            return "success";
        }

        /**
         * Combines this with another validation with the same {@link Key key}, presumably coming form other node.
         *
         * @param other a validation with the same {@link Key key}
         * @return a new combined validation built from the aggregation of this and {@code other}
         */
        Validation combineWith(Validation other)
        {
            if (other == null)
                return this;

            assert key.equals(other.key);

            Date combinedStartTime;
            if (startTime == null)
                combinedStartTime = other.startTime;
            else if (other.startTime == null)
                combinedStartTime = startTime;
            else
                combinedStartTime = startTime.before(other.startTime) ? startTime : other.startTime;

            Date combinedEndTime;
            if (endTime == null)
                combinedEndTime = other.endTime;
            else if (other.endTime == null)
                combinedEndTime = endTime;
            else
                combinedEndTime = endTime.after(other.endTime) ? endTime : other.endTime;

            Map<String, Long> combinedOutcomes = new HashMap<>(outcomes);
            for (ValidationOutcome outcome : ValidationOutcome.values())
            {
                String key = outcome.toString();
                Long v1 = outcomes.get(key);
                Long v2 = other.outcomes.get(key);

                if (v1 != null && v2 != null)
                    combinedOutcomes.put(key, v1 + v2);
                else if (v1 != null)
                    combinedOutcomes.put(key, v1);
                else if (v2 != null)
                    combinedOutcomes.put(key, v2);
            }

            return new Validation(key,
                                  status.combineWith(other.status),
                                  combinedOutcomes,
                                  combinedStartTime,
                                  combinedEndTime,
                                  segmentsToValidate + other.segmentsToValidate,
                                  segmentsValidated + other.segmentsValidated,
                                  bytesValidated + other.bytesValidated,
                                  bytesRepaired + other.bytesRepaired);
        }

        private static Validation fromRow(Row row)
        {
            UDTValue metrics = row.getUDTValue("metrics");
            return new Validation(new Validation.Key(row.getString("id"),
                                                     row.getString("keyspace_name"),
                                                     row.getString("table_name")),
                                  Status.from(row.getString("status")),
                                  row.getMap("outcomes", String.class, Long.class),
                                  row.getTimestamp("started_at"),
                                  row.getTimestamp("ended_at"),
                                  row.getLong("segments_to_validate"),
                                  row.getLong("segments_validated"),
                                  metrics == null ? 0 : metrics.getLong("data_validated"),
                                  metrics == null ? 0 : metrics.getLong("data_repaired"));
        }

        /**
         * The cluster-wide unique identifying key of a validation, formed by its id, keyspace and table.
         */
        private static class Key
        {
            private final String id;
            private final String keyspace;
            private final String table;

            public Key(String id, String keyspace, String table)
            {
                this.id = id;
                this.keyspace = keyspace;
                this.table = table;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;

                Key that = (Key) o;
                return id.equals(that.id) && keyspace.equals(that.keyspace) && table.equals(that.table);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(id, keyspace, table);
            }
        }
    }
}