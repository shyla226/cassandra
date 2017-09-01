/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.apollo.nodesync;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.metrics.NodeSyncMetrics;
import org.apache.cassandra.tools.nodetool.nodesync.RateSimulatorCmd;

public interface NodeSyncServiceMBean
{
    public static final String JMX_GROUP = "com.datastax.nodesync";
    public static final String MBEAN_NAME = String.format("%s:type=%s", JMX_GROUP, "NodeSyncService");

    /**
     * Enables the NodeSync service if it wasn't already running.
     * @return {@code true} if the service was started, {@code false} if it was already running.
     */
    public boolean enable();

    /**
     * Disables the NodeSync service (if it is running) and blocks (indefinitely) on the shutdown completing.
     * <p>
     * One should generally prefer the {@link #disable(boolean, long, TimeUnit)} (and this is what nodetool uses), which
     * amongst other things allow to provide a timeout, but that latter method is not available in JConsole due to the
     * use of {@link TimeUnit}, and so this is a simple replacement for when you just want to quickly disable the
     * service from JConsole. This method does not force the shutdown.
     *
     * @return {@code true} if the service was stopped, {@code false} if it wasn't already running.
     */
    public boolean disable();

    /**
     * Disables the NodeSync service (if it is running)  and blocks on the shutdown completing.
     *
     * @param force whether the shutdown should be forced, which means that ongoing validation will be interrupted and the
     *              service is stopped as quickly as possible. if {@code false}, a clean shutdown is performed where
     *              ongoing NodeSync segments validations are left time to finish so no ongoing work is thrown on the floor.
     *              Note that a clean shutdown shouldn't take long in general and is thus recommended.
     * @param timeout how long the method should wait for the service to report proper shutdown. If the service hasn't
     *                finish shutdown within this timeout, a {@link TimeoutException} is thrown.
     * @param timeoutUnit the unit for {@code timeout}.
     * @return {@code true} if the service was stopped, {@code false} if it wasn't already running.
     */
    public boolean disable(boolean force, long timeout, TimeUnit timeoutUnit) throws TimeoutException;

    /**
     * Returns whether the service is currently running.
     */
    public boolean isRunning();

    /**
     * Sets the validation rate for NodeSync.
     *
     * @param kbPerSecond the new rate to set in kilobytes-per-seconds.
     */
    public void setRate(int kbPerSecond);

    /**
     * Returns the currently "configured" validation rate for NodeSync.
     * <p>
     * Please note that this only return the configured "target" rate of NodeSync but may not necessarily correspond
     * to the rate at which NodeSync is currently operating (which cannot be greater that the value returned by this
     * method by definition, but can be lower if there is little to validate in the cluster or if the node is not
     * able to achieve the configured rate). If you want to know said "live" rate, you should look at the
     * {@link NodeSyncMetrics#dataValidated} metric.
     *
     * @return the configured rate in kilobytes-per-seconds.
     */
    public int getRate();

    /**
     * Starts a user validation.
     * <p>
     * A user validation forces the validation of a particular table over a particular list of local ranges. When
     * started, the validation of all the segments corresponding to the requested table and range will be forced (take
     * priority over any other "normal" NodeSync segment validation).
     * <p>
     * User validations are primary intended for the testing NodeSync (user validations runs even on tables where the
     * {@code nodesync} option is not enabled) or for extreme cases where one wants to manually force the validation
     * of some segments independently of NodeSync automatic segment prioritization.
     *
     * @param options the options for the validation (at least the id, keyspace, table; optionally the ranges on which to
     *                force validation (all local ranges are validated if no specific ranges are provided)). Those
     *                are passed in a string map for JMX sakes but see the comment {@link UserValidationOptions#fromMap}
     *                for details on valid (and mandatory) values for this argument.
     */
    public void startUserValidation(Map<String, String> options);

    /**
     * Starts a user validation.
     * <p>
     * This is a slightly less flexible version of {@link #startUserValidation(Map)} for use through JConsole where we
     * cannot input a map. This call is equivalent to:
     * <pre>
     *     Map<String, String> m = new HashMap();
     *     m.put("id", id);
     *     m.put("keyspace", keyspace);
     *     m.put("table", table);
     *     if (ranges != null && !ranges.isEmpty())
     *         m.put("ranges", ranges);
     *     startUserValidation(m);
     * </pre>
     * and so see the javadoc of {@link #startUserValidation(Map)} for details.
     *
     * @param id an identifier to identify this validation (for cancelling for instance). The only constraint on this
     *           id is that the node shouldn't have another ongoing validation with that ID or the new submission will
     *            be rejected.
     * @param keyspace the name of the keyspace to validate.
     * @param table the name of the table to validate.
     * @param ranges the ranges to validate (see {@link UserValidationOptions} for format) or {@code null} to validate
     *               all local ranges.
     */
    public void startUserValidation(String id, String keyspace, String table, String ranges);

    /**
     * Cancel a user validation given the validation identifier.
     * <p>
     * Cancelling a user validation means that no new segment validation for this user validation will be started. Note
     * however that ongoing segments will not be stopped and so the user validation may still be listed by the system
     * for a short while after this is called.
     *
     * @param id the identifier of the user validation to cancel.
     */
    public void cancelUserValidation(String id);

    /**
     * Returns the serialized-for-JMX information on tables required by the rate simulator.
     * <p>
     * This include, for every NodeSync-enabled table on the node (or all table if {@code includeAllTables} is true),
     * the table replication factor, data size and NodeSync deadline target. The information is build using
     * {@link RateSimulator.Info#compute} and serialized for JMX using {@link RateSimulator.Info#toJMX()}.
     *
     * @param includeAllTables if {@code false}, only NodeSync-enabled tables will be included. Otherwise, all tables
     *                         will be included (note that in any case, the rate simulator only even consider
     *                         NodeSync-enabled tables, but include all tables here allows to "fake-enabled" NodeSync
     *                         on tables in {@link RateSimulatorCmd} for convenience).
     * @return the result of calling {@code RateSimulator.Info.compute().toJMX()}.
     */
    public List<Map<String, String>> getRateSimulatorInfo(boolean includeAllTables);
}
