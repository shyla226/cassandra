/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.bdp.db.utils.concurrent.CompletableFutures;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;

/**
 * Continuously schedules the segment validations to be ran (by an {@link ValidationExecutor}) for NodeSync.
 * <p>
 * There is 2 types of validations that the scheduler deals with:
 * - normal 'continuous' NodeSync validations.
 * - user triggered user validations.
 * <p>
 * For the first kind, the scheduler maintains for each NodeSync-enable table a corresponding
 * {@link ContinuousValidationProposer}. it then "multiplex" the proposal of all those per-table continuous proposer to
 * provide the next validation that should be performed.
 * <p>
 * For the second kind, user validations, those are manually submitted by users through JMX. User validations always
 * take precedence over continuous ones, so continuous validations will basically "pause" as soon as a user validation
 * comes in. The scheduler will then schedule all the validation of that user validation and only resume continuous
 * validations once the user validation is finished. If a new user validation comes while another one is running, it
 * will queue up behind that running validation (and any previously queued user validations).
 * <p>
 * Note that priority between proposals is not decided by this class but is rather directly implemented by the
 * comparison on {@link ValidationProposal}, so this class main job is to merge the proposal from multiple proposers
 * in a way that respects priority and is thread-safe (individual {@link ValidationProposer} are not thread-safe but
 * {@link ValidationExecutor} is multi-threaded).
 * <p>
 * This class and all it's publicly-visible methods <b>ARE</b> thread-safe.
 */
class ValidationScheduler extends SchemaChangeListener implements IEndpointLifecycleSubscriber
{
    private static final Logger logger = LoggerFactory.getLogger(ValidationScheduler.class);

    final NodeSyncState state;

    private final Map<TableId, ContinuousValidationProposer> continuousValidations = new ConcurrentHashMap<>();
    private final PriorityQueue<ContinuousValidationProposer.Proposal> continuousProposals = new PriorityQueue<>();

    private final Map<String, UserValidationProposer> userValidations= new ConcurrentHashMap<>();
    private final Queue<UserValidationProposer> pendingUserValidations = new ArrayDeque<>();
    private UserValidationProposer currentUserValidation;


    // Note: we rely on the lock being re-entrant in queueProposal() below
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition hasProposals = lock.newCondition();

    private final AtomicLong scheduledValidations = new AtomicLong();

    private volatile boolean isShutdown;

    /**
     * Handle asynchronous action for the scheduler, including loading initial {@link TableState} (which is costly-ish,
     * as it reads system tables) and any actions performed on either schema or topology changes.
     * <p>
     * Note that for schema/topology changes in particular, it's a good idea to not do any costly work on the threads on
     * which their callbacks (from {@link SchemaChangeListener}/{@link IEndpointLifecycleSubscriber}) are called, as
     * those may be called on thread where blocking isn't a good idea (typically, {@link Stage#GOSSIP}).
     * <p>
     * Also note that in general the event this executor executes are infrequent and not that latency sensitive, but
     * we could come in burst (typically, at NodeSync startup, we'll need to load the state of all NodeSync-enabled
     * table). So we allow the executor to grow if necessary (to get some parallelism at startup, or when someone enable
     * on all its table typically) but let the threads timeout in general.
     */
    // Note: we want some parallelism when multiple table state has to be loaded (startup for instance) but user can
    // have very many table and we don't want that get out of hand so we use a max size. The choice of that max size
    // is a bit arbitrary tbh.
    private final ExecutorService eventExecutor = DebuggableThreadPoolExecutor.createWithMaximumPoolSize("ValidationSchedulerEventExecutor",
                                                                                                         8,
                                                                                                         60, TimeUnit.SECONDS);

    ValidationScheduler(NodeSyncState state)
    {
        this.state = state;
    }

    /**
     * Add a new user validation.
     * <p>
     * If no other user validation are running, this validation will start immediately and take all NodeSync bandwidth
     * on this node until completion. If another user validations is running, it will be queued.
     *
     * @param options the options user to create the user validation.
     */
    void addUserValidation(UserValidationOptions options)
    {
        UserValidationProposer proposer = UserValidationProposer.create(state, options);
        if (userValidations.putIfAbsent(proposer.id(), proposer) != null)
            throw new IllegalStateException(String.format("Cannot submit user validation with identifier %s as that "
                                                          + "identifier is already used by an ongoing validation", proposer.id()));

        proposer.completionFuture().whenComplete((s, e) -> {
            userValidations.remove(proposer.id());
            if (e == null || e instanceof CancellationException)
                logger.info("User triggered validation #{} on table {} {}",
                            proposer.id(), options.table, e == null ? "finished successfully" : "was cancelled");
            else
                logger.error("Unexpected error during user triggered validation #{} on table {}",
                             proposer.id(), options.table, e);
        });

        lock.lock();
        try
        {
            pendingUserValidations.offer(proposer);
            hasProposals.signalAll();
            // If this user validation is the next in line, it will be executed right away, but otherwise it will wait
            // on the currently running and potentially other pending, so give that feedback to the user
            int pendingBefore = (currentUserValidation == null ? 0 : 1) + pendingUserValidations.size() - 1;
            if (pendingBefore > 0)
                logger.info("Created user triggered validation #{} on table {}: queued after {} other user validations",
                            proposer.id(), options.table, pendingBefore);
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * The continuous proposer for the provider table, if there is one.
     *
     * @param table the table for which to retrieve the continuous proposer.
     * @return the continuous proposer for {@code table} if it is present, {@code null} otherwise.
     */
    @Nullable
    ContinuousValidationProposer getContinuousProposer(TableMetadata table)
    {
        return continuousValidations.get(table.id);
    }

    /**
     * Retrieve a user validation proposer by id.
     *
     * @param id the user validation id.
     * @return the proposer corresponding to {@code id}, or {@code null} if no such user validation is known of this
     * node (which may simply mean that the validation has completed).
     */
    @Nullable
    UserValidationProposer getUserValidation(String id)
    {
        return userValidations.get(id);
    }

    /**
     * Add the provided table ot the list of continuously validated tables. This is a no-op if the table is already
     * continuously validated.
     * <p>
     * As doing so will generally require loading the initial state of the table from the distributed system table, this
     * is done asynchronously on the scheduler {@link #eventExecutor}.
     * <p>
     * Note that if an unknown error happens during the addition, an error is logged by this error already, so that
     * caller thread don't need to.
     *
     * @param table the table to add.
     * @return a future on the completion of the addition of {@code table} to the scheduler. The future will return
     * {@code null} if the table was newly added and the proposer for that table if it was already present.
     */
    private CompletableFuture<ContinuousValidationProposer> addContinuous(TableMetadata table)
    {
        // Fast path if we know we already have the table.
        ContinuousValidationProposer proposer = continuousValidations.get(table.id);
        if (proposer != null)
            return CompletableFuture.completedFuture(proposer);

        return CompletableFuture.supplyAsync(() -> {
            // Outside the lock on purpose: we don't want to serialize state loading.
            TableState tableState = state.getOrLoad(table);
            lock.lock();
            try
            {
                return addContinuousInternal(tableState);
            }
            finally
            {
                lock.unlock();
            }
        }, eventExecutor).exceptionally(t -> {
            // Don't log on races with DROP, it's basically harmless.
            if (!Throwables.isCausedBy(t, UnknownTableException.class))
                logger.error("Unexpected error while starting NodeSync on {} following the table creation; " +
                             "The table will not be validated by NodeSync on this node until this is resolved",
                             table, Throwables.unwrapped(t));
            throw Throwables.cleaned(t);
        });
    }

    /**
     * Adds new tables to the list of continuously validated tables. Any table that was already present will be ignored.
     *
     * @param tables the tables to add.
     * @return a future on the completion of the addition that return the metadata for all table that have actually
     * been added (as any table already present is skipped).
     */
    CompletableFuture<List<TableMetadata>> addAllContinuous(Stream<TableMetadata> tables)
    {
        // We first submit the loading of all state to the eventExecutor in parallel. Once all have completed, we grab
        // the lock to add all the corresponding proposers to the scheduler.
        // Note that if a given table is already loaded, grabbing it's state will be immediate and addContinuousInternal
        // will them simply ignore the table.
        List<CompletableFuture<TableState>> stateLoaders =  tables.map(t -> CompletableFuture.supplyAsync(() -> state.getOrLoad(t),
                                                                                                          eventExecutor))
                                                                  .collect(Collectors.toList());

        if (stateLoaders.isEmpty())
            return CompletableFuture.completedFuture(Collections.emptyList());

        return CompletableFutures.allAsList(stateLoaders).thenApply(states -> {
            lock.lock();
            try
            {
                List<TableMetadata> added = new ArrayList<>(states.size());
                for (TableState state : states)
                {
                    if (addContinuousInternal(state) == null)
                        added.add(state.table());
                }
                return added;
            }
            finally
            {
                lock.unlock();
            }
        });

    }

    /**
     * Removes a table from the list of continuously validated tables. This is a no-op if the scheduler isn't validating
     * the provided table.
     *
     * @param table the table to remove.
     * @return {@code true} if {@code table} was removed, {@code false} if the scheduler didn't had that table.
     */
    private boolean removeContinuous(TableMetadata table)
    {
        lock.lock();
        try
        {
            return cancelAndRemove(table);
        }
        finally
        {
            lock.unlock();
        }
    }

    // *Must* only be called while holding the lock
    private ContinuousValidationProposer addContinuousInternal(TableState tableState)
    {
        TableId id = tableState.table().id;
        // Note that since we're holding the lock, that check is not racy with the following addition
        ContinuousValidationProposer previous = continuousValidations.get(id);
        if (previous != null)
            return previous;

        ContinuousValidationProposer proposer = new ContinuousValidationProposer(tableState, this::queueProposal);
        continuousValidations.put(id, proposer);
        proposer.start();
        return null;
    }

    // *Must* only be called while holding the lock
    private boolean cancelAndRemove(TableMetadata table)
    {
        ContinuousValidationProposer proposer = continuousValidations.remove(table.id);
        if (proposer == null)
            return false;

        proposer.cancel();
        return true;
    }

    /**
     * How many table are currently continuously validated by this scheduler.
     */
    int continuouslyValidatedTables()
    {
        return continuousValidations.size();
    }

    /**
     * How many validations have been scheduled since creation.
     * <p>
     * The main use of this value is that if it don't increase for a given period of time, it means nothing has been
     * done by the scheduler.
     */
    long scheduledValidations()
    {
        return scheduledValidations.get();
    }

    /**
     * Returns the next validation to be ran.
     * <p>
     * This method <b>may</b> block if {@code blockUntilAvailable == true} and there is no validation to be done at the
     * current time (including the case where no table have NodeSync currently activated and so the scheduler has no
     * proposers set) and this until a validation has to be done (there is no timeout).
     *
     * @param blockUntilAvailable if {@code true} and there is no currently available validation to run, the call will
     *                            block until a validation is available. In that case, the method is guaranteed to
     *                            return a non-null value. If {@code false}, this call will not block and will instead
     *                            return {@code null} if no validation is currently available.
     * @return a {@link Validator} corresponding to the next validation to be ran or {@code null} if
     * {@code blockUntilAvailable == false} and there is no validation to run at the current time.
     *
     * @throws ShutdownException if the scheduler has been shutdown.
     */
    Validator getNextValidation(boolean blockUntilAvailable)
    {
        if (isShutdown)
            throw new ShutdownException();

        while (true)
        {
            ValidationProposal proposal = getProposal(blockUntilAvailable);
            // We only get null if !blockUntilAvailable
            if (proposal == null)
                return null;

            // Note: activating a proposal is potentially costly so having that done outside the lock is very much
            // on purpose.
            Validator validator = proposal.activate();
            if (validator != null)
            {
                scheduledValidations.incrementAndGet();
                return validator;
            }
        }
    }

    /**
     * Shutdown this scheduler. The main effect of this call is to make any ongoing and future blocking calls to
     * {@link #getNextValidation} to throw a {@link ShutdownException}.
     */
    void shutdown()
    {
        // We need to set isShutdown, but also unblock any thread that is currently waiting on the hasProposals condition
        lock.lock();
        try
        {
            if (isShutdown)
                return;

            continuousValidations.values().forEach(ValidationProposer::cancel);
            continuousValidations.clear();
            continuousProposals.clear();

            userValidations.values().forEach(ValidationProposer::cancel);
            userValidations.clear();
            pendingUserValidations.clear();
            currentUserValidation = null;

            isShutdown = true;
            hasProposals.signalAll();
        }
        finally
        {
            lock.unlock();
        }

    }

    private ValidationProposal getProposal(boolean blockUntilAvailable)
    {
        ValidationProposal proposal;
        lock.lock();
        try
        {
            while ((proposal = nextProposal()) == null)
            {
                if (!blockUntilAvailable)
                    return null;
                if (isShutdown)
                    throw new ShutdownException();

                hasProposals.awaitUninterruptibly();
            }
            return proposal;
        }
        finally
        {
            lock.unlock();
        }
    }

    // This *must* be called while holding the lock
    private ValidationProposal nextProposal()
    {
        ValidationProposal p = nextUserValidationProposal();
        if (p != null)
            return p;

        return continuousProposals.poll();
    }

    private ValidationProposal nextUserValidationProposal()
    {
        if (currentUserValidation == null || !currentUserValidation.hasNext())
        {
            do
            {
                currentUserValidation = pendingUserValidations.poll();
                if (currentUserValidation == null)
                    return null;

                logger.info("Starting user triggered validation #{} on table {}",
                            currentUserValidation.id(), currentUserValidation.table());

            } while (!currentUserValidation.hasNext());
        }
        return currentUserValidation.next();
    }

    // This needs the lock but may or may not be called while already holding it, so we acquire said lock and rely
    // on reentrancy if it was hold already.
    private void queueProposal(ContinuousValidationProposer.Proposal proposal)
    {
        lock.lock();
        try
        {
            if (isShutdown)
                return;

            continuousProposals.add(proposal);
            hasProposals.signal();
        }
        finally
        {
            lock.unlock();
        }
    }

    /*
     * IEndpointLifecycleSubscriber methods
     *
     * These method handle actions to be taken on topology changes.
     */

    public void onJoinCluster(InetAddress endpoint)
    {
        // On single node cluster, we don't create any proposers since nothing can be validated. So now that we have
        // another node, we should add the proposers for any NodeSync-enabled table. Note that this is not the only situation
        // where we could have 0 proposers, this could also be due to no table being NodeSync-enabled/no keyspace having RF>1.
        // Calling ContinuousValidationProposer.createAll() is harmless in those case however, it will simply return an
        // empty list. And since join events are pretty rare and createAll is pretty cheap particularly in those cases, ...
        if (continuouslyValidatedTables() == 0)
        {
            addAllContinuous(NodeSyncHelpers.nodeSyncEnabledTables()).thenAccept(tables -> {
                if (!tables.isEmpty())
                    logger.info("{} has joined the cluster: starting NodeSync validations on tables {} as consequence",
                                endpoint, tables);
            });
        }
        else
        {
            maybeUpdateLocalRanges();
        }
    }

    private void maybeUpdateLocalRanges()
    {
        // Submit and update to each proposer with the current local ranges. If those haven't changed, the update will
        // simply be a no-op.
        for (ContinuousValidationProposer proposer : continuousValidations.values())
            eventExecutor.submit(() -> proposer.state.update(NodeSyncHelpers.localRanges(proposer.table().keyspace)));
    }


    public void onLeaveCluster(InetAddress endpoint)
    {
        maybeUpdateLocalRanges();
    }

    public void onUp(InetAddress endpoint)
    {
        // Nothing to do for this for now (though we could imagine to look up and re-prioritize segment that have been somewhat
        // recently validated but only partially due to the absence of this node; not trivial to mix that properly with
        // normal prioritization though so not v1 material).
    }

    public void onDown(InetAddress endpoint)
    {
        // Nothing to do for this for now (but as for onUp, we could imagine de-prioritizing (a bit but probably not
        // totally) segments for which this is a replica, so we focus on other segments first, so that if the node is
        // back relatively quickly, we end up limiting the segment we only partially validate but without losing much
        // progress; similarly not v1 material).
    }

    public void onMove(InetAddress endpoint)
    {
        maybeUpdateLocalRanges();
    }

    /*
     * SchemaChangeListener methods.
     *
     * We check for any event that implies some table proposer should be either added or removed.
     *
     * Note: we don't bother with onCreateKeyspace/onDropKeyspace because we'll get individual notifications for any
     * table that is affected by those.
     */

    public void onAlterKeyspace(String keyspace)
    {
        // We should handle RF changes from and to 1. Namely, if the RF is increased from 1, we should consider any
        // table from the keyspace for inclusion, while if it's decreased to 1, we should remove all tables.
        // Note that we actually cannot know what was the RF before the ALTER, only what it is now (something we should
        // change, but that's another story), so we simply blindly add/remove tables depending on the current RF and
        // rely on the fact that ContinuousValidationProposer equality is solely based on the table (by design), and so
        // this will do the right thing.
        Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
        // Shouldn't happen I suppose in practice, but could imagine a race between ALTER and DROP getting us this so
        // no point in failing
        if (ks == null)
            return;

        if (ks.getReplicationStrategy().getReplicationFactor() <= 1)
        {
            Set<TableMetadata> removed = new HashSet<>();
            ks.getColumnFamilyStores().forEach(s -> {
                if (removeContinuous(s.metadata()))
                    removed.add(s.metadata());
            });
            if (!removed.isEmpty())
                logger.info("Stopping NodeSync validations on tables {} because keyspace {} is not replicated anymore",
                            removed, keyspace);
        }
        else
        {
            addAllContinuous(NodeSyncHelpers.nodeSyncEnabledTables(ks)).thenAccept(tables -> {
                if (!tables.isEmpty())
                    // As mentioned above, it's absolutely possible the addition above was a complete no-op, but if it
                    // wasn't, that (almost surely, we could have raced with another change, but that's sufficiently
                    // unlikely that we ignore it for the purpose of logging) means the RF of the keyspace has just
                    // been increased.
                    logger.info("Starting NodeSync validations on tables {} following increase of the replication factor on {}",
                                tables, keyspace);
            });
        }
    }

    public void onCreateTable(String keyspace, String table)
    {
        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(keyspace, table);
        // As always, protect against hypothetical race with a drop, no matter how unlikely this is.
        if (store == null || !NodeSyncHelpers.isNodeSyncEnabled(store))
            return;

        addContinuous(store.metadata()).thenAccept(previous -> {
            if (previous == null)
                logger.info("Starting NodeSync validations on newly created table {}", store.metadata());
        });
    }

    public void onAlterTable(String keyspace, String table, boolean affectsStatements)
    {
        ColumnFamilyStore store = ColumnFamilyStore.getIfExists(keyspace, table);
        // As always, protect against hypothetical race with a drop, no matter how unlikely this is.
        if (store == null)
            return;

        // Don't bother doing anything if we're a one node cluster
        if (StorageService.instance.getTokenMetadata().getAllEndpoints().size() == 1)
            return;

        TableMetadata metadata = store.metadata();
        // Again, we don't really know what was the alter, but we want to add the table if NodeSync just got enabled
        // and remove it if it just got disabled. And as both adding an already present table and removing a non
        // present one are no-ops...
        if (NodeSyncHelpers.isNodeSyncEnabled(store))
        {
            addContinuous(metadata).thenAcceptAsync(previous -> {
                if (previous == null)
                    logger.info("Starting NodeSync validations on table {}: it has been enabled with ALTER TABLE", metadata);
                else
                    previous.state.onTableUpdate();
            }, eventExecutor);
        }
        else
        {
            eventExecutor.submit(() -> {
                if (removeContinuous(metadata))
                    logger.info("Stopping NodeSync validations on table {} following user deactivation", metadata);
            });
        }
    }

    public void onDropTable(String keyspace, String table)
    {
        eventExecutor.execute(() -> {
            // It's annoying because we don't get the TableMetadata/TableId and can't fetch it anymore due to the drop
            // (this is a downside of the current SchemaListener interface; could and should be fixed). So we have to
            // iterate.
            continuousValidations.values()
                                 .stream()
                                 .filter(p -> p.table().keyspace.equals(keyspace) && p.table().name.equals(table))
                                 .map(p -> p.table().id)
                                 .findAny()
                                 .ifPresent(id -> {
                                    continuousValidations.remove(id);
                                    // Logging at debug because when you explicitly dropped a table, it doesn't feel like you'd care too much
                                    // about that confirmation. Further, when a keyspace is dropped, this is called for every table it has
                                    // and this would feel like log spamming if the keyspace has very many tables.
                                    logger.debug("Stopping NodeSync validations on table {}.{} as the table has been dropped", keyspace, table);
                                });

            Iterator<UserValidationProposer> iter = userValidations.values().iterator();
            while (iter.hasNext())
            {
                UserValidationProposer proposer = iter.next();
                if (proposer.table().keyspace.equals(keyspace) && proposer.table().name.equals(table))
                {
                    proposer.cancel();
                    iter.remove();
                    // Note: we don't remove from the pending queue, but as we cancelled the proposer, this will be
                    // dealt with automatically
                }
            }
        });
    }

    /**
     * Thrown by {@link #getNextValidation} when the scheduler has been shutdown.
     */
    static class ShutdownException extends RuntimeException {}
}
