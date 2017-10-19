/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.nodesync;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple register for ongoing user triggered validations (see {@link UserValidationProposer}).
 * <p>
 * The main purpose of registering user validations here is to be able to easily list them and act on them by id.
 */
class UserValidations
{
    private static final Logger logger = LoggerFactory.getLogger(UserValidations.class);

    private final ValidationScheduler scheduler;
    private final Map<String, UserValidationProposer> proposers = new ConcurrentHashMap<>();

    UserValidations(ValidationScheduler scheduler)
    {
        this.scheduler = scheduler;
    }

    UserValidationProposer createAndStart(UserValidationOptions options)
    {
        UserValidationProposer proposer = UserValidationProposer.create(scheduler.service(), options);

        if (proposers.putIfAbsent(proposer.id(), proposer) != null)
            throw new IllegalStateException(String.format("Cannot submit user validation with identifier %s as that "
                                                          + "identifier is already used by an ongoing validation", proposer.id()));

        proposer.completionFuture().whenComplete((s, e) -> {
            proposers.remove(proposer.id());
            if (e == null || e instanceof CancellationException)
                logger.info("User triggered validation #{} on table {} {}",
                            proposer.id(), options.table, e == null ? "finished successfully" : "was cancelled");
            else
                logger.error("Unexpected error during user triggered validation #{} on table {}",
                             proposer.id(), options.table, e);
        });

        logger.info("Starting user triggered validation #{} on table {}", proposer.id(), options.table);
        scheduler.add(proposer);
        return proposer;
    }

    List<UserValidationProposer> listProposers()
    {
        return ImmutableList.copyOf(proposers.values());
    }

    UserValidationProposer get(String id)
    {
        return proposers.get(id);
    }

    void forceRemove(String id)
    {
        proposers.remove(id);
    }
}
