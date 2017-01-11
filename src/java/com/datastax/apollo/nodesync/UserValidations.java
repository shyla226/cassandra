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
package com.datastax.apollo.nodesync;

import java.util.List;
import java.util.Map;
import java.util.UUID;
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
    private final Map<UUID, UserValidationProposer> proposers = new ConcurrentHashMap<>();

    UserValidations(ValidationScheduler scheduler)
    {
        this.scheduler = scheduler;
    }

    UserValidationProposer createAndStart(UserValidationOptions options)
    {
        UserValidationProposer proposer = UserValidationProposer.create(scheduler.service(), options);

        proposers.put(proposer.id(), proposer);
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

    UserValidationProposer get(UUID id)
    {
        return proposers.get(id);
    }

    void forceRemove(UUID id)
    {
        proposers.remove(id);
    }
}
