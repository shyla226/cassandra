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
package org.apache.cassandra.net.interceptors;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.GossipVerbs;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.Verbs;

/**
 * An abstract class for writing interceptors that groups a number of common options.
 *
 * <h2>Options</h2>
 *
 * Interceptors that subclass {@code AbstractInterceptor} share a number of startup options.
 *
 * <h3>Configuring intercepted messages</h3>
 *
 * Which messages are intercepted can be configured through the {@code -Ddse.net.interceptors.intercepted} option.
 * That option takes a comma separated list of the verb groups to intercept, i.e a name corresponding to one of
 * {@link Verbs.Group} value. It is also possible to pass a single {@link Verb} by adding the verb name after the group
 * separated by a '.' (so "GOSSIP.SYN" refers to {@link GossipVerbs#SYN} for instance).
 * <p>
 * Examples:
 * <pre>
 *   -Ddse.net.interceptors.intercepted="WRITES,READS" // intercept all non-LWT writes and reads
 *   -Ddse.net.interceptors.intercepted="GOSSIP,OPERATIONS.TRUNCATE" // intercept all gossip messages and the truncate one
 * </pre>
 * <p>
 * On top of the group/verb, users can configure 3 others criteria to select which messages are interpreted:
 * <ul>
 *     <li>The "type" of message, request or response. This is controller by {@code -Ddse.net.interceptors.intercepted_types}
 *     and can be one or both of {@code REQUEST} and {@code RESPONSE}.</li>
 *     <li>The "direction" of message, sent or received, to control if the node should intercept message it sends, message
 *     it receive, or both. This is controller by {@code -Ddse.net.interceptors.intercepted_directions} and can be one
 *     or both of {@code SENDING} and {@code RECEIVING}.</li>
 *     <li>The "locality" of the message, local or remote, to control if we intercept only locally delivered message, only
 *     ones sent to/received from remote nodes, or both. This is controller by
 *     {@code -Ddse.net.interceptors.intercepted_localities} and can be one or both of {@code LOCAL} and {@code REMOTE}.</li>
 * </ul>
 * Note that for all of those 3 criteria, most interceptors will use decent default and those are often unnecessary to
 * configure.
 *
 * <h3>Disabling on startup</h3>
 *
 * By default, interceptors from this class are enabled on startup. If you want them disabled instead (to enable them
 * manually later through JMX, see below), you can use {@code -Ddse.net.interceptors.disable_on_startup="true"}.
 *
 * <h3>Random interception</h3>
 *
 * By default, all messages configured to be intercepted are intercepted. It is however possible to configure "random
 * interception" so that only a configured ratio of messages are intercepted (this is a ratio of the message configured
 * with {@code -Ddse.net.interceptors.intercepted}).
 *
 * This can be enabled with {@code -Ddse.net.interceptors.interception_chance} which then configure the ratio
 * of messages that are actually intercepted over all the message it is configured to intercept.
 *
 * Examples:
 * <pre>
 *   -Ddse.net.interceptors.interception_chance=0.5 // intercepts half of the messages
 *   -Ddse.net.interceptors.interception_chance=0.9 // intercepts 90% of the messages
 *   -Ddse.net.interceptors.interception_chance=0   // intercepts no message (effectively disabling the interceptor)
 *   -Ddse.net.interceptors.interception_chance=1   // intercepts all messages (disabling random interception)
 * </pre>
 * Note that random interception can be disabled by setting the ratio to 1.
 *
 * <h2>JMX</h2>
 *
 * Interceptors that subclass {@code AbstractInterceptor} expose a number of metrics and operations over JMX. Those are
 * listed in {@link AbstractInterceptorMBean} and covers amongst other things enabling/disabling the interceptor and
 * setting options at runtime.
 *
 * The MBeans are in the "com.datastax" JMX domain and share the "Interceptors" type.
 */
public abstract class AbstractInterceptor implements Interceptor, AbstractInterceptorMBean
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractInterceptor.class);

    private static final String INTERCEPTED_PROPERTY = "intercepted";

    private static final String INTERCEPTED_TYPES = "intercepted_types";
    private static final String INTERCEPTED_DIRECTIONS = "intercepted_directions";
    private static final String INTERCEPTED_LOCALITIES = "intercepted_localities";

    private static final String DISABLE_ON_STARTUP_PROPERTY = "disable_on_startup";
    private static final String RANDOM_INTERCEPTION = "random_interception_ratio";

    private final String name;

    // Verbs we intercept, anything else just pass through
    private volatile ImmutableSet<Verb<?, ?>> interceptedVerbs;
    // Types of message we intercept, anything else just pass through
    private volatile ImmutableSet<Message.Type> interceptedTypes;
    // Message "directions" we intercept, anything else just pass through
    private volatile ImmutableSet<MessageDirection> interceptedDirections;
    // Message "localities" we intercept, anything else just pass through
    private volatile ImmutableSet<Message.Locality> interceptedLocalities;

    private volatile boolean enabled;
    private volatile float interceptionChance;

    private final AtomicLong seen = new AtomicLong();
    private final AtomicLong intercepted = new AtomicLong();

    protected AbstractInterceptor(String name,
                                  ImmutableSet<Verb<?, ?>> interceptedVerbs,
                                  ImmutableSet<Message.Type> interceptedTypes,
                                  ImmutableSet<MessageDirection> interceptedDirections,
                                  ImmutableSet<Message.Locality> interceptedLocalities)
    {
        this.name = name;
        this.interceptedVerbs = interceptedVerbs;
        this.interceptedDirections = interceptedDirections;
        this.interceptedTypes = interceptedTypes;
        this.interceptedLocalities = interceptedLocalities;
        this.enabled = !disabledOnStartup();

        if (allowModifyingIntercepted())
        {
            setFromProperty(INTERCEPTED_PROPERTY, this::setIntercepted);
            setFromProperty(INTERCEPTED_TYPES, this::setInterceptedTypes);
            setFromProperty(INTERCEPTED_DIRECTIONS, this::setInterceptedDirections);
            setFromProperty(INTERCEPTED_LOCALITIES, this::setInterceptedLocalities);
        }

        setInterceptionChance(randomInterceptionRatio());
        registerJMX(name);
    }

    /**
     * Whether the interceptor let what is intercepted be configured through JMX or the runtime {@code intercepted*}
     * properties described above.
     * <p>
     * This is allowed by default but can be overriden by interceptors that can't, by their design, intercept anything
     * else that what they are configured for by default. Those interceptors may still want to extend
     * {@code AbstractInterceptor} as a way to get the other things this provides (metrics through JMX, other properties
     * like {@code enabled}, {@code randomInterceptionRation}, ...).
     */
    protected boolean allowModifyingIntercepted()
    {
        return true;
    }

    private void setFromProperty(String propertyName, Consumer<String> setter)
    {
        String configured = getProperty(propertyName);
        if (configured == null)
            return;

        try
        {
            setter.accept(configured);
        }
        catch (ConfigurationException e)
        {
            throw new ConfigurationException(String.format("Error parsing property -D%s%s: %s", Interceptors.PROPERTY_PREFIX, propertyName, e.getMessage()),
                                             e.getCause());
        }

    }

    private boolean disabledOnStartup()
    {
        return getProperty(DISABLE_ON_STARTUP_PROPERTY, "false").trim().toLowerCase().equals("true");
    }

    private float randomInterceptionRatio()
    {
        String property = getProperty(RANDOM_INTERCEPTION);
        if (property == null)
            return 1f;

        return Float.valueOf(property);
    }

    private void registerJMX(String name)
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            ObjectName jmxName = new ObjectName(String.format("%s:type=%s,name=%s", "com.datastax.net", "Interceptors", name));
            mbs.registerMBean(this, jmxName);
        }
        catch (InstanceAlreadyExistsException e)
        {
            throw new ConfigurationException(String.format("Multiple instances created with the same name '%s'. "
                                                           + "Use '%s=<someName>' when declaring interceptors to disambiguate",
                                                           name, getClass().getSimpleName()));
        }
        catch (Exception e)
        {
            // Interceptor are for testing, if we screwed up something we might not get what we expect and there is no
            // real point in doing heroic efforts to recover.
            throw new RuntimeException("Unexpected error while setting up JMX for " + getClass(), e);
        }
    }

    protected static String getProperty(String name)
    {
        return System.getProperty(Interceptors.PROPERTY_PREFIX + name);
    }

    protected static String getProperty(String name, String defaultValue)
    {
        return System.getProperty(Interceptors.PROPERTY_PREFIX + name, defaultValue);
    }

    private boolean shouldIntercept(Message<?> msg, MessageDirection direction)
    {
        // We only count "seen" message if the interceptor is enabled and those messages that are matching out what it is
        // set to intercept.
        if (!enabled
            || !interceptedVerbs.contains(msg.verb())
            || !interceptedDirections.contains(direction)
            || !interceptedLocalities.contains(msg.locality())
            || !interceptedTypes.contains(msg.type()))
            return false;

        seen.incrementAndGet();

        // Note: checking == 1f is just to avoid the call to random if it's not needed.
        return interceptionChance == 1f || ThreadLocalRandom.current().nextFloat() < interceptionChance;
    }

    public <M extends Message<?>> void intercept(M message, InterceptionContext<M> context)
    {
        if (shouldIntercept(message, context.direction()))
        {
            logger.debug("{} intercepted {}", name, message);
            intercepted.incrementAndGet();
            handleIntercepted(message, context);
        }
        else
        {
            context.passDown(message);
        }

    }

    protected abstract <M extends Message<?>> void handleIntercepted(M message, InterceptionContext<M> context);

    public void enable()
    {
        if (!enabled)
            logger.info("Enabling interceptor {}", name);

        enabled = true;
    }

    public void disable()
    {
        if (enabled)
            logger.info("Disabling interceptor {}", name);

        enabled = false;
    }

    public boolean getEnabled()
    {
        return enabled;
    }

    public long getSeenCount()
    {
        return seen.get();
    }

    public long getInterceptedCount()
    {
        return intercepted.get();
    }

    public String getIntercepted()
    {
        // Tries to rebuild a string of what's intercepted. For this to be readable, we extract the full verb groups
        // (we brute-force this a bit but performance doesn't matter, this is for JMX).

        // Gather all group we fully intercept
        Set<VerbGroup<?>> groups = Sets.newHashSet(Iterables.filter(Verbs.allGroups(),
                                                                    g -> Iterables.all(g, interceptedVerbs::contains)));

        // Then any verb that is in none of our group is standalone
        Iterable<Verb<?, ?>> verbs = Iterables.filter(interceptedVerbs,
                                                      v -> Iterables.all(groups, g -> !Iterables.contains(g, v)));

        return Joiner.on(",").join(Iterables.concat(Iterables.transform(groups, VerbGroup::toString),
                                                    Iterables.transform(verbs, v -> String.format("%s.%s", v.group(), v))));
    }

    public void setIntercepted(String interceptedString)
    {
        if (!allowModifyingIntercepted())
            throw new ConfigurationException("Cannot update/configure what this interceptor intercepts");

        ImmutableSet.Builder<Verb<?, ?>> builder = ImmutableSet.builder();
        List<String> names = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(interceptedString);
        for (String name : names)
        {
            List<String> vals = Splitter.on('.').splitToList(name.toUpperCase());
            if (vals.isEmpty() || vals.size() > 2)
                throw new ConfigurationException(String.format("Invalid value '%s' for intercepted", name));

            try
            {
                Verbs.Group groupName = Verbs.Group.valueOf(vals.get(0));
                VerbGroup<?> group = Iterables.find(Verbs.allGroups(), g -> g.id() == groupName);
                if (vals.size() == 1)
                    builder.addAll(group);
                else
                    builder.add(Iterables.find(group, v -> v.name().equalsIgnoreCase(vals.get(1))));
            }
            catch (Exception e)
            {
                throw new ConfigurationException(String.format("Invalid value '%s' for intercepted", name));
            }
        }
        interceptedVerbs = builder.build();
    }

    private <T extends Enum<?>> String getInterceptedEnum(Set<T> values)
    {
        return Joiner.on(",").join(values);
    }

    private <T extends Enum<T>> ImmutableSet<T> setInterceptedEnum(String interceptedString, Class<T> klass)
    {
        if (!allowModifyingIntercepted())
            throw new ConfigurationException("Cannot update/configure what this interceptor intercepts");

        try
        {
            Iterable<String> splits = Splitter.on(',').trimResults().omitEmptyStrings().split(interceptedString);
            return Sets.<T>immutableEnumSet(Iterables.transform(splits, str -> Enum.valueOf(klass, str.toUpperCase())));
        }
        catch (Exception e)
        {
            throw new ConfigurationException(String.format("Invalid value '%s' for intercepted directions", interceptedString));
        }
    }

    public String getInterceptedDirections()
    {
        return getInterceptedEnum(interceptedDirections);
    }

    public void setInterceptedDirections(String interceptedString)
    {
        interceptedDirections = setInterceptedEnum(interceptedString, MessageDirection.class);
    }

    public String getInterceptedTypes()
    {
        return getInterceptedEnum(interceptedTypes);
    }

    public void setInterceptedTypes(String interceptedString)
    {
        interceptedTypes = setInterceptedEnum(interceptedString, Message.Type.class);
    }

    public String getInterceptedLocalities()
    {
        return getInterceptedEnum(interceptedLocalities);
    }

    public void setInterceptedLocalities(String interceptedString)
    {
        interceptedLocalities = setInterceptedEnum(interceptedString, Message.Locality.class);
    }

    public float getInterceptionChance()
    {
        return interceptionChance;
    }

    public void setInterceptionChance(float ratio)
    {
        if (ratio < 0f || ratio > 1f)
            throw new ConfigurationException(String.format("Invalid value for %s: must be in [0, 1], got %f",
                                                           RANDOM_INTERCEPTION, ratio));
        interceptionChance = ratio;
    }
}
