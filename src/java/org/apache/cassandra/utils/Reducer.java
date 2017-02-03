package org.apache.cassandra.utils;

/** Accumulator that collects values of type A, and outputs a value of type B. */
public abstract class Reducer<In,Out>
{
    /**
     * @return true if Out is the same as In for the case of a single source iterator
     */
    public boolean trivialReduceIsTrivial()
    {
        return false;
    }

    /**
     * combine this object with the previous ones.
     * intermediate state is up to your implementation.
     */
    public abstract void reduce(int idx, In current);

    /** @return The last object computed by reduce */
    protected abstract Out getReduced();

    /**
     * Called at the beginning of each new key, before any reduce is called.
     * To be overridden by implementing classes.
     */
    protected void onKeyChange() {}

    /**
     * May be overridden by implementations that require cleaning up after use
     */
    public void close() {}
}