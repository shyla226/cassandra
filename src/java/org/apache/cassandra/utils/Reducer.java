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

    Throwable errors = null;

    public void error(Throwable error)
    {
        errors = Throwables.merge(errors, error);
    }

    public Throwable getErrors()
    {
        return errors;
    }

    /** @return The last object computed by reduce */
    public abstract Out getReduced();

    /**
     * Called at the beginning of each new key, before any reduce is called.
     * To be overridden by implementing classes.
     *
     * Note: There's no need to clear error; merging completes once one is found.
     */
    public void onKeyChange() {}

    /**
     * May be overridden by implementations that require cleaning up after use
     */
    public void close() {}
}