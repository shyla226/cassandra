/**
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.db.audit;

import java.util.LinkedList;
import java.util.List;

import org.apache.cassandra.utils.Pair;

public class BindVariablesFormatter
{
    // collect bound variable values so that we can format a string with them 
    // when we're ready. In the calling code we extract binding for keys before
    // other variables, so we expect those to appear first in the formatted
    // string (not perfect, but wygd?)
    private List<Pair<String, String>> boundVariables = new LinkedList<>();

    public void collect(String name, String value)
    {
        boundVariables.add(Pair.create(name, value));
    }

    public String format()
    {
        if (boundVariables.isEmpty())
            return "";

        boolean first = true;
        StringBuilder builder = new StringBuilder("[");
        for (Pair<String, String> var : boundVariables)
        {
            if (!first)
                builder.append(",");
            else
                first = false;

            builder.append(var.left);
            builder.append("=");
            builder.append(var.right);
        }
        builder.append("]");
        return builder.toString();
    }
}
