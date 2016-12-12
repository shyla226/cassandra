package org.apache.cassandra.stress.util;

import com.datastax.driver.core.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

class StatementPrinter
{
    private static final String TRUNCATED_OUTPUT = "...";
    private static final int MAX_PARAMETER_VALUE_LENGTH = 20;

    private final CodecRegistry codecRegistry;
    private final ProtocolVersion protocolVersion;

    private final PrintStream out;

    StatementPrinter(Cluster cluster, PrintStream out)
    {
        codecRegistry = cluster.getConfiguration().getCodecRegistry();
        protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
        this.out = out;
    }

    public void print(long consumerId, Statement statement, ResultSet result)
    {
        out.println(statementAndResultAsString(consumerId, statement, result));
    }

    private String statementAndResultAsString(long consumerId, Statement statement, ResultSet result)
    {
        StringBuilder sb = new StringBuilder();
        String prefix = String.format("%s [%02d]: ", ISODateTimeFormat.dateTime().print(new DateTime()), consumerId);
        sb.append(prefix);
        append(statement, 0, sb);
        append(result, sb);
        return sb.toString().replace("\n", "\n" + prefix);
    }

    private String[] getParameterValuesAsStrings(BoundStatement statement)
    {
        ColumnDefinitions metadata = statement.preparedStatement().getVariables();
        String[] strings = new String[metadata.size()];
        int numberOfParameters = metadata.size();
        if (numberOfParameters > 0)
        {
            List<ColumnDefinitions.Definition> definitions = metadata.asList();
            for (int i = 0; i < numberOfParameters; i++)
            {
                String value = statement.isSet(i)
                    ? parameterValueAsString(definitions.get(i), statement.getBytesUnsafe(i))
                    : "<UNSET>";
                strings[i] = value;
            }
        }
        return strings;
    }

    private String parameterValueAsString(ColumnDefinitions.Definition definition, ByteBuffer raw)
    {
        String valueStr;
        if (raw == null || raw.remaining() == 0)
        {
            valueStr = "NULL";
        }
        else
        {
            DataType type = definition.getType();
            TypeCodec<Object> codec = codecRegistry.codecFor(type);
            if (type.equals(DataType.blob()))
            {
                // prevent large blobs from being converted to strings
                int maxBufferLength = Math.max(2, (MAX_PARAMETER_VALUE_LENGTH - 2) / 2);
                boolean bufferTooLarge = raw.remaining() > maxBufferLength;
                if (bufferTooLarge)
                {
                    raw = (ByteBuffer) raw.duplicate().limit(maxBufferLength);
                }
                Object value = codec.deserialize(raw, protocolVersion);
                valueStr = codec.format(value);
                if (bufferTooLarge)
                {
                    valueStr = valueStr + TRUNCATED_OUTPUT;
                }
            }
            else
            {
                Object value = codec.deserialize(raw, protocolVersion);
                valueStr = codec.format(value);
            }
        }
        return valueStr;
    }

    private void appendParameters(SimpleStatement statement, StringBuilder buffer)
    {
        int numberOfParameters = statement.valuesCount();
        if (numberOfParameters > 0)
        {
            buffer.append(" [");
            Iterator<String> valueNames = null;
            if (statement.usesNamedValues())
            {
                valueNames = statement.getValueNames().iterator();
            }
            for (int i = 0; i < numberOfParameters; i++)
            {
                if (i != 0)
                    buffer.append(", ");
                if (valueNames != null && valueNames.hasNext())
                {
                    String valueName = valueNames.next();
                    buffer.append(
                        String.format("%s:%s", valueName, parameterValueAsString(statement.getObject(valueName))));
                }
                else
                {
                    buffer.append(parameterValueAsString(statement.getObject(i)));
                }
            }
            buffer.append("]");
        }
    }

    private String parameterValueAsString(Object value)
    {
        String valueStr;
        if (value == null)
        {
            valueStr = "NULL";
        }
        else
        {
            TypeCodec<Object> codec = codecRegistry.codecFor(value);
            if (codec.getCqlType().equals(DataType.blob()))
            {
                // prevent large blobs from being converted to strings
                ByteBuffer buf = (ByteBuffer) value;
                int maxBufferLength = Math.max(2, (MAX_PARAMETER_VALUE_LENGTH - 2) / 2);
                boolean bufferTooLarge = buf.remaining() > maxBufferLength;
                if (bufferTooLarge)
                {
                    value = buf.duplicate().limit(maxBufferLength);
                }
                valueStr = codec.format(value);
                if (bufferTooLarge)
                {
                    valueStr = valueStr + TRUNCATED_OUTPUT;
                }
            }
            else
            {
                valueStr = codec.format(value);
            }
        }
        return valueStr;
    }

    protected void append(BoundStatement statement, StringBuilder buffer)
    {
        buffer.append(
            String.format(
                statement.preparedStatement().getQueryString().trim().replace("?", "[%s]"),
                (Object[]) getParameterValuesAsStrings(statement)));
        buffer.append(";");
    }

    private void appendIndent(int indent, StringBuilder buffer)
    {
        for (int i = 0; i != indent; ++i)
        {
            buffer.append(' ');
        }
    }

    private void append(Statement statement, int indent, StringBuilder buffer)
    {
        appendIndent(indent, buffer);

        if (statement instanceof RegularStatement)
        {
            RegularStatement rs = (RegularStatement) statement;
            String query = rs.getQueryString();
            buffer.append(query.trim());
            buffer.append(";");
            if (statement instanceof SimpleStatement)
            {
                appendParameters((SimpleStatement) statement, buffer);
            }
        }
        else if (statement instanceof BoundStatement)
        {
            append((BoundStatement) statement, buffer);
        }
        else if (statement instanceof BatchStatement)
        {
            BatchStatement batchStatement = (BatchStatement) statement;
            buffer.append("BEGIN BATCH;");
            buffer.append(" [statements=");
            buffer.append(batchStatement.size());
            buffer.append("]");
            for (Statement stmt : batchStatement.getStatements())
            {
                buffer.append("\n");
                append(stmt, indent + 2, buffer);
            }
            buffer.append("\n");
            appendIndent(indent, buffer);
            buffer.append("APPLY BATCH;");
        }
        else
        {
            // Unknown types of statement
            // Call toString() as a last resort
            buffer.append(statement.toString());
            buffer.append(";");
        }
    }

    private void append(ResultSet result, StringBuilder buffer)
    {
        if (result == null)
        {
            return;
        }

        buffer.append(" -> [rows");
        if (result.isFullyFetched())
        {
            buffer.append("=");
            buffer.append(result.getAvailableWithoutFetching());
        }
        else
        {
            buffer.append(">");
            buffer.append(result.getAvailableWithoutFetching());
        }
        buffer.append("]");
    }

}
