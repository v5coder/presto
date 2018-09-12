package com.facebook.presto.kylin;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.VerifyException;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KylinRecordCursor implements RecordCursor {

    private static final Logger log = Logger.get(KylinRecordCursor.class);

    private final KylinColumnHandle[] columnHandles;
    private final BooleanReadFunction[] booleanReadFunctions;
    private final DoubleReadFunction[] doubleReadFunctions;
    private final LongReadFunction[] longReadFunctions;
    private final SliceReadFunction[] sliceReadFunctions;

    private final KylinClient kylinClient;
    private final KylinClientHelper connection;
    private final PreparedStatement statement;
    private final ResultSet resultSet;
    private boolean closed;

    public KylinRecordCursor(KylinClient kylinClient,
                             ConnectorSession session,
                             KylinSplit split,
                             List<KylinColumnHandle> columnHandles) {
        this.kylinClient = requireNonNull(kylinClient, "kylinClient is null");

        this.columnHandles = columnHandles.toArray(new KylinColumnHandle[0]);

        booleanReadFunctions = new BooleanReadFunction[columnHandles.size()];
        doubleReadFunctions = new DoubleReadFunction[columnHandles.size()];
        longReadFunctions = new LongReadFunction[columnHandles.size()];
        sliceReadFunctions = new SliceReadFunction[columnHandles.size()];

        for (int i = 0; i < this.columnHandles.length; i++) {
            ReadMapping readMapping = kylinClient.toPrestoType(session, new KylinTypeHandle(0, 0, 0))
                    .orElseThrow(() -> new VerifyException("Unsupported column type"));
            Class<?> javaType = readMapping.getType().getJavaType();
            ReadFunction readFunction = readMapping.getReadFunction();

            if (javaType == boolean.class) {
                booleanReadFunctions[i] = (BooleanReadFunction) readFunction;
            } else if (javaType == double.class) {
                doubleReadFunctions[i] = (DoubleReadFunction) readFunction;
            } else if (javaType == long.class) {
                longReadFunctions[i] = (LongReadFunction) readFunction;
            } else if (javaType == Slice.class) {
                sliceReadFunctions[i] = (SliceReadFunction) readFunction;
            } else {
                throw new IllegalStateException(format("Unsupported java type %s", javaType));
            }
        }

        try {
            connection = kylinClient.getConnection(split);
            statement = kylinClient.buildSql(connection, split, columnHandles);
            //log.debug("Executing: %s", statement.toString());
            resultSet = statement.executeQuery();
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        return null;
    }

    @Override
    public boolean advanceNextPosition() {
        if (closed) {
            return false;
        }

        try {
            return resultSet.next();
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public boolean getBoolean(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return booleanReadFunctions[field].readBoolean(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return longReadFunctions[field].readLong(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public double getDouble(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return doubleReadFunctions[field].readDouble(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field) {
        checkState(!closed, "cursor is closed");
        try {
            return sliceReadFunctions[field].readSlice(resultSet, field + 1);
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field) {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.length, "Invalid field index");

        try {
            // JDBC is kind of dumb: we need to read the field and then ask
            // if it was null, which means we are wasting effort here.
            // We could save the result of the field access if it matters.
            resultSet.getObject(field + 1);
            return resultSet.wasNull();
        } catch (SQLException | RuntimeException e) {
            throw handleSqlException(e);
        }
    }

    @SuppressWarnings("UnusedDeclaration")
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;

        // use try with resources to close everything properly
        /*try (Connection connection = this.connection;
             Statement statement = this.statement;
             ResultSet resultSet = this.resultSet) {
            kylinClient.abortReadConnection(connection);
        } catch (SQLException e) {
            // ignore exception from close
        }*/
    }

    private RuntimeException handleSqlException(Exception e) {
        try {
            close();
        } catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
    }
}
