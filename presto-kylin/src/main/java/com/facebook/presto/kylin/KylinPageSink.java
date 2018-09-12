package com.facebook.presto.kylin;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import org.joda.time.DateTimeZone;

import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.readBigDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

public class KylinPageSink
        implements ConnectorPageSink {
    private final KylinClientHelper connection;
    private final PreparedStatement statement;

    private final List<Type> columnTypes;
    private int batchSize;

    public KylinPageSink(KylinOutputTableHandle handle, KylinClient kylinClient) {
        connection = kylinClient.getConnection(handle);
        statement = null;//connection.prepareStatement(kylinClient.buildInsertSql(handle));
        columnTypes = handle.getColumnTypes();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page) {
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                    appendColumn(page, position, channel);
                }

                statement.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    statement.executeBatch();
                    batchSize = 0;
                }
            }
        } catch (SQLException e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(Page page, int position, int channel)
            throws SQLException {
        Block block = page.getBlock(channel);
        int parameter = channel + 1;

        if (block.isNull(position)) {
            statement.setObject(parameter, null);
            return;
        }

        Type type = columnTypes.get(channel);
        if (BOOLEAN.equals(type)) {
            statement.setBoolean(parameter, type.getBoolean(block, position));
        } else if (BIGINT.equals(type)) {
            statement.setLong(parameter, type.getLong(block, position));
        } else if (INTEGER.equals(type)) {
            statement.setInt(parameter, toIntExact(type.getLong(block, position)));
        } else if (SMALLINT.equals(type)) {
            statement.setShort(parameter, Shorts.checkedCast(type.getLong(block, position)));
        } else if (TINYINT.equals(type)) {
            statement.setByte(parameter, SignedBytes.checkedCast(type.getLong(block, position)));
        } else if (DOUBLE.equals(type)) {
            statement.setDouble(parameter, type.getDouble(block, position));
        } else if (REAL.equals(type)) {
            statement.setFloat(parameter, intBitsToFloat(toIntExact(type.getLong(block, position))));
        } else if (type instanceof DecimalType) {
            statement.setBigDecimal(parameter, readBigDecimal((DecimalType) type, block, position));
        } else if (isVarcharType(type) || isCharType(type)) {
            statement.setString(parameter, type.getSlice(block, position).toStringUtf8());
        } else if (VARBINARY.equals(type)) {
            statement.setBytes(parameter, type.getSlice(block, position).getBytes());
        } else if (DATE.equals(type)) {
            // convert to midnight in default time zone
            long utcMillis = DAYS.toMillis(type.getLong(block, position));
            long localMillis = getInstanceUTC().getZone().getMillisKeepLocal(DateTimeZone.getDefault(), utcMillis);
            statement.setDate(parameter, new Date(localMillis));
        } else {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish() {
        // commit and close
        /*try (Connection connection = this.connection;
             PreparedStatement statement = this.statement) {
            if (batchSize > 0) {
                statement.executeBatch();
                connection.commit();
            }
        } catch (SQLNonTransientException e) {
            throw new PrestoException(KylinErrorCode.KYLIN_NON_TRANSIENT_ERROR, e);
        } catch (SQLException e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }*/
        // the committer does not need any additional info
        return completedFuture(ImmutableList.of());
    }

    @SuppressWarnings("unused")
    @Override
    public void abort() {
        // rollback and close
        /*try (Connection connection = this.connection;
             PreparedStatement statement = this.statement) {
            connection.rollback();
        } catch (SQLException e) {
            throw new PrestoException(KylinErrorCode.KYLIN_ERROR, e);
        }*/
    }

    @SuppressWarnings("ObjectEquality")
    private static void closeWithSuppression(Connection connection, Throwable throwable) {
        try {
            connection.close();
        } catch (Throwable t) {
            // Self-suppression not permitted
            if (throwable != t) {
                throwable.addSuppressed(t);
            }
        }
    }
}