package com.facebook.presto.kylin;

import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionFactory
        extends AutoCloseable {
    KylinClientHelper openConnection();

    @Override
    default void close()
            throws SQLException {
    }
}