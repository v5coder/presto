package com.facebook.presto.kylin;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface LongReadFunction
        extends ReadFunction
{
    @Override
    default Class<?> getJavaType()
    {
        return long.class;
    }

    long readLong(ResultSet resultSet, int columnIndex)
            throws SQLException;
}