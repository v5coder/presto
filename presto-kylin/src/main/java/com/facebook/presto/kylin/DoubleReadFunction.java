package com.facebook.presto.kylin;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface DoubleReadFunction
        extends ReadFunction
{
    @Override
    default Class<?> getJavaType()
    {
        return double.class;
    }

    double readDouble(ResultSet resultSet, int columnIndex)
            throws SQLException;
}