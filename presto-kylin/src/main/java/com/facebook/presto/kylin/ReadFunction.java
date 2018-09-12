package com.facebook.presto.kylin;

public interface ReadFunction
{
    Class<?> getJavaType();

    // This should be considered to have a method as below (it doesn't to avoid autoboxing)
    //    T read(ResultSet resultSet, int columnIndex)
    //            throws SQLException;
}