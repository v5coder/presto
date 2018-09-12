package com.facebook.presto.kylin;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class KylinColumnHandle implements ColumnHandle {

    private final String connectorId;
    private final String columnName;
    private final KylinTypeHandle kylinTypeHandle;
    private final Type columnType;

    @JsonCreator
    public KylinColumnHandle(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("columnName") String columnName,
            @JsonProperty("jdbcTypeHandle") KylinTypeHandle kylinTypeHandle,
            @JsonProperty("columnType") Type columnType) {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.kylinTypeHandle = requireNonNull(kylinTypeHandle, "kylinTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
    }

    @JsonProperty
    public String getConnectorId() {
        return connectorId;
    }

    @JsonProperty
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty
    public KylinTypeHandle getKylinTypeHandle() {
        return kylinTypeHandle;
    }

    @JsonProperty
    public Type getColumnType() {
        return columnType;
    }

    public ColumnMetadata getColumnMetadata() {
        return new ColumnMetadata(columnName, columnType);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        KylinColumnHandle o = (KylinColumnHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("kylinTypeHandle", kylinTypeHandle)
                .add("columnType", columnType)
                .toString();
    }
}
