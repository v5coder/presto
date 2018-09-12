package com.facebook.presto.kylin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class KylinTypeHandle {

    private final int kylinType;
    private final int columnSize;
    private final int decimalDigits;

    @JsonCreator
    public KylinTypeHandle(
            @JsonProperty("kylinType") int kylinType,
            @JsonProperty("columnSize") int columnSize,
            @JsonProperty("decimalDigits") int decimalDigits) {
        this.kylinType = kylinType;
        this.columnSize = columnSize;
        this.decimalDigits = decimalDigits;
    }

    @JsonProperty
    public int getKylinType() {
        return kylinType;
    }

    @JsonProperty
    public int getColumnSize() {
        return columnSize;
    }

    @JsonProperty
    public int getDecimalDigits() {
        return decimalDigits;
    }

    @Override
    public int hashCode() {
        return Objects.hash(kylinType, columnSize, decimalDigits);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KylinTypeHandle that = (KylinTypeHandle) o;
        return kylinType == that.kylinType &&
                columnSize == that.columnSize &&
                decimalDigits == that.decimalDigits;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("kylinType", kylinType)
                .add("columnSize", columnSize)
                .add("decimalDigits", decimalDigits)
                .toString();
    }
}
