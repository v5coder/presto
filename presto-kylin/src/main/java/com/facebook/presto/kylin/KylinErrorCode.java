package com.facebook.presto.kylin;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;

public enum KylinErrorCode
        implements ErrorCodeSupplier
{
    KYLIN_ERROR(0, EXTERNAL),
    KYLIN_NON_TRANSIENT_ERROR(1, EXTERNAL);

    private final ErrorCode errorCode;

    KylinErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0400_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}