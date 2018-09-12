package com.facebook.presto.kylin;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class KylinSplitManager implements ConnectorSplitManager {

    private final KylinClient kylinClient;

    @Inject
    public KylinSplitManager(KylinClient kylinClient)
    {
        this.kylinClient = requireNonNull(kylinClient, "kylinClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layout, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KylinTableLayoutHandle layoutHandle = (KylinTableLayoutHandle) layout;
        return kylinClient.getSplits(layoutHandle);
    }
}
