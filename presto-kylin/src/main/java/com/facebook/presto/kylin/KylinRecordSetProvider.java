package com.facebook.presto.kylin;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class KylinRecordSetProvider implements ConnectorRecordSetProvider {
    private final KylinClient kylinClient;

    @Inject
    public KylinRecordSetProvider(KylinClient kylinClient) {
        this.kylinClient = requireNonNull(kylinClient, "kylinClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns) {
        KylinSplit kylinSplit = (KylinSplit) split;

        ImmutableList.Builder<KylinColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((KylinColumnHandle) handle);
        }

        return new KylinRecordSet(kylinClient, session, kylinSplit, handles.build());
    }
}
