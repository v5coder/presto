package com.facebook.presto.kylin;

import com.facebook.presto.spi.connector.*;
import com.facebook.presto.spi.transaction.IsolationLevel;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.spi.transaction.IsolationLevel.READ_COMMITTED;
import static com.facebook.presto.spi.transaction.IsolationLevel.checkConnectorSupports;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class KylinConnector implements Connector {

    private static final Logger log = Logger.get(KylinConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final KylinMetadataFactory kylinMetadataFactory;
    private final KylinSplitManager kylinSplitManager;
    private final KylinRecordSetProvider kylinRecordSetProvider;
    private final KylinPageSinkProvider kylinPageSinkProvider;

    private final ConcurrentMap<ConnectorTransactionHandle, KylinMetadata> transactions = new ConcurrentHashMap<>();

    @Inject
    public KylinConnector(
            LifeCycleManager lifeCycleManager,
            KylinMetadataFactory kylinMetadataFactory,
            KylinSplitManager kylinSplitManager,
            KylinRecordSetProvider kylinRecordSetProvider,
            KylinPageSinkProvider kylinPageSinkProvider
    ) {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.kylinMetadataFactory = requireNonNull(kylinMetadataFactory, "kylinMetadataFactory is null");
        this.kylinSplitManager = requireNonNull(kylinSplitManager, "kylinSplitManager is null");
        this.kylinRecordSetProvider = requireNonNull(kylinRecordSetProvider, "kylinRecordSetProvider is null");
        this.kylinPageSinkProvider = requireNonNull(kylinPageSinkProvider, "kylinPageSinkProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        checkConnectorSupports(READ_COMMITTED, isolationLevel);
        KylinTransactionHandle transaction = new KylinTransactionHandle();
        transactions.put(transaction, kylinMetadataFactory.create());
        return transaction;
    }

    @Override
    public boolean isSingleStatementWritesOnly() {
        return true;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transaction) {
        KylinMetadata metadata = transactions.get(transaction);
        checkArgument(metadata != null, "no such transaction: %s", transaction);
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return kylinSplitManager;
    }

    @Override
    public ConnectorRecordSetProvider getRecordSetProvider() {
        return kylinRecordSetProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider() {
        return kylinPageSinkProvider;
    }

    @Override
    public final void shutdown() {
        try {
            lifeCycleManager.stop();
        } catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
