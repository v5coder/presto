package com.facebook.presto.kylin;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class KylinModule implements Module {

    private final String connectorId;

    public KylinModule(String connectorId) {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder) {

        binder.bind(KylinConnectorId.class).toInstance(new KylinConnectorId(connectorId));
        binder.bind(KylinMetadataFactory.class).in(Scopes.SINGLETON);
        binder.bind(KylinSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(KylinRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(KylinPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(KylinConnector.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(KylinMetadataConfig.class);
    }
}
