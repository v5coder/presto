package com.facebook.presto.kylin;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class KylinClientModule
        extends AbstractConfigurationAwareModule {

    @Override
    protected void setup(Binder binder) {
        binder.bind(KylinClient.class).in(Scopes.SINGLETON);
        buildConfigObject(KylinConfig.class).getConnectionUrl();
        configBinder(binder).bindConfig(KylinConfig.class);
    }

}
