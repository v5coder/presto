package com.facebook.presto.kylin;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.MoreObjects.firstNonNull;

public class KylinPlugin
        implements Plugin {

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return ImmutableList.of(new KylinConnectorFactory("kylin", new KylinClientModule(), getClassLoader()));
    }

    private static ClassLoader getClassLoader() {
        return firstNonNull(Thread.currentThread().getContextClassLoader(), KylinPlugin.class.getClassLoader());
    }
}
