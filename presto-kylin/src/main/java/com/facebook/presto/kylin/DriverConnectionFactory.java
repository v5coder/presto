package com.facebook.presto.kylin;

public class DriverConnectionFactory
        implements ConnectionFactory {

    private final KylinConfig kylinConfig;

    public DriverConnectionFactory(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    @Override
    public KylinClientHelper openConnection() {
        KylinClientHelper helper = new KylinClientHelper(kylinConfig);
        return helper;
    }
}