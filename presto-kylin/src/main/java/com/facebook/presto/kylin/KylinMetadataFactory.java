package com.facebook.presto.kylin;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class KylinMetadataFactory {

    private final KylinClient kylinClient;
    private final boolean allowDropTable;

    @Inject
    public KylinMetadataFactory(KylinClient kylinClient, KylinMetadataConfig config)
    {
        this.kylinClient = requireNonNull(kylinClient, "kylinClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
    }

    public KylinMetadata create()
    {
        return new KylinMetadata(kylinClient, allowDropTable);
    }
}
