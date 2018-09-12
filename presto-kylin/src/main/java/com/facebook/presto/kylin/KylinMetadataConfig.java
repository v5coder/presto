package com.facebook.presto.kylin;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class KylinMetadataConfig {

    private boolean allowDropTable;

    public boolean isAllowDropTable() {
        return allowDropTable;
    }

    @Config("allow-drop-table")
    @ConfigDescription("Allow connector to drop tables")
    public KylinMetadataConfig setAllowDropTable(boolean allowDropTable) {
        this.allowDropTable = allowDropTable;
        return this;
    }
}
