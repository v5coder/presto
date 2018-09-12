package com.facebook.presto.kylin;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KylinRecordSet implements RecordSet {

    private final KylinClient kylinClient;
    private final List<KylinColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final KylinSplit split;
    private final ConnectorSession session;

    public KylinRecordSet(KylinClient kylinClient,
                          ConnectorSession session,
                          KylinSplit split,
                          List<KylinColumnHandle> columnHandles) {
        this.kylinClient = requireNonNull(kylinClient, "kylinClient is null");
        this.split = requireNonNull(split, "split is null");

        requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KylinColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new KylinRecordCursor(kylinClient, session, split, columnHandles);
    }
}
