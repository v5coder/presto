package com.facebook.presto.kylin;

import com.alibaba.fastjson.JSON;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.testing.TestingConnectorContext;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TestKylinPlugin {

    @Test
    public void testCreateConnector() {
        Plugin plugin = new KylinPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:kylin://101.37.223.69:7070/infrasystem"), new TestingConnectorContext());
    }

    @Test
    public void testListSchema() {
        KylinClient client = new KylinClient(new KylinConnectorId("kylin"), new KylinConfig());
        Set<String> schemas = client.getSchemaNames();
        System.out.println(JSON.toJSONString(schemas));
        Iterator<String> it = schemas.iterator();
        while (it.hasNext()) {
            String schema = it.next();
            List<SchemaTableName> list = client.getTableNames(schema);
            System.out.println(JSON.toJSONString(list));
            for (int i = 0; i < list.size(); i++) {
                KylinTableHandle tableHandle = client.getTableHandle(list.get(i));

            }
        }
    }

    @Test
    public void testListTable() {
        KylinClient client = new KylinClient(new KylinConnectorId("kylin"), new KylinConfig());
        List<SchemaTableName> list = client.getTableNames("infrasystem");
        System.out.println(JSON.toJSONString(list));
    }

    @Test
    public void testGetTableHandle() {
        KylinClient client = new KylinClient(new KylinConnectorId("kylin"), new KylinConfig());
        KylinTableHandle tableHandle = client.getTableHandle(new SchemaTableName("infrasystem", "v_billy_order_model_cube"));
        System.out.println(JSON.toJSONString(tableHandle));
    }

    @Test
    public void testGetColumns() {
        KylinClient client = new KylinClient(new KylinConnectorId("kylin"), new KylinConfig());
        List<KylinColumnHandle> columnHandles = client.getColumns(null, new KylinTableHandle(
                "kylin",
                new SchemaTableName("infrasystem", "v_billy_order_model_cube"),
                "v_billy_order_model_cube",
                "infrasystem",
                "v_billy_order_model_cube"));
        System.out.println(columnHandles.size());
    }
}