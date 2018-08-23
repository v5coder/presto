package com.facebook.presto.server;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.CatalogManagerConfig;
import com.facebook.presto.metadata.PropertiesUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.*;

import static com.facebook.presto.server.PrestoServer.updateDatasourcesAnnouncement;
import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;

@Path("/v1/catalog")
public class CatalogResource {

    private static final Logger log = Logger.get(CatalogResource.class);

    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private static Announcer announcer;
    private final File catalogConfigurationDir;

    @Inject
    public CatalogResource(
            ConnectorManager connectorManager,
            CatalogManager catalogManager,
            Announcer announcer,
            CatalogManagerConfig config) {
        this(connectorManager, catalogManager, announcer,
                config.getCatalogConfigurationDir());
    }

    public CatalogResource(ConnectorManager connectorManager, CatalogManager catalogManager, Announcer announcer, File catalogConfigurationDir) {
        this.connectorManager = connectorManager;
        this.catalogManager = catalogManager;
        this.announcer = announcer;
        this.catalogConfigurationDir = catalogConfigurationDir;
    }

    @GET
    @Path("add/{newTopicName}")
    public Response updateKafkaTableNames(@PathParam("newTopicName") String newTopicName) throws Exception {

        // 先移除kafka connector
        connectorManager.removeConnector("kafka");

        File file = new File(catalogConfigurationDir, "kafka.properties");
        PropertiesUtil propertiesUtil = PropertiesUtil.getInstance(file);
        String kafkaTableNameStr = (String) propertiesUtil.getValue("kafka.table-names");
        Set<String> kafkaTableNames = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(kafkaTableNameStr));
        if (kafkaTableNames.contains(newTopicName)) {
            return Response.ok("[" + newTopicName + "]存在重复数据").build();
        }
        kafkaTableNames.add(newTopicName);
        propertiesUtil.setProperty("kafka.table-names", Joiner.on(',').join(kafkaTableNames));
        catalogManager.loadCatalog(file);
        updateDatasourcesAnnouncement("kafka", PrestoServer.DatasourceAction.ADD);
        return Response.ok("[" + newTopicName + "]添加成功").build();
    }

    @GET
    @Path("del/{delTopicName}")
    public Response delKafkaTableNames(@PathParam("delTopicName") String delTopicName) throws Exception {

        // 先移除kafka connector
        connectorManager.removeConnector("kafka");

        File file = new File(catalogConfigurationDir, "kafka.properties");
        PropertiesUtil propertiesUtil = PropertiesUtil.getInstance(file);
        String kafkaTableNameStr = propertiesUtil.getValue("kafka.table-names");
        Set<String> kafkaTableNames = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(kafkaTableNameStr));
        if (!kafkaTableNames.contains(delTopicName)) {
            return Response.ok("[" + delTopicName + "]不存在数据").build();
        }
        kafkaTableNames.remove(delTopicName);
        propertiesUtil.setProperty("kafka.table-names", Joiner.on(',').join(kafkaTableNames));
        catalogManager.loadCatalog(file);
        updateDatasourcesAnnouncement("kafka", PrestoServer.DatasourceAction.ADD);
        return Response.ok("[" + delTopicName + "]删除成功").build();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements) {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new IllegalArgumentException("Presto announcement not found: " + announcements);
    }

    // 根据action更新数据源
    public static void updateDatasourcesAnnouncement(String connectorId, PrestoServer.DatasourceAction action) {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        // update datasources property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("datasources"));
        log.info("update datasources announcement : {" + action + "}");
        Set<String> datasources = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        log.info("connector id : {" + connectorId + "}");
        datasources.add(connectorId);
        log.info("datasources : {" + Joiner.on(',').join(datasources) + "}");
        properties.put("datasources", Joiner.on(',').join(datasources));
        // update announcement
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
    }
}