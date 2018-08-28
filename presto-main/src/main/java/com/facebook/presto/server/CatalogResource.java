package com.facebook.presto.server;

import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticCatalogStoreConfig;
import com.facebook.presto.util.PropertiesUtil;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Strings.nullToEmpty;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;

@Path("/v1/catalog")
public class CatalogResource {

    private static final Logger log = Logger.get(CatalogResource.class);

    private final ConnectorManager connectorManager;
    private final CatalogManager catalogManager;
    private static Announcer announcer;
    private final File catalogConfigurationDir;
    private final StaticCatalogStore staticCatalogStore;

    @Inject
    public CatalogResource(
            ConnectorManager connectorManager,
            CatalogManager catalogManager,
            Announcer announcer,
            StaticCatalogStoreConfig config,
            StaticCatalogStore staticCatalogStore) {
        this(connectorManager, catalogManager, announcer,
                config.getCatalogConfigurationDir(), staticCatalogStore);
    }

    public CatalogResource(ConnectorManager connectorManager,
                           CatalogManager catalogManager,
                           Announcer announcer,
                           File catalogConfigurationDir,
                           StaticCatalogStore staticCatalogStore) {
        this.connectorManager = connectorManager;
        this.catalogManager = catalogManager;
        this.announcer = announcer;
        this.catalogConfigurationDir = catalogConfigurationDir;
        this.staticCatalogStore = staticCatalogStore;
    }

    @GET
    @Path("add/{newTopicName}")
    public Response updateKafkaTableNames(@PathParam("newTopicName") String newTopicName) throws Exception {


        File file = new File(catalogConfigurationDir, "kafka.properties");
        PropertiesUtil propertiesUtil = PropertiesUtil.getInstance(file);
        Map<String, String> propertiesMap = propertiesUtil.loadProperties(file);
        String kafkaTableNameStr = propertiesMap.get("kafka.table-names");
        Set<String> kafkaTableNames = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(kafkaTableNameStr));
        if (kafkaTableNames.contains(newTopicName)) {
            return Response.ok("[" + newTopicName + "] is exists").build();
        }
        // 如果新增操作继续，则移除kafka connector
        connectorManager.dropConnection("kafka");
        kafkaTableNames.add(newTopicName);
        propertiesUtil.setProperty("kafka.table-names", Joiner.on(',').join(kafkaTableNames));
        //catalogManager.loadCatalog(file);
        staticCatalogStore.loadCatalog(file);
        updateDatasourcesAnnouncement("kafka", "ADD");
        return Response.ok("[" + newTopicName + "] add success").build();
    }

    @GET
    @Path("del/{delTopicName}")
    public Response delKafkaTableNames(@PathParam("delTopicName") String delTopicName) throws Exception {

        File file = new File(catalogConfigurationDir, "kafka.properties");
        PropertiesUtil propertiesUtil = PropertiesUtil.getInstance(file);
        Map<String, String> propertiesMap = propertiesUtil.loadProperties(file);
        String kafkaTableNameStr = propertiesMap.get("kafka.table-names");
        Set<String> kafkaTableNames = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(kafkaTableNameStr));
        if (!kafkaTableNames.contains(delTopicName)) {
            return Response.ok("[" + delTopicName + "] not exists").build();
        }
        // 如果删除操作继续，则移除kafka connector
        connectorManager.dropConnection("kafka");
        kafkaTableNames.remove(delTopicName);
        propertiesUtil.setProperty("kafka.table-names", Joiner.on(',').join(kafkaTableNames));
        staticCatalogStore.loadCatalog(file);
        updateDatasourcesAnnouncement("kafka", "DELETE");
        return Response.ok("[" + delTopicName + "] delete success").build();
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
    public static void updateDatasourcesAnnouncement(String connectorId, String type) {
        // get existing announcement
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        // update datasources property
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("datasources"));
        log.info("update datasources announcement : {" + type + "}");
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
