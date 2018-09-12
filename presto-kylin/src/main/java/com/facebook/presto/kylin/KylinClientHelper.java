package com.facebook.presto.kylin;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class KylinClientHelper {

    String auth = null;
    private final KylinConfig kylinConfig;

    public KylinClientHelper(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        login(kylinConfig.getConnectionUser(), kylinConfig.getConnectionPassword());
    }

    /**
     * Authentication
     *
     * @param user
     * @param passwd
     * @return
     */
    public String login(String user, String passwd) {
        String method = "POST";
        String param = "/user/authentication";
        byte[] key = (user + ":" + passwd).getBytes();
        auth = new sun.misc.BASE64Encoder().encode(key);
        return excute(method, param, null);
    }

    /**
     * list projects
     *
     * @return
     */
    public String listProjects() {
        String method = "GET";
        String param = "/projects";
        return excute(method, param, null);
    }

    /**
     * list cubes
     *
     * @param projectName 项目名称
     * @return
     */
    public String listCubes(String projectName) {
        String method = "GET";
        String param = "/cubes?limit=65535&offset=0&projectName=" + projectName;
        return excute(method, param, null);
    }

    /**
     * query
     *
     * @param body <p>
     *             sql - required string The text of sql statement.
     *             <p>
     *             offset - optional int Query offset. If offset is set in sql,
     *             curIndex will be ignored.
     *             <p>
     *             limit - optional int Query limit. If limit is set in sql,
     *             perPage will be ignored.
     *             <p>
     *             acceptPartial - optional bool Whether accept a partial result
     *             or not, default be “false”. Set to “false” for production use.
     *             <p>
     *             project - optional string Project to perform query. Default
     *             value is ‘DEFAULT’.
     * @return
     */
    public String query(String body) {
        String method = "POST";
        String param = "/query";
        return excute(method, param, body);
    }

    /**
     * list queryable tables
     *
     * @param projectName The project to load tables
     * @return
     */
    public String listQueryableTables(String projectName) {
        String method = "GET";
        String param = "/tables_and_columns?project=" + projectName;
        return excute(method, param, null);
    }

    // cube

    /**
     * list cubes
     *
     * @param offset      offset used by pagination
     * @param limit       cubes per page
     * @param cubeName    keyword for cube name, to find cubes whose name contains this
     *                    keyword.
     * @param projectName project name
     * @return
     */
    public String listCubes(int offset, int limit, String cubeName, String projectName) {
        String method = "GET";
        String param = "/cubes?offset=" + offset + "&limit=" + limit + "&cubeName=" + cubeName + "&projectName="
                + projectName;
        return excute(method, param, null);
    }

    /**
     * Get cube
     *
     * @param cubeName Cube name to find.
     * @return
     */
    public String getCube(String cubeName) {
        String method = "GET";
        String param = "/cube_desc/" + cubeName;
        return excute(method, param, null);
    }

    /**
     * Get cube descriptor
     *
     * @param cubeName Cube name.
     * @return
     */
    public String getCubeDes(String cubeName) {
        String method = "GET";
        String param = "/cube_desc/" + cubeName;
        return excute(method, param, null);
    }

    /**
     * Get data model
     *
     * @param modelName Data model name, by default it should be the same with cube
     *                  name.
     * @return
     */
    public String getDataModel(String modelName) {
        String method = "GET";
        String param = "/model/" + modelName;
        return excute(method, param, null);
    }

    /**
     * Build cube. Note: kylin-2.3 (/cubes/cubename/build);
     * kylin-1.5(/cubes/cubename/rebuild)
     *
     * @param cubeName Cube name.
     * @param body     <p>
     *                 startTime - Start timestamp of data to build, e.g.
     *                 1388563200000 for 2014-1-1
     *                 <p>
     *                 endTime - End timestamp of data to build
     *                 <p>
     *                 buildType - Supported build type: ‘BUILD’, ‘MERGE’, ‘REFRESH’
     * @return
     */
    public String buildCube(String cubeName, String body) {
        String method = "PUT";
        String param = "/cubes/" + cubeName + "/build";
        return excute(method, param, body);
    }

    /**
     * enable cube
     *
     * @param cubeName Cube name.
     * @return
     */
    public String enableCube(String cubeName) {
        String method = "PUT";
        String param = "/cubes/" + cubeName + "/enable";
        return excute(method, param, null);
    }

    /**
     * disable cube
     *
     * @param cubeName Cube name.
     * @return
     */
    public String disableCube(String cubeName) {
        String method = "PUT";
        String param = "/cubes/" + cubeName + "/disable";
        return excute(method, param, null);
    }

    /**
     * purge cube
     *
     * @param cubeName Cube name.
     * @return
     */
    public String purgeCube(String cubeName) {
        String method = "PUT";
        String param = "/cubes/" + cubeName + "/purge";
        return excute(method, param, null);
    }

    /**
     * delete segment
     *
     * @param cubeName
     * @param segmentName
     * @return
     */
    public String deleteSegment(String cubeName, String segmentName) {
        String method = "DELETE";
        String param = "/cubes/" + cubeName + "/segs/" + segmentName;
        return excute(method, param, null);
    }

    // job

    /**
     * resume job
     *
     * @param jobId Job id
     * @return
     */
    public String resumeJob(String jobId) {
        String method = "PUT";
        String param = "/jobs/" + jobId + "/resume";
        return excute(method, param, null);
    }

    /**
     * Pause job
     *
     * @param jobId Job id
     * @return
     */
    public String pauseJob(String jobId) {
        String method = "PUT";
        String param = "/jobs/" + jobId + "/pause";
        return excute(method, param, null);
    }

    /**
     * discard job
     *
     * @param jobId Job id.
     * @return
     */
    public String discardJob(String jobId) {

        String method = "PUT";
        String param = "/jobs/" + jobId + "/cancel";
        return excute(method, param, null);

    }

    /**
     * get job status
     *
     * @param jobId Job id.
     * @return
     */
    public String getJobStatus(String jobId) {
        String method = "GET";
        String param = "/jobs/" + jobId;
        return excute(method, param, null);
    }

    /**
     * get job step output
     *
     * @param jobId  Job id.
     * @param stepId Step id; the step id is composed by jobId with step sequence
     *               id; for example, the jobId is
     *               “fb479e54-837f-49a2-b457-651fc50be110”, its 3rd step id is
     *               “fb479e54-837f-49a2-b457-651fc50be110-3”,
     * @return
     */
    public String getJobStepOutput(String jobId, String stepId) {
        String method = "GET";
        String param = "/jobs/" + jobId + "/steps/" + stepId + "/output";
        return excute(method, param, null);
    }

    /**
     * Get job list.
     *
     * @param cubeName
     * @param projectName
     * @param status      (NEW: 0, PENDING: 1, RUNNING: 2, STOPPED: 32, FINISHED: 4,
     *                    ERROR: 8, DISCARDED: 16)
     * @param offset
     * @param limit
     * @param timeFilter  (LAST ONE DAY: 0, LAST ONE WEEK: 1, LAST ONE MONTH: 2, LAST
     *                    ONE YEAR: 3, ALL: 4)
     * @return
     */
    public String getJobList(String cubeName, String projectName, int status, int offset, int limit, int timeFilter) {
        String method = "GET";
        String param = "/jobs?cubeName=" + cubeName + "&projectName=" + projectName + "&status=" + status + "&offset="
                + offset + "&limit=" + limit + "&timeFilter=" + timeFilter;
        return excute(method, param, null);
    }

    // metadata

    /**
     * get hive table
     *
     * @param project   project name
     * @param tableName table name to find.
     * @return
     */
    public String getHiveTable(String project, String tableName) {
        String method = "GET";
        String param = "/tables/" + project + "/" + tableName;
        return excute(method, param, null);
    }

    /**
     * get hive tables
     *
     * @param project will list all tables in the project.
     * @param ext     boolean set true to get extend info of table.
     * @return
     */
    public String getHiveTables(String project, boolean ext) {
        String method = "GET";
        String param = "/tables?project=" + project + "&ext=" + ext;
        return excute(method, param, null);
    }

    /**
     * load hive tables
     *
     * @param tables  table names you want to load from hive, separated with comma.
     * @param project the project which the tables will be loaded into.
     * @return
     */
    public String loadHiveTables(String tables, String project) {
        String method = "POST";
        String param = "/tables/" + tables + "/" + project;
        return excute(method, param, null);
    }

    // cache

    /**
     * @param type   ‘METADATA’ or ‘CUBE’
     * @param name   Cache key, e.g the cube name.
     * @param action ‘create’, ‘update’ or ‘drop’
     * @return
     */
    public String wipeCache(String type, String name, String action) {
        String method = "PUT";
        String param = "/cache/" + type + "/" + name + "/" + action;
        return excute(method, param, null);
    }

    private String excute(String method, String param, String body) {
        return excute(kylinConfig.getConnectionUrl(), auth, param, method, body);
    }

    public String excute(String baseURL, String auth, String para, String method, String body) {
        StringBuilder out = new StringBuilder();
        try {
            URL url = new URL(baseURL + para);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(method);
            connection.setDoOutput(true);
            connection.setRequestProperty("Authorization", "Basic " + auth);
            connection.setRequestProperty("Content-Type", "application/json");
            if (body != null) {
                byte[] outputInBytes = body.getBytes("UTF-8");
                OutputStream os = connection.getOutputStream();
                os.write(outputInBytes);
                os.close();
            }
            InputStream content = (InputStream) connection.getInputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(content));
            String line;
            while ((line = in.readLine()) != null) {
                out.append(line);
            }
            in.close();
            connection.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return out.toString();
    }
}
