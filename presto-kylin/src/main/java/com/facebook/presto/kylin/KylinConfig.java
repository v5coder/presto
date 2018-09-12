package com.facebook.presto.kylin;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigSecuritySensitive;

import javax.validation.constraints.NotNull;

public class KylinConfig {

    private String projectName = "kylin-project";
    private String connectionUrl = "http://10.13.10.82:7070/kylin/api";
    private String connectionUser = "admin";
    private String connectionPassword = "KYLIN";

    @Config("kylin.project-name")
    public KylinConfig setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public String getProjectName() {
        return projectName;
    }

    @NotNull
    public String getConnectionUrl() {
        return connectionUrl;
    }

    @Config("connection-url")
    public KylinConfig setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
        return this;
    }

    public String getConnectionUser() {
        return connectionUser;
    }

    @Config("connection-user")
    public KylinConfig setConnectionUser(String connectionUser) {
        this.connectionUser = connectionUser;
        return this;
    }

    public String getConnectionPassword() {
        return connectionPassword;
    }

    @Config("connection-password")
    @ConfigSecuritySensitive
    public KylinConfig setConnectionPassword(String connectionPassword) {
        this.connectionPassword = connectionPassword;
        return this;
    }
}
