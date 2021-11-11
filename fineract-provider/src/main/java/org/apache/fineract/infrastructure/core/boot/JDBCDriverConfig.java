/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.infrastructure.core.boot;

import org.apache.commons.lang3.StringUtils;
import org.apache.fineract.infrastructure.core.boot.db.DataSourceDialect;
import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.TimeZone;

@Service
public class JDBCDriverConfig {

    @Value("${fineract.ds.driver}")
    private String driverClassName;

    @Value("${fineract.ds.protocol}")
    private String protocol;

    @Value("${fineract.ds.subprotocol}")
    private String subProtocol;

    @Value("${fineract.ds.host}")
    private String host;

    @Value("${fineract.ds.port}")
    private Integer port;

    @Value("${fineract.ds.tenants.db}")
    private String listDbName;

    @Value("${fineract.ds.username}")
    private String username;

    @Value("${fineract.ds.password}")
    private String password;

    @Value("${fineract.ds.dialect}")
    private DataSourceDialect dialect;

    @Value("${fineract.ds.timezone}")
    private String timezone;

    public String getDriverClassName() {
        return driverClassName;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getSubProtocol() {
        return subProtocol;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getListDbName() {
        return listDbName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public DataSourceDialect getDialect() {
        return dialect;
    }

    public String getTimezone() {
        return timezone;
    }

    public String constructUrl(String schemaServer, String schemaServerPort, String schemaName, String connectionParameters) {
        String url = getProtocol() + ':' + getSubProtocol() + "://" + schemaServer + ':' + schemaServerPort + '/' + schemaName;
        if (!Strings.isEmpty(connectionParameters)) {
            url += (url.contains("?") ? "&" : "?") + connectionParameters;
        }
        return url;
    }

    public String constructUrl(String schemaName) {
        String timezone = getTimezone();
        if (StringUtils.isEmpty(timezone)) {
            timezone = TimeZone.getDefault().getID(); // has to be set as some databases are not able to handle summer time
        }
        return constructUrl(getHost(), getPort().toString(), schemaName, "serverTimezone=" + timezone);
    }
}
