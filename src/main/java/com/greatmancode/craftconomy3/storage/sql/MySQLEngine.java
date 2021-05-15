/**
 * This file is part of Craftconomy3.
 *
 * Copyright (c) 2011-2016, Greatman <http://github.com/greatman/>
 *
 * Craftconomy3 is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Craftconomy3 is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with Craftconomy3.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.greatmancode.craftconomy3.storage.sql;

import com.greatmancode.craftconomy3.Common;
import com.greatmancode.craftconomy3.storage.sql.tables.*;
import com.greatmancode.tools.utils.Tools;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MySQLEngine extends SQLStorageEngine {

    public MySQLEngine() {
        HikariConfig config = new HikariConfig();
        initConnectionClass(config);
        String url = "jdbc:mysql://" +
                Common.getInstance().getMainConfig().getString("System.Database.Address")
                + ":" + Common.getInstance().getMainConfig().getString("System.Database.Port")
                + "/" + Common.getInstance().getMainConfig().getString("System.Database.Db")
                + (Common.getInstance().getMainConfig().has("System.Database.UrlParams")
                        ? Common.getInstance().getMainConfig().getString("System.Database.UrlParams") : "");
        if (config.getDataSourceClassName() != null) {
            config.addDataSourceProperty("url", url);
        } else {
            config.setJdbcUrl(url);
        }

        config.setUsername(Common.getInstance().getMainConfig().getString("System.Database.Username"));
        config.setPassword(Common.getInstance().getMainConfig().getString("System.Database.Password"));
        config.setMaximumPoolSize(Common.getInstance().getMainConfig().getInt("System.Database.Poolsize"));
        config.addDataSourceProperty("autoDeserialize", true);
        config.setConnectionTimeout(5000);
        db = new HikariDataSource(config);
        this.tablePrefix = Common.getInstance().getMainConfig().getString("System.Database.Prefix");
        accessTable = new AccessTable(tablePrefix);
        accountTable = new AccountTable(tablePrefix);
        balanceTable = new BalanceTable(tablePrefix);
        configTable = new ConfigTable(tablePrefix);
        currencyTable = new CurrencyTable(tablePrefix);
        exchangeTable = new ExchangeTable(tablePrefix);
        logTable = new LogTable(tablePrefix);
        worldGroupTable = new WorldGroupTable(tablePrefix);
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = db.getConnection();
            statement = connection.prepareStatement(accountTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(currencyTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(configTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(worldGroupTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(balanceTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(logTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(accessTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();

            statement = connection.prepareStatement(exchangeTable.createTableMySQL);
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            Tools.closeJDBCStatement(statement);
            Tools.closeJDBCConnection(connection);
        }
    }

    private void initConnectionClass(HikariConfig config) throws RuntimeException {
        String dataSourceClassName = tryDataSourceClassName("org.mariadb.jdbc.MariaDbDataSource");
        if (dataSourceClassName == null) {
            dataSourceClassName = tryDataSourceClassName("com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        }
        if (dataSourceClassName != null) {
            Common.getInstance().getLogger().info("Using " + dataSourceClassName + " database source");
            config.setDataSourceClassName(dataSourceClassName);
        }

        if (dataSourceClassName == null) {
            String driverClassName = tryDriverClassName("org.mariadb.jdbc.Driver");
            if (driverClassName == null) {
                driverClassName = tryDriverClassName("com.mysql.cj.jdbc.Driver");
            }
            if (driverClassName == null) {
                driverClassName = tryDriverClassName("com.mysql.jdbc.Driver");
            }

            if (driverClassName != null) {
                Common.getInstance().getLogger().info("Using " + driverClassName + " database driver");
                config.setDriverClassName(driverClassName);
            } else {
                throw new RuntimeException("Could not find database driver or data source class! Plugin wont work without a database!");
            }
        }
    }

    private String tryDriverClassName(String className) {
        try {
            Class.forName(className).newInstance();
            return className;
        } catch (Exception ignored) {}
        return null;
    }

    private String tryDataSourceClassName(String className) {
        try {
            Class.forName(className);
            return className;
        } catch (Exception ignored) {}
        return null;
    }

}
