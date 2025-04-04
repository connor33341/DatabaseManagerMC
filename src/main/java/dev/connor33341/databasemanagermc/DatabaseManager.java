package dev.connor33341.databasemanagermc;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;
import org.slf4j.Logger;

public class DatabaseManager {

    private HikariDataSource dataSource;
    private final Logger logger;
    private Connection connection;
    private final String dbUrl;
    private final String dbUsername;
    private final String dbPassword;

    public DatabaseManager(Logger logger, String dbUrl, String dbUsername, String dbPassword) {
        this.logger = logger;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            logger.info("MySQL JDBC Driver registered successfully.");
        } catch (ClassNotFoundException e) {
            logger.error("Failed to register MySQL JDBC Driver: " + e.getMessage());
            throw new RuntimeException("MySQL driver not found in classpath", e);
        }
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.setMaximumPoolSize(15); // May need to be increased later lol
        this.dataSource = new HikariDataSource(config);
        createTableIfNotExists();
    }

    public void connect() throws SQLException {
        logger.info("Successfully connected to the database.");
    }

    public void disconnect() {
        if (dataSource != null && !dataSource.isClosed()){
            dataSource.close();
            logger.info("Disconnected from DB");
        }
    }

    private void createTableIfNotExists() {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS player_data (" +
                "uuid VARCHAR(36) PRIMARY KEY, " +
                "username VARCHAR(16) NOT NULL, " +
                "userid VARCHAR(36) UNIQUE, " +
                "rank VARCHAR(50) DEFAULT 'default', " +
                "balance DOUBLE NOT NULL DEFAULT 0.0, " +
                "other_data TEXT, " +
                "last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(createTableSQL)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error creating table: " + e.getMessage());
        }
    }

    public double getBalance(UUID uuid) {
        String query = "SELECT balance FROM player_data WHERE uuid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, uuid.toString());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getDouble("balance");
            }
        } catch (SQLException e) {
            logger.error("Error fetching balance for " + uuid + ": " + e.getMessage());
        }
        return -1;
    }

    public long getLastUpdated(UUID uuid) {
        String query = "SELECT UNIX_TIMESTAMP(last_updated) FROM player_data WHERE uuid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, uuid.toString());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.error("Error fetching last updated time for " + uuid + ": " + e.getMessage());
        }
        return 0;
    }

    public void setBalance(UUID uuid, double balance) {
        String upsert = "INSERT INTO player_data (uuid, balance) VALUES (?, ?) " +
                "ON DUPLICATE KEY UPDATE balance = ?, last_updated = CURRENT_TIMESTAMP";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(upsert)) {
            stmt.setString(1, uuid.toString());
            stmt.setDouble(2, balance);
            stmt.setDouble(3, balance);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error setting balance for " + uuid + ": " + e.getMessage());
        }
    }

    public void updatePlayerData(UUID uuid, String username, String userid, String rank, double balance, String otherData) {
        String upsert = "INSERT INTO player_data (uuid, username, userid, rank, balance, other_data) " +
                "VALUES (?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "username = ?, userid = ?, rank = ?, balance = ?, other_data = ?, last_updated = CURRENT_TIMESTAMP";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(upsert)) {
            stmt.setString(1, uuid.toString());
            stmt.setString(2, username);
            stmt.setString(3, userid);
            stmt.setString(4, rank);
            stmt.setDouble(5, balance);
            stmt.setString(6, otherData);
            stmt.setString(7, username);
            stmt.setString(8, userid);
            stmt.setString(9, rank);
            stmt.setDouble(10, balance);
            stmt.setString(11, otherData);
            stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error("Error updating player data for " + uuid + ": " + e.getMessage());
        }
    }

    public PlayerData getPlayerData(UUID uuid) {
        String query = "SELECT username, userid, rank, balance, other_data FROM player_data WHERE uuid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, uuid.toString());
            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return new PlayerData(
                        rs.getString("username"),
                        rs.getString("userid"),
                        rs.getString("rank"),
                        rs.getDouble("balance"),
                        rs.getString("other_data")
                );
            }
        } catch (SQLException e) {
            logger.error("Error fetching player data for " + uuid + ": " + e.getMessage());
        }
        return null;
    }

    public static class PlayerData {
        private final String username;
        private final String userid;
        private final String rank;
        private final double balance;
        private final String otherData;

        public PlayerData(String username, String userid, String rank, double balance, String otherData) {
            this.username = username;
            this.userid = userid;
            this.rank = rank;
            this.balance = balance;
            this.otherData = otherData;
        }

        public String getUsername() { return username; }
        public String getUserid() { return userid; }
        public String getRank() { return rank; }
        public double getBalance() { return balance; }
        public String getOtherData() { return otherData; }
    }
}