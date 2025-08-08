package org.example;

import java.sql.*;
import com.google.gson.Gson;

public class DatabaseHelper {
    private static final String URL = "jdbc:mysql://localhost:3306/flipped_lb";
    private static final String USER = "root"; // default XAMPP user
    private static final String PASSWORD = ""; // default XAMPP password is blank

    private static final Gson gson = new Gson();

    static {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver"); // Load MySQL driver
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("MySQL Driver not found", e);
        }
    }

    public static void saveMessage(Message msg, String status) {
        String sql = "INSERT INTO messages (messageId, sourceServerId, destinationId, " +
                "messageType, priority, timestamp, payload, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, msg.getMessageId());
            stmt.setString(2, msg.getSourceServerId());
            stmt.setString(3, msg.getDestinationId());
            stmt.setString(4, msg.getMessageType());
            stmt.setInt(5, msg.getPriority());
            stmt.setLong(6, msg.getTimestamp());
            stmt.setString(7, gson.toJson(msg.getPayload()));
            stmt.setString(8, status);

            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
