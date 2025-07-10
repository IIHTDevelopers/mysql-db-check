package com.example;

import org.junit.jupiter.api.*;
import java.sql.*;

import static com.example.utils.TestUtils.businessTestFile;
import static com.example.utils.TestUtils.currentTest;
import static com.example.utils.TestUtils.testReport;
import static com.example.utils.TestUtils.yakshaAssert;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MySQLDatabaseTest {

    private static final String URL = "jdbc:mysql://localhost:3306/";
    private static final String USER = "root";
    private static final String PASSWORD = "root";

    private static Connection connection;

    @BeforeAll
    public static void setup() throws SQLException {
        connection = DriverManager.getConnection(URL, USER, PASSWORD);
    }

    @AfterAll
    public static void teardown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Test
    @Order(1)
    public void testDatabaseExists() throws SQLException {
        String query = "SHOW DATABASES LIKE 'empdb'";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(), "Database 'empdb' does not exist");
        }
    }

    @Test
    @Order(2)
    public void testTableExists() throws SQLException {
        String query = "SHOW TABLES IN empdb LIKE 'empmaster'";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(), "Table 'empmaster' does not exist in database 'empdb'");
        }
    }

    @Test
    @Order(3)
    public void testTableSchema() throws SQLException {
        String query = "DESCRIBE empdb.empmaster";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            String[][] expectedSchema = {
                {"ID", "int"},
                {"empname", "text"},
                {"empemail", "text"},
                {"dept", "text"},
                {"age", "int"}
            };
            int i = 0;
            while (rs.next()) {
                assertEquals(expectedSchema[i][0], rs.getString("Field"));
                assertTrue(rs.getString("Type").startsWith(expectedSchema[i][1]),
                        "Mismatch in field type for " + expectedSchema[i][0]);
                i++;
            }
            assertEquals(expectedSchema.length, i, "Schema field count does not match");
        }
    }

    @Test
    @Order(4)
    public void testRecordExists() throws SQLException {
        String query = "SELECT * FROM empdb.empmaster WHERE ID = ? AND empname = ? AND empemail = ? AND dept = ? AND age = ?";
        try (PreparedStatement stmt = connection.prepareStatement(query)) {
            stmt.setInt(1, 1);
            stmt.setString(2, "David");
            stmt.setString(3, "david@gmail.com");
            stmt.setString(4, "Finanace");
            stmt.setInt(5, 30);

            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next(), "Record with given values does not exist in 'empmaster'");
            }
        }
    }

    @Test
    @Order(5)
    public void testStoredProceduresExist() throws SQLException {
        String query = "SELECT ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = 'empdb'";
        try (PreparedStatement stmt = connection.prepareStatement(query);
             ResultSet rs = stmt.executeQuery()) {
            assertTrue(rs.next(), "No stored procedures found in 'empdb'");
        }
    }

    @Test
    @Order(6)
    public void testStoredProcedureCall() throws SQLException {
        CallableStatement stmt = connection.prepareCall("{call empdb.getEmployeeById(?)}");
        stmt.setInt(1, 1);
        boolean hasResult = stmt.execute();
    
        assertTrue(hasResult, "Stored procedure did not return a result");
    
        try (ResultSet rs = stmt.getResultSet()) {
            assertTrue(rs.next(), "No employee found with ID=1");
            assertEquals("David", rs.getString("empname"));
        }
    }
    
}
