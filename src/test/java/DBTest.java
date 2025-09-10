
import org.junit.jupiter.api.Test;
import java.io.IOException;

public class DBTest {
    @Test
    public void testProcessAsyncFile() throws IOException {
    
        // Query H2 DB to verify tables and data
        try {
            Class.forName("org.h2.Driver");
            java.sql.Connection conn = java.sql.DriverManager.getConnection("jdbc:h2:file:./data/upload-db", "sa", "");
            java.sql.Statement stmt = conn.createStatement();

            String[] tables = {"stocks", "market_conditions", "holdings", "accounts"};
            for (String table : tables) {
                java.sql.ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table);
                if (rs.next()) {
                    int count = rs.getInt(1);
                    System.out.println("Table '" + table + "' row count: " + count);
                }
                rs.close();
            }
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
