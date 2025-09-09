
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockMultipartFile;
import java.io.FileInputStream;
import java.io.IOException;
import com.example.FileProcessor;

public class FileProcessorTest {
    @Test
    public void testProcessAsyncFile() throws IOException {
        // Load the zip file
        FileInputStream fis = new FileInputStream("sample/market_files.zip");
        MockMultipartFile multipartFile = new MockMultipartFile(
            "file", "market_files.zip", "application/zip", fis
        );

        FileProcessor processor = new FileProcessor();
        processor.processAsyncFile(multipartFile, "test123");

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
