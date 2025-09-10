import com.example.*;
import com.example.model.AccountRebalanceCriteria;
import com.example.model.RebalanceRequest;
import com.example.service.RebalanceHandler;

import org.junit.jupiter.api.Test;
import java.util.*;

public class RebalanceHandlerTest {
    @Test
    public void testHandleAsyncRebalRequest() {
        List<AccountRebalanceCriteria> criterias = Arrays.asList(
            new AccountRebalanceCriteria("age", ">", "40"),
            new AccountRebalanceCriteria("annualIncome", "=", "250000"),
            new AccountRebalanceCriteria("riskTolerance", "=", "Moderate")
        );
        RebalanceRequest request = new RebalanceRequest(
            "fww9959b-1beb-4f8d-a731-edd5f6156802",
            criterias
        );
        RebalanceHandler handler = new RebalanceHandler();
        String response = handler.handleAsyncRebalRequest(request);
        System.out.println("Rebalance request result: " + response);
        assert "done".equals(response);
        // Wait for async thread to finish
        try {
            Thread.sleep(2000); // Adjust time as needed
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
