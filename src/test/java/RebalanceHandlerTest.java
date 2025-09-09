import com.example.*;
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
            "fww9959b-1beb-4f8d-a731-edd5f61568d9",
            criterias
        );
        RebalanceHandler handler = new RebalanceHandler();
        AccountsRebalanceResponse response = handler.handleAsyncRebalRequest(request);
        System.out.println("Matched accounts: " + response.getAccounts());
    }
}
