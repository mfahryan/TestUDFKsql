package org.example;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "credit_scoring_status",
        author = "All Data International (https://alldataint.com/)",
        version = "0.0.1",
        description = "A custom formula for important business logic.")
public class CreditScoringStatus {

    @Udf(description = "The standard version of the formula with integer parameters.")
    public String credit_scoring_woe
            (@UdfParameter long sum_woe) {
        String status ;
        if (sum_woe < 400){
            status="BAD";
        } else if (sum_woe > 400 && sum_woe < 700) {
            status="GOOD";
        } else {
            status="APPROVED";
        }
        return status;
    }
}
