package org.example;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "credit_scoring",
        author = "All Data International (https://alldataint.com/)",
        version = "0.0.1",
        description = "A custom formula for important business logic.")
public class CreditScoring {

    @Udf(description = "The standard version of the formula with integer parameters.")
    public long credit_scoring_woe
            (@UdfParameter double woe_age,
             @UdfParameter double woe_debt_ratio,
             @UdfParameter double woe_monthly_income,
             @UdfParameter double woe_30_59_days,
             @UdfParameter double woe_60_89_days,
             @UdfParameter double woe_90_days,
             @UdfParameter double no_depedents) {
        if (woe_age < 51.5){
            woe_age=144;
        } else if ( woe_age > 51.5 && woe_age <= 59.6) {
            woe_age=103;
        } else if ( woe_age > 59.6 && woe_age <= 62.5) {
            woe_age=100;
        } else {
            woe_age=82;
        }

        if (woe_debt_ratio < 0.5){
            woe_debt_ratio=101;
        } else if (woe_debt_ratio > 0.5 && woe_debt_ratio <= 1.5) {
            woe_debt_ratio=116;
        }else if (woe_debt_ratio > 1.5 && woe_debt_ratio <= 2.5) {
            woe_debt_ratio=134;
        }else {
            woe_debt_ratio=80;
        }

        if (woe_monthly_income < 937){
            woe_monthly_income=95;
        } else if (woe_monthly_income > 937 && woe_monthly_income <= 5301.5) {
            woe_monthly_income=110;
        }else {
            woe_monthly_income=101;
        }

        if (woe_30_59_days < 0.5){
            woe_30_59_days=91;
        } else if ( woe_30_59_days > 0.5 && woe_30_59_days <= 0.9) {
            woe_30_59_days=131;
        } else if ( woe_30_59_days > 0.9 && woe_30_59_days <= 4) {
            woe_30_59_days=145;
        } else {
            woe_30_59_days=163;
        }

        if (woe_60_89_days <= 0.5){
            woe_60_89_days=100;
        } else if (woe_60_89_days > 0.5 && woe_60_89_days <= 1.5) {
            woe_60_89_days=142;
        }else {
            woe_60_89_days=167;
        }

        if (woe_90_days <= 0.5){
            woe_90_days=96;
        } else if (woe_90_days > 0.5 && woe_90_days <= 1.5) {
            woe_90_days=155;
        }else {
            woe_90_days=183;
        }

        if (no_depedents <= 2){
            no_depedents=105;
        }else {
            no_depedents=108;
        }

        double sum_woe = woe_age +
                woe_debt_ratio +
                woe_monthly_income +
                woe_30_59_days +
                woe_60_89_days +
                woe_90_days +
                no_depedents;

        return ((long) sum_woe);
    }
}
