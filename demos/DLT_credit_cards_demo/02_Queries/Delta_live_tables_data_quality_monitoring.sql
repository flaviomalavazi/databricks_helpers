-- Create a view to query the external table:
CREATE OR REPLACE VIEW YOUR_CATALOG.YOUR_SCHEMA.expectation_data_ingestion AS 
SELECT
  event_date as event_date,
  row_expectations.dataset as dataset,
  row_expectations.name as expectation,
  SUM(row_expectations.passed_records) as passing_records,
  SUM(row_expectations.failed_records) as failing_records
FROM
  (
    SELECT
    date_trunc("minute", `timestamp`) as event_date,
      explode(
        from_json(
          details :flow_progress :data_quality :expectations,
          "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
        )
      ) row_expectations
    FROM
      (
				select * from event_log("<YOUR_PIPELINE_ID>")
			)
    WHERE
      event_type = 'flow_progress'
  )
GROUP BY
  row_expectations.dataset,
  row_expectations.name,
  event_date;

select
    date_trunc('minute', event_date) day
    , dqm.dataset as dataset
    , case 
    		when dqm.expectation in ('bill_value', 'bill_value_valid') THEN 'bill' 
    		when dqm.expectation in ('card_bin', 'card_bin_in_card_network') THEN 'bin' 
    		when dqm.expectation in ('card_expiration_date_valid') THEN 'exp_date' 
    		when dqm.expectation in ('card_holder', 'card_holder_not_null') THEN 'holder' 
    		when dqm.expectation in ('card_network', 'card_network_in_approved_list') THEN 'network' 
    		when dqm.expectation in ('installments_greater_than_zero', 'installments') THEN 'installments' 
    		when dqm.expectation in ('merchant_name_not_null', 'restaurant_name') THEN 'merchant' 
    		else dqm.expectation
    		end as expectation
    , sum(failing_records)/sum(failing_records+passing_records)*100 failure_rate
    , sum(failing_records) as failing_records
  	, sum(passing_records) as passing_records
from YOUR_CATALOG.YOUR_SCHEMA.expectation_data_ingestion dqm
group by day, dataset, expectation order by day
