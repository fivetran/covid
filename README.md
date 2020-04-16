This repo contains the data and analysis described in http://fivetran.com/blog/covid-19-count

`1.Load.sql` and `2.Transform.sql` load the data into the BigQuery project https://console.cloud.google.com/bigquery?project=fivetran-covid&p=fivetran-covid&d=covid&page=dataset

The data is publically accessible, so you can reproduce our results simply by running the script in `4.Evaluate.sql` in BigQuery.