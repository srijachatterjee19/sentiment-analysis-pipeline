run-duckdb:
	docker run --rm -v $(PWD)/data:/data -it duckdb/duckdb /bin/sh -c 'duckdb'

query-duckdb:
	docker run --rm -v $(PWD)/data:/data duckdb/duckdb data/social_enriched/*.parquet \
	"SELECT sentiment, COUNT(*) FROM parquet_scan('data/social_enriched/*.parquet') GROUP BY sentiment;" 

run:
	spark-submit \
	  --jars https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/0.36.1/spark-bigquery-with-dependencies_2.12-0.36.1.jar \
	  spark_job.py

docker-build:
	docker build -t sentiment-spark .

docker-run:
	make run && docker run --rm -v $(PWD):/app -w /app sentiment-spark 
