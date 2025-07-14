run-duckdb:
	docker run --rm -v $(PWD)/data:/data -it duckdb/duckdb /bin/sh -c 'duckdb'

query-duckdb:
	docker run --rm -v $(PWD)/data:/data duckdb/duckdb data/social_enriched/*.parquet \
	"SELECT sentiment, COUNT(*) FROM parquet_scan('data/social_enriched/*.parquet') GROUP BY sentiment;" 