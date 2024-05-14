select * from <TABLE>
where <DATE>=DATE_FORMAT(update_date, '%Y%m%d')
into outfile S3 's3://<DATASOURCE_BUCKET_NAME>/<DATE>/<TABLE>/<TABLE>.csv'
FORMAT CSV HEADER
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
MANIFEST ON OVERWRITE ON;