CREATE DATABASE public_dataset
LOCATION '/public-dataset/hive';

DROP TABLE IF EXISTS public_dataset.listings;
CREATE EXTERNAL TABLE public_dataset.listings(
id bigint,
station_id bigint,
commodity_id bigint,
supply bigint,
buy_price bigint,
sell_price bigint,
demand bigint,
collected_at bigint)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "'",
   "escapeChar"    = "\\"
)  
STORED AS TEXTFILE
LOCATION '/public-dataset/hive/listings';

ALTER TABLE public_dataset.listings SET TBLPROPERTIES(
'comment' = 'commodities with bid/ask',
'format' = 'CSV',
'creator' = 'hive',
'dataSetType' = 'PublicDataSet',
'tags' = '[ "market data", "price" ]'
);

SELECT * FROM public_dataset.listings LIMIT 10;


CREATE TABLE public_dataset.stations(
id bigint,
name string,
government string,
allegiance string,
economies array<string>,
market_updated_at bigint,
is_planetary boolean
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '/public-dataset/hive/stations';

ALTER TABLE public_dataset.stations SET TBLPROPERTIES(
'comment' = 'stations list that sell commodities',
'format' = 'JSON',
'creator' = 'hive',
'dataSetType' = 'PublicDataSet',
'tags' = '[ "market data", "referential" ]'
);

CREATE DATABASE project_sample
LOCATION '/project/sample/hive/default';
