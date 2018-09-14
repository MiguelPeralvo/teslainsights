DROP TABLE IF EXISTS analysis_global_sentiment;
CREATE TABLE IF NOT EXISTS analysis_global_sentiment (
sentiment_type VARCHAR(30) NOT NULL,
created_at_epoch_ms BIGINT(20) NOT NULL,
sentiment_absolute DECIMAL(10,3) NULL,
sentiment_normalized DECIMAL(5,3) NULL,
PRIMARY KEY (`sentiment_type`, `created_at_epoch_ms`));

ALTER TABLE analysis_global_sentiment ADD INDEX created_at_epoch_ms (created_at_epoch_ms);

ALTER TABLE analysis_global_sentiment ADD sentiment_seconds_back BIGINT(20) AFTER sentiment_type;

UPDATE analysis_global_sentiment
SET sentiment_seconds_back=3*3600*1000
WHERE created_at_epoch_ms >=(SELECT UNIX_TIMESTAMP(NOW())*1000-(60*24*3600*1000));


ALTER TABLE analysis_global_sentiment
DROP PRIMARY KEY,
ADD PRIMARY KEY (`sentiment_type`, `sentiment_seconds_back`, `created_at_epoch_ms`);