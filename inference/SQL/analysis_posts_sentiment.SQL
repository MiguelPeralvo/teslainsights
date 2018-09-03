DROP TABLE IF EXISTS analysis_posts_sentiment;
CREATE TABLE IF NOT EXISTS analysis_posts_sentiment (
post_type VARCHAR(30) NOT NULL,
post_id BIGINT(22) NOT NULL,
body TEXT NULL,
user_name VARCHAR(64) NOT NULL,
created_at_epoch_ms BIGINT(20) NOT NULL,
sentiment_ml_model DECIMAL(5,2) NULL,
sentiment_vader_normalised DECIMAL(5,2) NULL,
sentiment_mixed DECIMAL(5,2) NULL,
client_received_epoch_ms BIGINT(20) NOT NULL,
PRIMARY KEY (`post_type`, `post_id`),
FULLTEXT KEY `body` (`body`));

ALTER TABLE analysis_posts_sentiment ADD INDEX client_received_epoch_ms (client_received_epoch_ms);
ALTER TABLE analysis_posts_sentiment ADD INDEX created_at_epoch_ms (created_at_epoch_ms);
ALTER TABLE analysis_posts_sentiment ADD INDEX user_name (user_name);
