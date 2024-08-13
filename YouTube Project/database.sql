CREATE DATABASE if not EXISTS Youtube_data;
USE Youtube_data;

CREATE TABLE IF NOT EXISTS channel_data (
    channel_name VARCHAR(255),
    channel_id VARCHAR(255),
    subscription_count BIGINT,
    channel_views BIGINT,
    channel_description TEXT,
    playlist_id VARCHAR(255)
);
