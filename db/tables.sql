use edgetrail_db;

create table futures_daily(
	id int not null auto_increment,
    ticker varchar(50) not null,
    date DATE not null,
    open decimal(10,4),
    high decimal(10,4),
    low decimal(10,4),
    close decimal(10,4),
    volume BIGINT,
    PRIMARY KEY(id),
    UNIQUE KEY uk_futures_daily (ticker,date),
    INDEX idx_futures_daily (ticker,date));

CREATE TABLE fred_monthly (
    id INT NOT NULL AUTO_INCREMENT,
    series_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(12, 6),
    units VARCHAR(50),
    series_name VARCHAR(100),
    PRIMARY KEY (id),
    UNIQUE KEY uk_fred_monthly (series_id, date),
    INDEX idx_fred_monthly_series_date (series_id, date)
);

CREATE TABLE fred_monthly (
    id INT NOT NULL AUTO_INCREMENT,
    series_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(12, 6),
    units VARCHAR(50),
    series_name VARCHAR(100),
    PRIMARY KEY (id),
    UNIQUE KEY uk_fred_monthly (series_id, date),
    INDEX idx_fred_monthly_series_date (series_id, date)
);

CREATE TABLE fred_quarterly (
    id INT NOT NULL AUTO_INCREMENT,
    series_id VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    value DECIMAL(12, 6),
    units VARCHAR(50),
    series_name VARCHAR(100),
    PRIMARY KEY (id),
    UNIQUE KEY uk_fred_quarterly (series_id, date),
    INDEX idx_fred_quarterly_series_date (series_id, date)
);
