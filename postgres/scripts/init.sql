CREATE SCHEMA workshop;

DROP TABLE IF EXISTS stocks;
CREATE TABLE stocks (
  full_date timestamptz NOT NULL,
  symbol varchar(10) NOT NULL,
  category varchar(64) NOT NULL,
  open double precision	NOT NULL,
  high double precision	NOT NULL,
  low double precision	NOT NULL,
  close double precision	NOT NULL,
  MA20 double precision	NOT NULL,
  MA50 double precision	NOT NULL,
  MA100 double precision	NOT NULL,
  PRIMARY KEY(full_date, symbol)
);

DROP TABLE IF EXISTS cryptostocks;
CREATE TABLE cryptostocks (
  datetime timestamptz NOT NULL,
  ticker varchar(10) NOT NULL,
  open double precision	NOT NULL,
  high double precision	NOT NULL,
  low double precision	NOT NULL,
  close double precision	NOT NULL,
  volume double precision	NOT NULL,
  MACD_12_26_9 double precision	NOT NULL,
  MACDh_12_26_9 double precision	NOT NULL,
  MACDs_12_26_9 double precision	NOT NULL,
  RSI_14 double precision	NOT NULL,
  PRIMARY KEY(datetime, ticker)
);