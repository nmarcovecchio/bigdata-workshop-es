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

DROP TABLE IF EXISTS wallet;
CREATE TABLE wallet (
    ticker varchar(10) NOT NULL,
    tipo varchar(10) NOT NULL,
    datetime timestamptz NOT NULL,
    result double precision	NOT NULL,
  PRIMARY KEY(datetime, ticker)
);