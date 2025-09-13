CREATE TABLE IF NOT EXISTS eod_summary(
  as_of date PRIMARY KEY,
  orders int, fills int, symbols int, realized_pnl double precision, created_at timestamptz default now()
);
