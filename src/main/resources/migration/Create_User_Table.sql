CREATE TABLE IF NOT EXISTS delta.`/tmp/delta/user` (
  id LONG NOT NULL,
  first_name STRING,
  last_name STRING,
  gender STRING,
  birth_date TIMESTAMP,
  salary INT,
  updated_on DATE
) USING DELTA PARTITIONED BY (updated_on);