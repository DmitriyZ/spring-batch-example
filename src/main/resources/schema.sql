drop table if exists numbers, result;

create table numbers
(
    id varchar(36) NOT NULL UNIQUE primary key,
    value BIGINT not null,
    status smallint default 0
);

-- create table rule
-- (
--   k BIGINT not null primary key
-- );

create table result
(
  id varchar(36) NOT NULL UNIQUE primary key,
  value BIGINT not null
);

-- insert into rule values (2);
-- insert into rule values (5);