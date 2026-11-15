create table orders (
                        id            bigserial primary key,
                        drink         varchar(50) not null,
                        customer_name varchar(50) not null,
                        status        varchar(20) not null,
                        created_at    timestamptz not null
);