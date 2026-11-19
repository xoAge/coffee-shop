create table outbox (
                        id           uuid primary key,
                        aggregate_id varchar(64)  not null,
                        topic        varchar(100) not null,
                        payload      text         not null,
                        created_at   timestamptz  not null,
                        sent_at      timestamptz
);

-- ретранслятор ищет только неотправленные, по порядку создания
create index idx_outbox_unsent on outbox (created_at) where sent_at is null;