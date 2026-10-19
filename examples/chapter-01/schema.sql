BEGIN;

CREATE TABLE accounts (
    account_id    bigint PRIMARY KEY,
    balance_minor bigint NOT NULL CHECK (balance_minor >= 0)
);

CREATE TABLE purchases (
    purchase_id   uuid PRIMARY KEY,
    account_id    bigint NOT NULL REFERENCES accounts,
    product_id    text NOT NULL,
    amount_minor  bigint NOT NULL CHECK (amount_minor > 0),
    currency      text NOT NULL CHECK (currency = 'CNY'),
    status        text NOT NULL CHECK (
        status IN ('pending', 'succeeded', 'insufficient_funds')
    ),
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE orders (
    order_id      uuid PRIMARY KEY,
    purchase_id   uuid NOT NULL UNIQUE REFERENCES purchases,
    product_id    text NOT NULL,
    amount_minor  bigint NOT NULL CHECK (amount_minor > 0),
    currency      text NOT NULL CHECK (currency = 'CNY'),
    status        text NOT NULL CHECK (status = 'paid')
);

CREATE TABLE debits (
    debit_id      uuid PRIMARY KEY,
    order_id      uuid NOT NULL UNIQUE REFERENCES orders,
    account_id    bigint NOT NULL REFERENCES accounts,
    amount_minor  bigint NOT NULL CHECK (amount_minor > 0),
    balance_after_minor bigint NOT NULL
        CHECK (balance_after_minor >= 0),
    created_at    timestamptz NOT NULL DEFAULT now()
);

INSERT INTO accounts(account_id, balance_minor)
VALUES (1, 10000), (2, 10000), (3, 10000);

COMMIT;
