-- AUTHORITY
CREATE TABLE directory(id integer PRIMARY KEY CHECK(id=1),epoch integer NOT NULL,mode text NOT NULL);
INSERT INTO directory VALUES(1,8,'open');
CREATE TABLE sessions(id text PRIMARY KEY,account_id bigint NOT NULL,action text NOT NULL,
 deadline bigint NOT NULL,status text NOT NULL CHECK(status IN ('active','closed')));
CREATE TABLE counters(id integer PRIMARY KEY CHECK(id=1),p bigint NOT NULL,c bigint NOT NULL,
 o bigint NOT NULL,ev bigint NOT NULL,rf bigint NOT NULL);
INSERT INTO counters VALUES(1,0,0,0,0,0);
CREATE TABLE wallets(account_id bigint PRIMARY KEY,balance bigint NOT NULL CHECK(balance>=0));
INSERT INTO wallets VALUES(1,10000);
CREATE TABLE operations(s jsonb PRIMARY KEY,version text NOT NULL,input jsonb NOT NULL,
 p text UNIQUE NOT NULL,c text UNIQUE,t text UNIQUE,acceptance jsonb NOT NULL,result jsonb);
CREATE TABLE debits(id text PRIMARY KEY,p text UNIQUE NOT NULL REFERENCES operations(p),
 account_id bigint NOT NULL,amount bigint NOT NULL CHECK(amount>0),balance_after bigint NOT NULL);
CREATE TABLE charges(c text PRIMARY KEY,p text UNIQUE NOT NULL REFERENCES operations(p),
 t text UNIQUE NOT NULL,scope text NOT NULL,key text NOT NULL,input jsonb NOT NULL,
 status text NOT NULL,provider_result jsonb,payment_result jsonb,o text UNIQUE,ev text UNIQUE,event jsonb,
 UNIQUE(scope,key));
CREATE TABLE workflow(t text PRIMARY KEY,c text UNIQUE NOT NULL REFERENCES charges(c),
 epoch bigint NOT NULL,owner text,status text NOT NULL CHECK(status IN ('ready','running','done','check')));
CREATE TABLE refunds(rf text PRIMARY KEY,s jsonb UNIQUE NOT NULL,source_c text NOT NULL REFERENCES charges(c),
 source_x text NOT NULL,version text NOT NULL,input jsonb NOT NULL,amount bigint NOT NULL CHECK(amount>0),
 status text NOT NULL CHECK(status IN ('pending','succeeded','rejected')),
 scope text NOT NULL,key text NOT NULL,provider_input jsonb NOT NULL,provider_result jsonb,result jsonb,
 UNIQUE(scope,key));
-- LOCAL
CREATE TABLE gate(id integer PRIMARY KEY CHECK(id=1),epoch integer NOT NULL,mode text NOT NULL);
INSERT INTO gate VALUES(1,8,'open');
CREATE TABLE purchases(id text PRIMARY KEY,s jsonb UNIQUE NOT NULL,body jsonb NOT NULL);
CREATE TABLE payments(id text PRIMARY KEY,p text UNIQUE NOT NULL REFERENCES purchases(id),
 scope text NOT NULL,key text NOT NULL,body jsonb NOT NULL,UNIQUE(scope,key));
CREATE TABLE tasks(id text PRIMARY KEY,c text UNIQUE NOT NULL REFERENCES payments(id),body jsonb NOT NULL);
CREATE TABLE orders(id text PRIMARY KEY,p text UNIQUE NOT NULL REFERENCES purchases(id),
 c text UNIQUE NOT NULL REFERENCES payments(id),x text UNIQUE NOT NULL,body jsonb NOT NULL);
CREATE TABLE outbox(id text PRIMARY KEY,o text UNIQUE NOT NULL REFERENCES orders(id),body jsonb NOT NULL);
CREATE TABLE refund_projection(id text PRIMARY KEY,c text NOT NULL REFERENCES payments(id),body jsonb NOT NULL);
CREATE TABLE projection_cursor(id integer PRIMARY KEY CHECK(id=1),body jsonb NOT NULL);
-- PLATFORM
CREATE TABLE accounts(id bigint PRIMARY KEY,balance bigint NOT NULL CHECK(balance>=0));
INSERT INTO accounts VALUES(1,10000);
CREATE TABLE platform_operations(scope text NOT NULL,key text NOT NULL,kind text NOT NULL,
 input jsonb NOT NULL,status text NOT NULL,result jsonb,PRIMARY KEY(scope,key));
-- CONSUMER
CREATE TABLE fulfillments(id bigint GENERATED ALWAYS AS IDENTITY(START WITH 501) PRIMARY KEY,
 order_id text UNIQUE NOT NULL,body jsonb NOT NULL);
CREATE TABLE business_guard(b text PRIMARY KEY,f bigint NOT NULL REFERENCES fulfillments(id));
CREATE TABLE inbox(event_id text PRIMARY KEY,event jsonb NOT NULL,f bigint NOT NULL REFERENCES fulfillments(id),
 receipt jsonb NOT NULL);
CREATE TABLE dispatch(key text PRIMARY KEY,f bigint UNIQUE NOT NULL REFERENCES fulfillments(id),
 body jsonb NOT NULL,result jsonb);
