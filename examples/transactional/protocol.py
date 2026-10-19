"""Executable teaching subset, not a service or production payment integration.

J is one PostgreSQL cluster. Local data, platform ledger and consumer each use
another cluster. Authentication and trusted time are harness inputs. The payment,
Seal, delivery and dispatch APIs are deterministic fixtures. SQL transactions are
real, READ COMMITTED, fsync and synchronous_commit on. All amounts are minor units.
"""

import hashlib
import json

from db import jb, lit, sql, value

CHARGE_SCOPE = "simulator:merchant-7:test:charge.v1"
REFUND_SCOPE = "simulator:merchant-7:test:refund.v1"
DEADLINE = 1000
OCCURRED = "2026-10-18T01:00:00Z"


class ProtocolError(RuntimeError):
    pass


def operation(account=1, action="purchase.create.external.v2", session="E1", key="K1"):
    return {"account_id": account, "action": action, "session_id": session, "idempotency_key": key}


def purchase_input(amount=3000, version="external-v2"):
    if version == "local-v1":
        return {
            "product_id": "paper-001",
            "quantity": 1,
            "quote_id": "Q7",
            "currency": "CNY",
            "amount_minor": amount,
        }
    return {
        "product_id": "paper-001",
        "quantity": 1,
        "quote_id": "Q7",
        "currency": "CNY",
        "amount_minor": amount,
        "payment_method": "saved-card-7",
    }


def register(session="E1", action="purchase.create.external.v2"):
    # A duplicate registration must exactly match. A missing OLD session is never
    # registered by accept(); only this explicit operator-controlled test setup.
    sql(
        "authority",
        f"""
      INSERT INTO sessions VALUES({lit(session)},1,{lit(action)},{DEADLINE},'active');
    """,
    )


def close_session(session="E1"):
    sql(
        "authority",
        f"""BEGIN;
      SELECT id FROM sessions WHERE id={lit(session)} FOR UPDATE;
      UPDATE sessions SET status='closed' WHERE id={lit(session)};
      COMMIT;""",
    )


def accept(s, inp, version="external-v2", now_upper=1):
    """A_J (or the local-v1 transaction). A single session row orders close/A_J.

    Harness authentication is account=1 and the exact permitted action. Quotes
    are the fixed Q7 fixture; existing bindings precede current quote validation.
    """
    action = s["action"]
    session = s["session_id"]
    if s["account_id"] != 1 or action not in ("purchase.create.external.v2", "purchase.create.v1"):
        return {"code": "unauthorized"}
    expected_version = "local-v1" if action.endswith(".v1") else "external-v2"
    if version != expected_version:
        return {"code": "invalid_input_version"}
    fields = {"product_id", "quantity", "quote_id", "currency", "amount_minor"}
    if version == "external-v2":
        fields.add("payment_method")
    structurally_valid = (
        set(inp) == fields and type(inp["amount_minor"]) is int and inp["amount_minor"] > 0
    )
    valid_quote = (
        structurally_valid
        and inp["product_id"] == "paper-001"
        and inp["quantity"] == 1
        and inp["quote_id"] == "Q7"
        and inp["currency"] == "CNY"
        and inp["amount_minor"] == 3000
        and (version != "external-v2" or inp["payment_method"] == "saved-card-7")
    )
    result = value(
        "authority",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.accept() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE sess sessions; old operations; n counters; bal bigint;
      pid text; cid text; tid text; acc jsonb; ans jsonb; pi jsonb;
    BEGIN
      SELECT * INTO sess FROM sessions WHERE id={lit(session)} FOR UPDATE;
      IF NOT FOUND OR sess.status<>'active' OR sess.deadline<={int(now_upper)}
        OR sess.account_id<>1 OR sess.action<>{lit(action)} THEN
        RETURN '{{"code":"operation_expired"}}'::jsonb;
      END IF;
      PERFORM id FROM directory WHERE id=1 FOR UPDATE;
      IF (SELECT mode FROM directory WHERE id=1)<>'open' THEN
        RETURN '{{"code":"recovering"}}'::jsonb;
      END IF;
      SELECT * INTO old FROM operations WHERE s={jb(s)};
      IF FOUND THEN
        IF old.version<>{lit(version)} OR old.input<>{jb(inp)} THEN
          RETURN '{{"code":"conflict"}}'::jsonb;
        END IF;
        RETURN coalesce(old.result,old.acceptance);
      END IF;
      IF NOT {lit(valid_quote)} THEN RETURN '{{"code":"invalid_purchase_input"}}'::jsonb; END IF;
      UPDATE counters SET p=p+1 WHERE id=1 RETURNING * INTO n;
      pid := 'P'||n.p; cid := NULL; tid := NULL;
      IF {lit(version)}='external-v2' THEN
        UPDATE counters SET c=c+1 WHERE id=1 RETURNING * INTO n;
        cid:='C'||n.c; tid:='T'||n.c;
      END IF;
      acc:=jsonb_build_object('purchase_id',pid,'charge_id',cid,'operation',{jb(s)},
        'product_id','paper-001','quantity',1,'quote_id','Q7','currency','CNY','amount_minor',3000);
      IF {lit(version)}='local-v1' THEN
        SELECT balance INTO bal FROM wallets WHERE account_id=1 FOR UPDATE;
        IF bal<3000 THEN
          ans:=jsonb_build_object('idempotency_key',{lit(s["idempotency_key"])},'purchase_id',pid,
            'status','rejected','reason','insufficient_funds');
          INSERT INTO operations VALUES({jb(s)},{lit(version)},{jb(inp)},pid,NULL,NULL,acc,ans);
          RETURN ans;
        END IF;
        UPDATE wallets SET balance=balance-3000 WHERE account_id=1 RETURNING balance INTO bal;
        UPDATE counters SET o=o+1 WHERE id=1 RETURNING * INTO n;
        ans:=jsonb_build_object('idempotency_key',{lit(s["idempotency_key"])},'purchase_id',pid,
          'status','succeeded','order_id','O'||n.o,'debit_id','D'||n.p,
          'product_id','paper-001','quote_id','Q7','currency','CNY','amount_minor',3000,
          'balance_after_minor',bal,'retry_until','2026-10-18T10:00:00Z');
        INSERT INTO operations VALUES({jb(s)},{lit(version)},{jb(inp)},pid,NULL,NULL,acc,ans);
        INSERT INTO debits VALUES('D'||n.p,pid,1,3000,bal);
        RETURN ans;
      END IF;
      INSERT INTO operations VALUES({jb(s)},{lit(version)},{jb(inp)},pid,cid,tid,acc,NULL);
      pi:=jsonb_build_object('account_id',1,'charge_id',cid,'currency','CNY','amount_minor',3000);
      INSERT INTO charges VALUES(cid,pid,tid,{lit(CHARGE_SCOPE)},'pay-'||cid,pi,
        'unconfirmed',NULL,NULL,NULL,NULL,NULL);
      INSERT INTO workflow VALUES(tid,cid,0,NULL,'ready');
      RETURN acc;
    END $fn$;
    SELECT pg_temp.accept();
    """,
    )
    return result


def charge(c="C1"):
    return value("authority", f"SELECT to_jsonb(rowdata) FROM charges rowdata WHERE c={lit(c)}")


def query_operation(s, authenticated_account=1):
    """Authorized read independent of expired admission; exact complete S lookup.

    The fixture's authenticated principal is supplied by its harness. A network
    deployment must establish it from verified credentials, not a request field.
    """
    if authenticated_account != s["account_id"]:
        return {"code": "unauthorized"}
    return value(
        "authority",
        f"""SELECT coalesce(
      (SELECT jsonb_build_object('acceptance',acceptance,'result',result,'input_version',version,
                                'original_input',input,'charge_id',c,'task_id',t)
       FROM operations WHERE s={jb(s)}),
      '{{"code":"not_found_in_complete_authority"}}'::jsonb)""",
    )


def claim(c="C1", owner="W1"):
    return value(
        "authority",
        f"""UPDATE workflow SET epoch=epoch+1,owner={lit(owner)},status='running'
      WHERE c={lit(c)} AND status<>'done' RETURNING to_jsonb(workflow)""",
    )


def platform_call(scope, key, kind, inp, mode="succeed"):
    """Fixture keyed by exact scope and input; modes pending/reject/succeed.

    A pending operation may only change through resolve_platform(), not by
    interpreting a retry's mode. Seal closes a missing identity in this same row.
    """
    return value(
        "platform",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.apply() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE r platform_operations; bal bigint; ans jsonb; ident text;
    BEGIN
      PERFORM id FROM accounts WHERE id=1 FOR UPDATE;
      SELECT * INTO r FROM platform_operations WHERE scope={lit(scope)} AND key={lit(key)};
      IF FOUND THEN
        IF r.kind<>{lit(kind)} OR r.input<>{jb(inp)} THEN RETURN '{{"code":"provider_conflict"}}'::jsonb; END IF;
        RETURN coalesce(r.result,jsonb_build_object('status','pending','operation_id','H:'||r.key));
      END IF;
      IF {lit(mode)}='pending' THEN
        INSERT INTO platform_operations VALUES({lit(scope)},{lit(key)},{lit(kind)},{jb(inp)},'pending',NULL);
        RETURN jsonb_build_object('status','pending','operation_id','H:'||{lit(key)});
      END IF;
      IF {lit(mode)}='reject' THEN
        ans:=jsonb_build_object('status','rejected','scope',{lit(scope)},'key',{lit(key)},
          'kind',{lit(kind)},'input',{jb(inp)},'final',true,'past_no_success',true,
          'accepted_and_inflight_terminated',true,'future_closed',true);
        INSERT INTO platform_operations VALUES({lit(scope)},{lit(key)},{lit(kind)},{jb(inp)},'rejected',ans);
        RETURN ans;
      END IF;
      IF {lit(kind)}='refund' AND NOT EXISTS(
        SELECT 1 FROM platform_operations p WHERE p.kind='charge' AND p.status='succeeded'
         AND p.result->>'provider_charge_id'={lit(inp.get("source_x", ""))}) THEN
        RAISE EXCEPTION 'missing original successful charge';
      END IF;
      IF {lit(kind)}='charge' THEN
        UPDATE accounts SET balance=balance-{int(inp["amount_minor"])} WHERE id=1 RETURNING balance INTO bal;
        ident:='X'||(SELECT count(*)+1 FROM platform_operations WHERE kind='charge' AND status='succeeded');
        ans:=jsonb_build_object('status','succeeded','scope',{lit(scope)},'key',{lit(key)},
          'account_id',1,'charge_id',{lit(inp.get("charge_id"))},'provider_charge_id',ident,
          'currency','CNY','amount_minor',{int(inp["amount_minor"])});
      ELSE
        IF (SELECT coalesce(sum((input->>'amount_minor')::bigint),0) FROM platform_operations
            WHERE kind='refund' AND status='succeeded' AND input->>'source_x'={lit(inp.get("source_x", ""))})
            +{int(inp["amount_minor"])}>
            (SELECT (result->>'amount_minor')::bigint FROM platform_operations
             WHERE kind='charge' AND result->>'provider_charge_id'={lit(inp.get("source_x", ""))}) THEN
          RAISE EXCEPTION 'platform refund capacity exceeded';
        END IF;
        UPDATE accounts SET balance=balance+{int(inp["amount_minor"])} WHERE id=1 RETURNING balance INTO bal;
        ident:='PX-RF'||(SELECT count(*)+1 FROM platform_operations WHERE kind='refund' AND status='succeeded');
        ans:=jsonb_build_object('status','succeeded','scope',{lit(scope)},'key',{lit(key)},
          'refund_id',{lit(inp.get("refund_id"))},'source_charge_id',{lit(inp.get("source_c"))},
          'source_provider_charge_id',{lit(inp.get("source_x"))},'provider_refund_id',ident,
          'currency','CNY','amount_minor',{int(inp["amount_minor"])});
      END IF;
      INSERT INTO platform_operations VALUES({lit(scope)},{lit(key)},{lit(kind)},{jb(inp)},'succeeded',ans);
      RETURN ans;
    END $fn$;
    SELECT pg_temp.apply();
    """,
    )


def resolve_platform(scope, key, succeed=True):
    old = value(
        "platform",
        f"SELECT to_jsonb(rowdata) FROM platform_operations rowdata WHERE scope={lit(scope)} AND key={lit(key)}",
    )
    if old["status"] != "pending":
        return old["result"]
    # The fixture resolver is a platform-side state transition under the account
    # lock. Delete+reapply is inside one transaction; no other caller can observe
    # an unbound key. The public caller cannot invoke this privileged transition.
    # Implement as a temporary, exact replacement in one SQL batch by nesting
    # the existing function body, preserving this one transaction's account lock.
    # Use the public call to install pg_temp.apply, then adapt its call in one
    # connection. Extracting SQL is avoided: a resolver supplies a recorded
    # definitive rejection, or an explicit successful ledger transition below.
    return value(
        "platform",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.resolve() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE r platform_operations; ans jsonb; ident text;
    BEGIN
      PERFORM id FROM accounts WHERE id=1 FOR UPDATE;
      SELECT * INTO r FROM platform_operations WHERE scope={lit(scope)} AND key={lit(key)} FOR UPDATE;
      IF r.status<>'pending' THEN RETURN r.result; END IF;
      IF NOT {lit(succeed)} THEN
        ans:=jsonb_build_object('status','rejected','scope',r.scope,'key',r.key,'kind',r.kind,'input',r.input,
          'final',true,'past_no_success',true,'accepted_and_inflight_terminated',true,'future_closed',true);
      ELSIF r.kind='refund' THEN
        IF (SELECT coalesce(sum((input->>'amount_minor')::bigint),0) FROM platform_operations
            WHERE kind='refund' AND status='succeeded' AND input->>'source_x'=r.input->>'source_x')
            +(r.input->>'amount_minor')::bigint>
            (SELECT (result->>'amount_minor')::bigint FROM platform_operations
             WHERE kind='charge' AND result->>'provider_charge_id'=r.input->>'source_x') THEN
          RAISE EXCEPTION 'platform refund capacity exceeded';
        END IF;
        UPDATE accounts SET balance=balance+(r.input->>'amount_minor')::bigint WHERE id=1;
        ident:='PX-RF'||(SELECT count(*)+1 FROM platform_operations WHERE kind='refund' AND status='succeeded');
        ans:=jsonb_build_object('status','succeeded','scope',r.scope,'key',r.key,
          'refund_id',r.input->>'refund_id','source_charge_id',r.input->>'source_c',
          'source_provider_charge_id',r.input->>'source_x','provider_refund_id',ident,
          'currency','CNY','amount_minor',(r.input->>'amount_minor')::bigint);
      ELSE
        UPDATE accounts SET balance=balance-(r.input->>'amount_minor')::bigint WHERE id=1;
        ident:='X'||(SELECT count(*)+1 FROM platform_operations WHERE kind='charge' AND status='succeeded');
        ans:=jsonb_build_object('status','succeeded','scope',r.scope,'key',r.key,
          'account_id',1,'charge_id',r.input->>'charge_id','provider_charge_id',ident,
          'currency','CNY','amount_minor',(r.input->>'amount_minor')::bigint);
      END IF;
      UPDATE platform_operations SET status=ans->>'status',result=ans WHERE scope=r.scope AND key=r.key;
      IF NOT FOUND THEN RAISE EXCEPTION 'platform resolve updated zero rows'; END IF;
      RETURN ans;
    END $fn$;
    SELECT pg_temp.resolve();""",
    )


def seal(c="C1"):
    q = charge(c)
    return platform_call(q["scope"], q["key"], "charge", q["input"], "reject")


def execute(c="C1", mode="succeed", lose_response=False):
    q = charge(c)
    ans = platform_call(q["scope"], q["key"], "charge", q["input"], mode)
    if lose_response:
        raise ProtocolError("injected: platform committed, response lost before R_J")
    return ans


def observe_charge(evidence, c="C1", token=None, owner=None):
    q = charge(c)
    status = evidence.get("status")
    if status == "pending":
        return {"status": "unconfirmed"}
    if status == "succeeded":
        expected = {
            "status",
            "scope",
            "key",
            "account_id",
            "charge_id",
            "provider_charge_id",
            "currency",
            "amount_minor",
        }
        if (
            set(evidence) != expected
            or evidence["scope"] != q["scope"]
            or evidence["key"] != q["key"]
            or evidence["charge_id"] != c
            or evidence["account_id"] != 1
            or evidence["currency"] != "CNY"
            or evidence["amount_minor"] != q["input"]["amount_minor"]
        ):
            raise ProtocolError("untrusted or mismatched charge evidence")
    elif status == "rejected":
        if (
            evidence.get("scope") != q["scope"]
            or evidence.get("key") != q["key"]
            or evidence.get("input") != q["input"]
            or evidence.get("kind") != "charge"
            or not all(
                evidence.get(k) is True
                for k in (
                    "final",
                    "past_no_success",
                    "accepted_and_inflight_terminated",
                    "future_closed",
                )
            )
        ):
            raise ProtocolError("insufficient final rejection evidence")
    else:
        raise ProtocolError("not terminal evidence")
    tokencheck = "true" if token is None else f"w.epoch={int(token)} AND w.owner={lit(owner)}"
    return value(
        "authority",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.observe() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE q charges; w workflow; n counters; decision_result jsonb; evt jsonb; acc jsonb;
    BEGIN
      SELECT * INTO q FROM charges WHERE c={lit(c)} FOR UPDATE;
      SELECT * INTO w FROM workflow WHERE t=q.t FOR UPDATE;
      IF NOT ({tokencheck}) THEN RETURN '{{"code":"stale_owner"}}'::jsonb; END IF;
      IF q.status<>'unconfirmed' THEN
        IF q.provider_result<>{jb(evidence)} THEN RAISE EXCEPTION 'incompatible charge terminal evidence'; END IF;
        RETURN q.payment_result;
      END IF;
      IF {lit(status)}='succeeded' THEN
        PERFORM id FROM directory WHERE id=1 FOR UPDATE;
        UPDATE counters SET o=o+1,ev=ev+1 WHERE id=1 RETURNING * INTO n;
        decision_result:=jsonb_build_object('purchase_id',q.p,'charge_id',q.c,'order_id','O'||n.o,
          'status','succeeded','provider_scope',q.scope,'provider_charge_id',{lit(evidence.get("provider_charge_id"))},
          'currency','CNY','amount_minor',(q.input->>'amount_minor')::bigint);
        evt:=jsonb_build_object('event_id','EV'||n.ev,'event_type','purchase.paid','event_version',1,
          'occurred_at',{lit(OCCURRED)},'payload',jsonb_build_object('purchase_id',q.p,
          'charge_id',q.c,'order_id','O'||n.o,'provider_charge_id',{lit(evidence.get("provider_charge_id"))},
          'currency','CNY','amount_minor',(q.input->>'amount_minor')::bigint));
        UPDATE charges SET status='succeeded',provider_result={jb(evidence)},payment_result=decision_result,
          o='O'||n.o,ev='EV'||n.ev,event=evt WHERE charges.c=q.c;
      ELSE
        decision_result:=jsonb_build_object('purchase_id',q.p,'charge_id',q.c,'status','rejected',
          'reason','provider_final_rejection','provider_scope',q.scope,'currency','CNY',
          'amount_minor',(q.input->>'amount_minor')::bigint);
        UPDATE charges SET status='rejected',provider_result={jb(evidence)},payment_result=decision_result WHERE charges.c=q.c;
      END IF;
      IF NOT FOUND THEN RAISE EXCEPTION 'charge finalization updated zero rows'; END IF;
      UPDATE workflow SET status='done' WHERE t=q.t;
      IF NOT FOUND THEN RAISE EXCEPTION 'workflow finalization updated zero rows'; END IF;
      SELECT acceptance INTO acc FROM operations WHERE p=q.p;
      UPDATE operations SET result=jsonb_build_object('result_version','purchase-result-external-v2',
        'acceptance',acc,'progress',jsonb_build_object('business_state',
        CASE WHEN {lit(status)}='succeeded' THEN 'paid' ELSE 'payment_rejected' END,
        'fulfillment_state','unfulfilled'),'payment_result',decision_result) WHERE p=q.p;
      IF NOT FOUND THEN RAISE EXCEPTION 'operation finalization updated zero rows'; END IF;
      RETURN decision_result;
    END $fn$;
    SELECT pg_temp.observe();""",
    )


def accept_refund(s, source_c, inp, version="refund-v1", now_upper=1):
    """Lock original charge and reserve pending+success in the same admission."""
    if s["account_id"] != 1 or s["action"] != "refund.create.v1" or version != "refund-v1":
        return {"code": "unauthorized"}
    valid = (
        set(inp) == {"source_charge_id", "currency", "amount_minor", "reason"}
        and inp.get("source_charge_id") == source_c
        and inp.get("currency") == "CNY"
        and type(inp.get("amount_minor")) is int
        and inp["amount_minor"] > 0
        and inp.get("reason") in ("customer_request", "partial_return")
    )
    return value(
        "authority",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.refund() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE sess sessions; old refunds; q charges; n counters; used bigint; rid text; pi jsonb; ans jsonb;
    BEGIN
      SELECT * INTO sess FROM sessions WHERE id={lit(s["session_id"])} FOR UPDATE;
      IF NOT FOUND OR sess.status<>'active' OR sess.deadline<={int(now_upper)}
        OR sess.account_id<>1 OR sess.action<>'refund.create.v1' THEN
        RETURN '{{"code":"operation_expired"}}'::jsonb; END IF;
      SELECT * INTO old FROM refunds WHERE s={jb(s)};
      IF FOUND THEN
        IF old.source_c<>{lit(source_c)} OR old.version<>{lit(version)} OR old.input<>{jb(inp)} THEN
          RETURN '{{"code":"conflict"}}'::jsonb; END IF;
        RETURN coalesce(old.result,jsonb_build_object('refund_id',old.rf,'status','pending','amount_minor',old.amount));
      END IF;
      SELECT * INTO q FROM charges WHERE c={lit(source_c)} FOR UPDATE;
      IF NOT FOUND OR q.status<>'succeeded' THEN RETURN '{{"code":"source_not_successful"}}'::jsonb; END IF;
      PERFORM id FROM directory WHERE id=1 FOR UPDATE;
      IF (SELECT mode FROM directory WHERE id=1)<>'open' THEN RETURN '{{"code":"recovering"}}'::jsonb; END IF;
      IF NOT {lit(valid)} THEN RETURN '{{"code":"invalid_refund_input"}}'::jsonb; END IF;
      SELECT coalesce(sum(amount),0) INTO used FROM refunds WHERE source_c=q.c AND status IN ('pending','succeeded');
      IF used+{int(inp.get("amount_minor", 0))}>(q.input->>'amount_minor')::bigint THEN
        RETURN '{{"code":"refund_capacity_unavailable"}}'::jsonb; END IF;
      UPDATE counters SET rf=rf+1 WHERE id=1 RETURNING * INTO n;
      rid:='RF'||n.rf;
      pi:=jsonb_build_object('refund_id',rid,'source_c',q.c,'source_x',q.provider_result->>'provider_charge_id',
          'currency','CNY','amount_minor',{int(inp.get("amount_minor", 0))});
      INSERT INTO refunds VALUES(rid,{jb(s)},q.c,q.provider_result->>'provider_charge_id',
        {lit(version)},{jb(inp)},{int(inp.get("amount_minor", 0))},'pending',
        {lit(REFUND_SCOPE)},'refund-'||rid,pi,NULL,NULL);
      ans:=jsonb_build_object('refund_id',rid,'status','pending','amount_minor',{int(inp.get("amount_minor", 0))});
      RETURN ans;
    END $fn$;
    SELECT pg_temp.refund();""",
    )


def refund_row(rf):
    return value("authority", f"SELECT to_jsonb(rowdata) FROM refunds rowdata WHERE rf={lit(rf)}")


def execute_refund(rf, mode="succeed", lose_response=False):
    r = refund_row(rf)
    ans = platform_call(r["scope"], r["key"], "refund", r["provider_input"], mode)
    if lose_response:
        raise ProtocolError("injected: refund credited, response lost before refund decision")
    return ans


def observe_refund(rf, evidence):
    r = refund_row(rf)
    if evidence.get("status") == "pending":
        return {"refund_id": rf, "status": "pending"}
    success = evidence.get("status") == "succeeded"
    if success:
        keys = {
            "status",
            "scope",
            "key",
            "refund_id",
            "source_charge_id",
            "source_provider_charge_id",
            "provider_refund_id",
            "currency",
            "amount_minor",
        }
        valid = (
            set(evidence) == keys
            and evidence["scope"] == r["scope"]
            and evidence["key"] == r["key"]
            and evidence["refund_id"] == rf
            and evidence["source_charge_id"] == r["source_c"]
            and evidence["source_provider_charge_id"] == r["source_x"]
            and evidence["currency"] == "CNY"
            and evidence["amount_minor"] == r["amount"]
        )
    else:
        valid = (
            evidence.get("status") == "rejected"
            and evidence.get("scope") == r["scope"]
            and evidence.get("key") == r["key"]
            and evidence.get("input") == r["provider_input"]
            and evidence.get("kind") == "refund"
            and all(
                evidence.get(k) is True
                for k in (
                    "final",
                    "past_no_success",
                    "accepted_and_inflight_terminated",
                    "future_closed",
                )
            )
        )
    if not valid:
        raise ProtocolError("untrusted or insufficient refund evidence")
    result = {
        "refund_id": rf,
        "source_charge_id": r["source_c"],
        "source_provider_charge_id": r["source_x"],
        "status": "succeeded" if success else "rejected",
        "currency": "CNY",
        "amount_minor": r["amount"],
    }
    if success:
        result["provider_refund_id"] = evidence["provider_refund_id"]
    else:
        result["reason"] = "provider_final_rejection"
    return value(
        "authority",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.observe() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE old refunds;
    BEGIN
      PERFORM c FROM charges WHERE c={lit(r["source_c"])} FOR UPDATE;
      SELECT * INTO old FROM refunds WHERE rf={lit(rf)} FOR UPDATE;
      IF old.status<>'pending' THEN
        IF old.provider_result<>{jb(evidence)} OR old.result<>{jb(result)} THEN
          RAISE EXCEPTION 'incompatible refund terminal evidence'; END IF;
        RETURN old.result;
      END IF;
      UPDATE refunds SET status={lit(result["status"])},provider_result={jb(evidence)},result={jb(result)}
        WHERE refunds.rf={lit(rf)} AND status='pending';
      IF NOT FOUND THEN RAISE EXCEPTION 'refund terminal updated zero rows'; END IF;
      RETURN {jb(result)};
    END $fn$;
    SELECT pg_temp.observe();""",
    )


def materialize(force_zero=False, expected_epoch=8):
    """Rebuild entire current authority image atomically; exact comparisons.

    Existing rows may equal only the immutable A image or current R image.
    No generic UPSERT overwrites a conflict. Missing rows are inserted, matching
    pending rows advance through a compare-and-set. Any wrong key, association,
    input, result or event aborts all tables and cursor. Zero-row CAS aborts too.
    This is a frozen-image teaching rebuild, not an online high-volume projector.
    """
    image = value(
        "authority",
        """SELECT jsonb_build_object(
      'operations',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY p),'[]') FROM operations r),
      'charges',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY c),'[]') FROM charges r),
      'refunds',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY rf),'[]') FROM refunds r))""",
    )
    # One SQL statement reads all three relations from the same committed
    # snapshot. The manifest identifies this full image, not a consensus offset.
    ops, charges, refs = image["operations"], image["charges"], image["refunds"]
    desired = []
    byp = {q["p"]: q for q in charges}
    for op in ops:
        if op["c"] is None:
            continue  # local-v1's atomic wallet+result is authority-only in this subset.
        q = byp[op["p"]]
        abase = {
            "acceptance": op["acceptance"],
            "version": op["version"],
            "input": op["input"],
            "business_state": "awaiting_payment",
        }
        final = dict(
            abase,
            business_state=(
                "paid"
                if q["status"] == "succeeded"
                else "payment_rejected"
                if q["status"] == "rejected"
                else "awaiting_payment"
            ),
        )
        desired.append(("purchases", "id", op["p"], {"s": op["s"]}, abase, final))
        cbase = {
            "purchase_id": q["p"],
            "task_id": q["t"],
            "provider_scope": q["scope"],
            "provider_key": q["key"],
            "payment_input": q["input"],
            "status": "unconfirmed",
            "provider_result": None,
            "payment_result": None,
        }
        cfinal = dict(
            cbase,
            status=q["status"],
            provider_result=q["provider_result"],
            payment_result=q["payment_result"],
        )
        desired.append(
            (
                "payments",
                "id",
                q["c"],
                {"p": q["p"], "scope": q["scope"], "key": q["key"]},
                cbase,
                cfinal,
            )
        )
        tbase = {"charge_id": q["c"], "state": "ready"}
        tfinal = dict(tbase, state="done" if q["status"] != "unconfirmed" else "ready")
        desired.append(("tasks", "id", q["t"], {"c": q["c"]}, tbase, tfinal))
        if q["status"] == "succeeded":
            obody = {
                "purchase_id": q["p"],
                "charge_id": q["c"],
                "provider_scope": q["scope"],
                "provider_charge_id": q["provider_result"]["provider_charge_id"],
                "currency": "CNY",
                "amount_minor": q["input"]["amount_minor"],
                "fulfillment_state": "unfulfilled",
            }
            desired.append(
                (
                    "orders",
                    "id",
                    q["o"],
                    {"p": q["p"], "c": q["c"], "x": q["provider_result"]["provider_charge_id"]},
                    obody,
                    obody,
                )
            )
            desired.append(("outbox", "id", q["ev"], {"o": q["o"]}, q["event"], q["event"]))
    for r in refs:
        base = {
            k: r[k]
            for k in (
                "s",
                "source_c",
                "source_x",
                "version",
                "input",
                "amount",
                "scope",
                "key",
                "provider_input",
            )
        }
        a = dict(base, status="pending", provider_result=None, result=None)
        b = dict(base, status=r["status"], provider_result=r["provider_result"], result=r["result"])
        desired.append(("refund_projection", "id", r["rf"], {"c": r["source_c"]}, a, b))
    statements = []
    for table, pk, key, cols, abase, final in desired:
        extra = list(cols)
        immutable = " AND ".join(
            f"old.{k} IS NOT DISTINCT FROM {jb(cols[k]) if isinstance(cols[k], dict) else lit(cols[k])}"
            for k in extra
        )
        colnames = ",".join([pk] + extra + ["body"])
        values = ",".join(
            [lit(key)]
            + [jb(cols[k]) if isinstance(cols[k], dict) else lit(cols[k]) for k in extra]
            + [jb(final)]
        )
        predicate = "false" if force_zero and table == "payments" else f"body={jb(abase)}"
        statements.append(f"""
          SELECT * INTO old_{table} FROM {table} WHERE {pk}={lit(key)} FOR UPDATE;
          IF FOUND THEN
            IF NOT ({immutable.replace("old.", f"old_{table}.")}) OR
              old_{table}.body NOT IN ({jb(abase)},{jb(final)}) THEN
              RAISE EXCEPTION 'projection conflict: {table} {key}'; END IF;
            IF old_{table}.body<>{jb(final)} THEN
              UPDATE {table} SET body={jb(final)} WHERE {pk}={lit(key)} AND {predicate};
              IF NOT FOUND THEN RAISE EXCEPTION 'projection updated zero rows: {table} {key}'; END IF;
            END IF;
          ELSE INSERT INTO {table}({colnames}) VALUES({values});
          END IF;""")
    decl = " ".join(
        f"old_{t} {t};"
        for t in ("purchases", "payments", "tasks", "orders", "outbox", "refund_projection")
    )
    manifest = {
        "operations": len(ops),
        "charges": len(charges),
        "refunds": len(refs),
        "image": "chapter15-frozen-authority-image-v1",
        "snapshot_sha256": hashlib.sha256(
            json.dumps(image, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    sql(
        "local",
        f"""
      BEGIN;
      DO $fn$ DECLARE {decl} BEGIN
        PERFORM id FROM gate WHERE id=1 FOR UPDATE;
        IF NOT FOUND THEN RAISE EXCEPTION 'missing projection gate'; END IF;
        IF (SELECT epoch FROM gate WHERE id=1)<>{int(expected_epoch)} THEN
          RAISE EXCEPTION 'stale projection epoch'; END IF;
        {"".join(statements)}
        INSERT INTO projection_cursor VALUES(1,{jb(manifest)})
          ON CONFLICT(id) DO UPDATE SET body=EXCLUDED.body;
      END $fn$;
      COMMIT;""",
    )
    return manifest


def consume(event):
    """Q jointly commits I, B, actual F501 and stable dispatch intention."""
    if (
        set(event) != {"event_id", "event_type", "event_version", "occurred_at", "payload"}
        or event["event_type"] != "purchase.paid"
        or event["event_version"] != 1
        or set(event["payload"])
        != {
            "purchase_id",
            "charge_id",
            "order_id",
            "provider_charge_id",
            "currency",
            "amount_minor",
        }
    ):
        raise ProtocolError("invalid event envelope")
    payload = event["payload"]
    order = payload["order_id"]
    return value(
        "consumer",
        f"""
    CREATE OR REPLACE FUNCTION pg_temp.consume() RETURNS jsonb LANGUAGE plpgsql AS $fn$
    DECLARE old inbox; f fulfillments; guard text; receipt jsonb;
    BEGIN
      PERFORM pg_advisory_xact_lock(15001);
      SELECT * INTO old FROM inbox WHERE event_id={lit(event["event_id"])};
      IF FOUND THEN
        IF old.event<>{jb(event)} THEN RAISE EXCEPTION 'same event id different payload'; END IF;
        RETURN old.receipt;
      END IF;
      guard:='initial:'||{lit(order)};
      SELECT ff.* INTO f FROM business_guard b JOIN fulfillments ff ON b.f=ff.id WHERE b.b=guard;
      IF FOUND THEN
        IF f.body<>{jb(payload)} THEN RAISE EXCEPTION 'business guard ownership conflict'; END IF;
      ELSE
        INSERT INTO fulfillments(order_id,body) VALUES({lit(order)},{jb(payload)}) RETURNING * INTO f;
        INSERT INTO business_guard VALUES(guard,f.id);
        INSERT INTO dispatch VALUES('dispatch:initial:'||{lit(order)},f.id,
          jsonb_build_object('fulfillment_id',f.id,'order_id',{lit(order)},'kind','initial'),NULL);
      END IF;
      receipt:=jsonb_build_object('event_id',{lit(event["event_id"])},'order_id',{lit(order)},
        'fulfillment_id',f.id,'status','fulfillment_accepted');
      INSERT INTO inbox VALUES({lit(event["event_id"])},{jb(event)},f.id,receipt);
      RETURN receipt;
    END $fn$;
    SELECT pg_temp.consume();""",
    )


def dispatch():
    sql(
        "consumer",
        """UPDATE dispatch SET result=jsonb_build_object('status','accepted',
        'dispatch_key',key,'fulfillment_id',f,'provider_dispatch_id','DSP-'||f)
        WHERE result IS NULL""",
    )
    return value("consumer", "SELECT coalesce(jsonb_agg(result ORDER BY key),'[]') FROM dispatch")


def recover_once(owner="scanner"):
    """A resumable scan of durable responsibility, not a list of known demo IDs.

    The fixture protects keys without expiry. A real adapter must apply its
    window/Seal rules before a missing query permits execute(). Scans always
    recover the saved C/RF and input. Direct dispatch remains a controlled fixture.
    """
    work = value(
        "authority",
        """SELECT coalesce(jsonb_agg(c ORDER BY c),'[]') FROM workflow
         WHERE status IN ('ready','running','check')""",
    )
    for c in work:
        lease = claim(c, owner)
        q = charge(c)
        observed = value(
            "platform",
            f"""SELECT coalesce(
          (SELECT coalesce(result,jsonb_build_object('status','pending','operation_id','H:'||key))
           FROM platform_operations WHERE scope={lit(q["scope"])} AND key={lit(q["key"])}),'null'::jsonb)""",
        )
        if observed is None:
            observed = execute(c)
        observe_charge(observed, c, lease["epoch"], owner)
    refunds = value(
        "authority",
        "SELECT coalesce(jsonb_agg(rf ORDER BY rf),'[]') FROM refunds WHERE status='pending'",
    )
    for rf in refunds:
        r = refund_row(rf)
        observed = value(
            "platform",
            f"""SELECT coalesce(
          (SELECT coalesce(result,jsonb_build_object('status','pending','operation_id','H:'||key))
           FROM platform_operations WHERE scope={lit(r["scope"])} AND key={lit(r["key"])}),'null'::jsonb)""",
        )
        if observed is None:
            observed = execute_refund(rf)
        observe_refund(rf, observed)
    image = materialize()
    events = value("local", "SELECT coalesce(jsonb_agg(body ORDER BY id),'[]') FROM outbox")
    receipts = [consume(event) for event in events]
    dispatches = dispatch()
    return {
        "scanned_charges": len(work),
        "scanned_refunds": len(refunds),
        "image": image,
        "receipts": receipts,
        "dispatch": dispatches,
    }
