CREATE TABLE IF NOT EXISTS public.flipbench_environment_guard (
    role text PRIMARY KEY CHECK (role = 'hot'),
    marker uuid NOT NULL
);

INSERT INTO public.flipbench_environment_guard (role, marker)
VALUES ('hot', '2bbd8f35-7fa3-4a48-91ce-1d79df58d68d')
ON CONFLICT (role) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.dbz_heartbeat (
    id integer PRIMARY KEY,
    touched_at timestamptz NOT NULL
);

INSERT INTO public.dbz_heartbeat (id, touched_at)
VALUES (1, clock_timestamp())
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.dbz_heartbeat_active (
    id integer PRIMARY KEY,
    touched_at timestamptz NOT NULL
);

INSERT INTO public.dbz_heartbeat_active (id, touched_at)
VALUES (1, clock_timestamp())
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.dbz_heartbeat_migration (
    id integer PRIMARY KEY,
    touched_at timestamptz NOT NULL
);

INSERT INTO public.dbz_heartbeat_migration (id, touched_at)
VALUES (1, clock_timestamp())
ON CONFLICT (id) DO NOTHING;

CREATE OR REPLACE FUNCTION public.reject_record_key_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'id and created_at are immutable record-key columns';
    END IF;
    RETURN NEW;
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flipbench_guard_owner') THEN
        CREATE ROLE flipbench_guard_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;
    END IF;
END;
$$;

CREATE SCHEMA IF NOT EXISTS flipbench_guard AUTHORIZATION flipbench_guard_owner;
REVOKE ALL ON SCHEMA flipbench_guard FROM PUBLIC;

CREATE TABLE IF NOT EXISTS flipbench_guard.partition_write_gates (
    cell text NOT NULL,
    timeslot text NOT NULL CHECK (timeslot IN ('active', 'retiring')),
    ownership_epoch bigint NOT NULL CHECK (ownership_epoch > 0),
    state text NOT NULL CHECK (state IN ('open', 'parked')),
    park_attempt_id uuid,
    attempt_epoch bigint,
    version bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (state = 'open' AND park_attempt_id IS NULL AND attempt_epoch IS NULL)
        OR (state = 'parked' AND park_attempt_id IS NOT NULL)
    ),
    PRIMARY KEY (cell, timeslot)
);

CREATE TABLE IF NOT EXISTS flipbench_guard.write_routes (
    cell text NOT NULL,
    timeslot text NOT NULL CHECK (timeslot IN ('active', 'retiring')),
    parent_name name NOT NULL,
    PRIMARY KEY (cell, timeslot, parent_name)
);

ALTER TABLE flipbench_guard.partition_write_gates OWNER TO flipbench_guard_owner;
ALTER TABLE flipbench_guard.write_routes OWNER TO flipbench_guard_owner;

CREATE OR REPLACE FUNCTION flipbench_guard.insert_events(
    p_cell text,
    p_timeslot text,
    p_expected_ownership_epoch bigint,
    p_parent_name name,
    p_rows jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    gate_state text;
    gate_epoch bigint;
    inserted_count integer;
BEGIN
    IF p_timeslot NOT IN ('active', 'retiring') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown hot write timeslot';
    END IF;
    IF jsonb_typeof(p_rows) <> 'array' OR jsonb_array_length(p_rows) < 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'guarded insert rows must be a non-empty JSON array';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_rows) AS item
        WHERE CASE p_timeslot
            WHEN 'retiring' THEN
                (item->>'created_at')::timestamptz < timestamptz '2026-07-31 12:00:00+00'
                OR (item->>'created_at')::timestamptz >= timestamptz '2026-08-01 00:00:00+00'
            WHEN 'active' THEN
                (item->>'created_at')::timestamptz < timestamptz '2026-08-01 00:00:00+00'
                OR (item->>'created_at')::timestamptz >= timestamptz '2026-08-01 12:00:00+00'
            ELSE true
        END
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'guarded insert row does not belong to the selected timeslot';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flipbench_guard.write_routes AS route
        WHERE route.cell = p_cell
          AND route.timeslot = p_timeslot
          AND route.parent_name = p_parent_name
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'guarded insert route is not authorized';
    END IF;

    SELECT gate.state, gate.ownership_epoch
    INTO gate_state, gate_epoch
    FROM flipbench_guard.partition_write_gates AS gate
    WHERE gate.cell = p_cell AND gate.timeslot = p_timeslot
    FOR SHARE;

    IF NOT FOUND
       OR gate_state <> 'open'
       OR gate_epoch <> p_expected_ownership_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: ownership gate is not open for the expected epoch';
    END IF;

    EXECUTE format(
        'INSERT INTO public.%I (id, experiment_run_id, sequence_no, created_at, payload) '
        'SELECT (item->>''id'')::uuid, (item->>''experiment_run_id'')::uuid, '
        '(item->>''sequence_no'')::bigint, (item->>''created_at'')::timestamptz, item->''payload'' '
        'FROM jsonb_array_elements($1) AS item',
        p_parent_name
    ) USING p_rows;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    RETURN inserted_count;
END;
$$;

ALTER FUNCTION flipbench_guard.insert_events(text, text, bigint, name, jsonb)
OWNER TO flipbench_guard_owner;
REVOKE ALL ON FUNCTION flipbench_guard.insert_events(text, text, bigint, name, jsonb) FROM PUBLIC;

DROP FUNCTION IF EXISTS flipbench_guard.insert_events_optimistic(text, text, bigint, jsonb);
DROP FUNCTION IF EXISTS flipbench_guard.insert_events_optimistic(text, text, bigint, name, jsonb);

CREATE OR REPLACE FUNCTION flipbench_guard.admit_optimistic_batch(
    p_cell text,
    p_timeslot text,
    p_expected_ownership_epoch bigint
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    gate_state text;
    gate_epoch bigint;
BEGIN
    IF p_timeslot NOT IN ('active', 'retiring') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown optimistic batch timeslot';
    END IF;
    SELECT gate.state, gate.ownership_epoch
    INTO gate_state, gate_epoch
    FROM flipbench_guard.partition_write_gates AS gate
    WHERE gate.cell = p_cell AND gate.timeslot = p_timeslot;

    IF NOT FOUND
       OR gate_state <> 'open'
       OR gate_epoch <> p_expected_ownership_epoch THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic batch admission rejected';
    END IF;
    RETURN gate_epoch;
END;
$$;

ALTER FUNCTION flipbench_guard.admit_optimistic_batch(text, text, bigint)
OWNER TO flipbench_guard_owner;
REVOKE ALL ON FUNCTION flipbench_guard.admit_optimistic_batch(text, text, bigint) FROM PUBLIC;

CREATE OR REPLACE FUNCTION flipbench_guard.insert_events_optimistic(
    p_cell text,
    p_timeslot text,
    p_parent_name name,
    p_rows jsonb
)
RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    inserted_count integer;
BEGIN
    IF p_timeslot NOT IN ('active', 'retiring') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'unknown optimistic write timeslot';
    END IF;
    IF jsonb_typeof(p_rows) <> 'array'
       OR jsonb_array_length(p_rows) < 1
       OR jsonb_array_length(p_rows) > 100000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic write batch must contain 1..100000 rows';
    END IF;
    IF pg_column_size(p_rows) > 67108864 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic write payload exceeds the 64 MiB safety limit';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM flipbench_guard.write_routes AS route
        WHERE route.cell = p_cell
          AND route.timeslot = p_timeslot
          AND route.parent_name = p_parent_name
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'optimistic write route is not authorized';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_rows) AS row_item
        WHERE CASE p_timeslot
            WHEN 'retiring' THEN
                (row_item->>'created_at')::timestamptz < timestamptz '2026-07-31 12:00:00+00'
                OR (row_item->>'created_at')::timestamptz >= timestamptz '2026-08-01 00:00:00+00'
            WHEN 'active' THEN
                (row_item->>'created_at')::timestamptz < timestamptz '2026-08-01 00:00:00+00'
                OR (row_item->>'created_at')::timestamptz >= timestamptz '2026-08-01 12:00:00+00'
            ELSE true
        END
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'optimistic write row does not belong to the selected timeslot';
    END IF;

    BEGIN
        EXECUTE format(
            'INSERT INTO public.%I (id, experiment_run_id, sequence_no, created_at, payload) '
            'SELECT (item->>''id'')::uuid, (item->>''experiment_run_id'')::uuid, '
            '(item->>''sequence_no'')::bigint, (item->>''created_at'')::timestamptz, item->''payload'' '
            'FROM jsonb_array_elements($1) AS item',
            p_parent_name
        ) USING p_rows;
        GET DIAGNOSTICS inserted_count = ROW_COUNT;
    EXCEPTION
        WHEN check_violation OR lock_not_available OR query_canceled THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'hot writer parked: optimistic detach race aborted one batch operation';
    END;
    RETURN inserted_count;
END;
$$;

ALTER FUNCTION flipbench_guard.insert_events_optimistic(text, text, name, jsonb)
OWNER TO flipbench_guard_owner;
REVOKE ALL ON FUNCTION flipbench_guard.insert_events_optimistic(text, text, name, jsonb) FROM PUBLIC;
