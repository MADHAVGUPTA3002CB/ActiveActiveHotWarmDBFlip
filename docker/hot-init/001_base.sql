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
