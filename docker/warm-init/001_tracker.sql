CREATE TABLE IF NOT EXISTS public.flipbench_environment_guard (
    role text PRIMARY KEY CHECK (role = 'warm'),
    marker uuid NOT NULL
);

INSERT INTO public.flipbench_environment_guard (role, marker)
VALUES ('warm', 'ca093fd2-6f08-44d1-a114-3494b55ca6bf')
ON CONFLICT (role) DO NOTHING;

CREATE TABLE IF NOT EXISTS public.partition_tracker (
    cell text NOT NULL,
    timeslot text NOT NULL,
    state text NOT NULL CHECK (state IN ('hot_primary', 'locked', 'drained', 'warm_primary', 'recovering')),
    attempt_epoch bigint,
    version bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (state = 'hot_primary' AND attempt_epoch IS NULL)
        OR (state <> 'hot_primary' AND attempt_epoch IS NOT NULL)
    ),
    PRIMARY KEY (cell, timeslot)
);

CREATE TABLE IF NOT EXISTS public.flip_attempts (
    attempt_epoch bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    attempt_id uuid NOT NULL UNIQUE,
    cell text NOT NULL,
    timeslot text NOT NULL,
    state text NOT NULL CHECK (state IN ('locked', 'drained', 'warm_primary', 'recovering', 'reverted')),
    table_count integer NOT NULL CHECK (table_count IN (5, 10, 15, 20)),
    hot_system_identifier text,
    hot_database text,
    slot_name text NOT NULL,
    publication_name text NOT NULL,
    fence_lsn pg_lsn,
    manifest jsonb NOT NULL,
    manifest_sha256 text NOT NULL,
    connector_config_sha256 text NOT NULL,
    stage_timestamps jsonb NOT NULL DEFAULT '{}'::jsonb,
    error jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE public.partition_tracker
ADD CONSTRAINT partition_tracker_attempt_epoch_fk
FOREIGN KEY (attempt_epoch) REFERENCES public.flip_attempts(attempt_epoch);

CREATE UNIQUE INDEX IF NOT EXISTS one_nonterminal_flip_per_timeslot
ON public.flip_attempts (cell, timeslot)
WHERE state IN ('locked', 'drained', 'recovering');

CREATE TABLE IF NOT EXISTS public.flip_table_states (
    attempt_epoch bigint NOT NULL REFERENCES public.flip_attempts(attempt_epoch),
    parent_name text NOT NULL,
    leaf_name text NOT NULL,
    state text NOT NULL CHECK (state IN ('attached', 'detach_started', 'pending_finalize', 'detached', 'reattached')),
    detach_started_at timestamptz,
    detach_finished_at timestamptz,
    detach_duration_ns bigint,
    PRIMARY KEY (attempt_epoch, parent_name)
);

CREATE TABLE IF NOT EXISTS public.flip_offsets (
    attempt_epoch bigint NOT NULL REFERENCES public.flip_attempts(attempt_epoch),
    consumer_group text NOT NULL,
    topic text NOT NULL,
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    target_next_offset bigint NOT NULL CHECK (target_next_offset >= 0),
    final_committed_next_offset bigint CHECK (final_committed_next_offset >= 0),
    PRIMARY KEY (attempt_epoch, consumer_group, topic, partition_id)
);
