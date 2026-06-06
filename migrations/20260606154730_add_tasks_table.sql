-- Create "tasks" table
CREATE TABLE "public"."tasks" (
  "id" uuid NOT NULL,
  "name" text NOT NULL,
  "status" text NOT NULL,
  "queue" text NOT NULL,
  "args" jsonb NULL,
  "output" jsonb NULL,
  "config" jsonb NULL,
  "created_at" timestamptz NULL,
  "started_at" timestamptz NULL,
  "completed_at" timestamptz NULL,
  "duration_ms" bigint NULL DEFAULT 0,
  PRIMARY KEY ("id")
);
