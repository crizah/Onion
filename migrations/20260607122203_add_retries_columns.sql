-- Modify "tasks" table
ALTER TABLE "public"."tasks" ADD COLUMN "retry_attempt" integer NULL DEFAULT 0, ADD COLUMN "retried_at" timestamptz NULL;
