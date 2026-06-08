-- Create index "idx_tasks_created_at" to table: "tasks"
CREATE INDEX "idx_tasks_created_at" ON "public"."tasks" ("created_at" DESC);
-- Create index "idx_tasks_name" to table: "tasks"
CREATE INDEX "idx_tasks_name" ON "public"."tasks" ("name");
-- Create index "idx_tasks_queue" to table: "tasks"
CREATE INDEX "idx_tasks_queue" ON "public"."tasks" ("queue");
-- Create index "idx_tasks_status" to table: "tasks"
CREATE INDEX "idx_tasks_status" ON "public"."tasks" ("status");
