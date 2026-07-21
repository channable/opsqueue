-- Uses the same formula as '20250803174028_better_random_order_formula.down.sql'.
ALTER TABLE submissions ADD COLUMN random_order INTEGER NOT NULL GENERATED ALWAYS AS (
    (((id + (id >> 22)) % 65536) * 40503) % 65536
) VIRTUAL;

-- 3. Create the index on the submissions table to support random ordering queries
CREATE INDEX random_submissions_order ON submissions (
      random_order
    , id
);
