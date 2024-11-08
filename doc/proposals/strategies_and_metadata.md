# Strategies & Metadata

Two core principles of Opsqueue are:
1. We want to be able to support backlogs of many millions of chunks (billions of operations).
1. We want to make usage of the system as flexible as possible, as long as it doesn't conflict with the first point.

In the past we've looked at providing a few basic strategies, and then stating "if you want other strategies, you'll have to modify Opsqueue", with 'modify' being either compiling a fork of, or using some kind of to-be-designed 'plug-in mechanism'.

But there is an alterntaive: If we start from a small but exhaustive set of composable strategy building blocks,
then people can configure their strategy without having to change Opsqueue itself.

The question is however: can we make this efficient enough?

I believe that it is possible to have a reasonably-scalable system without programmers having to recompile Opsqueue when they want more flexibility in the strategies. That is, to allow (within reasonable bounds) programmers to mix and match (and compose) strategies at will without hampering opsqueue's scalability.

## Building blocks

```rust
enum Strategy {
    /// Select randomly, reasonably fairly
    Random,
    /// Prefer chunks from the newest submission first, i.e. LIFO
    NewestFirst,
    /// Prefer chunks from the oldest submission first, i.e. FIFO
    OldestFirst,
    /// Only select chunks from submissions that match the given key-value set when submission
    SelectOnly(meta_field: String, value: MetaValue, underlying: Strategy),
    /// Select chunks based on a custom priority value set when inserting each submission (high->low)
    CustomPriority,
    /// If there are more than `max` chunks whose submission has the same value for `meta_field`,
    /// prefer picking up other submissions. (But if there aren't any other, still pick up chunks from these)
    /// 
    /// This is 'soft' rate-limiting
    PreferDistinct(meta_field: String, max: PosInt, underlying: Strategy),
    /// If there are more than `max` chunks whose submission has the same value for `meta_field`,
    /// pretend they don't exist.
    /// 
    /// This is 'hard' rate-limiting
    MaxSimultaneous(meta_field: String, max: PosInt, underlying: Strategy),
    /// Try the first strategy, but if it gives no results, try the fallback strategy.
    OrElse(try_first: Strategy, fallback: Strategy)
}
```

I think above set is reasonably complete, in that it can handle any different desires I can come up with.
This particular set of strategies has been chosen to keep Opsqueue itself, especially its `reserve_chunks` query, efficient.

One 'simple' way to increase efficiency is to add more indexes and join tables. However, there is a balance to strike since every extra index results in write amplification and a larger in-memory and on-disk database.
At some point, this will make the other operations, especially insertion, too slow and the database itself painfully heavy to handle.

In the rest of this document, the strategies and their potential implementation are each described in detail, and some tricks/ideas on making the right tradeoff w.r.t. what indexes to create while keeping the database manageable are given at the end.

## Assumptions & Invariants

It is good to be explicit with the assumptions I'll be making in this document.
Most of these are straightforward, but to make sure everyone is on the same page:

- The number of submissions in the backlog might be thousands to hundreds of thousands (10³ - 10⁵).
- The number of chunks in all those submissions in the backlog might be many millions (10⁶ - 10⁹).
- (The number of operations in these chunks might again be a factor thousand or more; but the Opsqueue binary itself does not care about the contents of the chunks, as they do not pass through the queue but instead are managed separately in object_storage).
- The number of producers waiting for a submission to be completed at one time is equal to the number of submissions in the backlog (10³ - 10⁵)
- The number of connected consumers at one time is between 1 and 10_000 (10⁴).

Much of the current design of Opsqueue, and indeed also the new ideas proposed in this document depend on these assumptions. Especially the last point is important to emphasize. Opsqueue keeps a persistent connection for each connected consumer, and a little bit of data ('what chunks did this consumer register') in ephemeral memory. And specifically the `reserve_chunks` operation has a short critical section to ensure chunk reserevations are unique. As such, If someone wants or needs to scale to more than a 10_000 consumers, we should first ask:
- Is your chunk size already optimal? It is quite likely that when making the chunk size larger or smaller, they can make better usage of a smaller pool of consumers.
- If not, can you split your queue into separate unrelated operation-types?
- If not, then you might have reached the scale at which you should shard[^1] the queue. 


[^1]: Split the queue into two or more queues. With this you lose a 'full picture' of the system but you regain more scalability. There is a path forward to support sharded queues gracefully; this is for instance one of the three reasons Submission IDs are snowflake IDs (knowing which of the shards a submission ID belongs to). But that is not the topic of this document.

Furthermore, it might be important to note:
- SQLite's concurrency model: There can be many readers at a single time, but only 1 writer at a time. Readers never block other readers nor writers. Writes never block readers. Witers _do_ block other writers. If a write takes longer than a configurable (default: 5sec) timeout, waiting writers will fail.
- Searching for something when we can use an index is fast and can be done in streaming fashion, as it is a binary search in a B tree.
- Searching for something when we *don't* have an index, requires a full table scan. Especially on the chunks backlog this is too slow.
- Joining tables on indexes is fast, it just requires a nested index join loop which means a double binary search per element. Furthermore, this can still be done in streaming fashion. Note that this does require that both indexes share the same order.
- Joining tables without indexes, or joining tables but wanting the result in a different order, is too slow, memory hungry, and cannot be done in streaming fashion: It requires building a temporary table.
- SQLite integers are 64-bit _signed_ integers. As such, the largest unsigned value they can store is 63 bit.

### Opsqueue's consumer API

Opsqueue's consumer API consists of four main producures:
- `reserve_chunks(max: PosInt, strategy: Strategy) -> Stream<Chunk>`
- `complete_chunk(SubmmissionId, ChunkIndex) -> ()`
- `retry_or_fail_chunk(SubmissiionId, ChunkIndex) -> ()`

`complete_chunk` and `retry_or_faill_chunk` take a super brief write lock on  the SQLite database to update the state of the particular chunk in its table,
either incrementing the number of retries, moving it to `chunks_failed` or `chunks_completed`. In very rare situations (once per submission), they do more work,
by moving the submission to its result table as well (and for `retry_or_fail_chunk`, moving all remaining chunks of that submissionn as well).
These two calls never block a consumer however; from the perspective of the consumer they  are fire-and-forget.

`reserve_chunks` is arguably the most important call: At this point we don't have a `(SubmissionId, ChunkIndex)` yet, so the queries we run are not 'obviously' fast.
It also is the place where multiple consumers might race/contest: We need to make sure that we don't give the same chunk to two customers at the same time.
`reserve_chunks` is implemented by a _read only_  query (that does not block any other readers or writers in SQLite) which returns a stream of results
(i.e. evaluated lazily one by one by SQLite). Then in Rust we have a concurrent hashmap that keeps track of all open reservations.
This concurrent hashmap we call the '**reserver**'. It is essentially our alternative to `SELECT FOR ... SKIP LOCKED` in Postgres, except that we keep the results reserved until 
a call to `complete_chunk`/`retry_or_fail_chunk` rather than only until the query returns.
By keeping this hasmap only in memory and not persisting it to disk, we can just forget it during shutdown and start with a clean slate of 'no reservations' on startup, which is the behaviour that we want.


Because of how this is implemented, we want the queries that are generated from the various user-provided strategies to uphold the following properties:

- They need to be executable in streaming fashion; no hash joins or other 'temporary table materialization' in the middle.
   - Again, because of the backlog size, building such a temporary table on the fly will take seconds/minutes and a significant amount of RAM.
- We need queries to return 'many' results rather than 'zero or one' result, because the output of the stream-query is filtered by the reserver afterwards.
- We really don't like telling a consumer 'there is no work to do' when actually work exists.

## The Strategy building blocks in detail

### NewestFirst / OldestFirst

These are the simplest strategies to implement.
Since submissions and chunks are stored using the `submission_id`, which is a Snowflake ID containing the insertion timestamp, as primary key,
these translate to simply using an 

```sql
SELECT submission_id, chunk_index FROM chunks ORDER BY submission_id {ASC | DESC}
```

As Falco noted, there is a risk of starvation specifically when using `NewestFirst`, when the consumer(s) cannot work faster than work is coming in (_equally fast_ is not enough).
As such we might want to leave it out from the initial version; it is of course also possible to emulate it using `CustomPriority`.


### Random

The random strategy has two goals:
1. Ensure fairness.
2. Provide extra opportunities for us to optimize chunk retrievial.

Randomness is a little bit tricky to implement efficiently: `ORDER BY RANDOM()` is an oft-repeated piece of bad advice online: it stops working once you have more than a few hundred rows in your table, as it will have to do a full-table-scan each time.

One somewhat more advanced alternative is to attempt to predict the primary key (or even: the `rowid`, the 'internal primary key' in use by SQLite) of the rows in the table and make a subselection of those based on either skipping some rows or using the modulo `%` operator.
Such a query is however difficult to get right, as its exact workings very much depend on how much data is currently in the table and what kind of values their primary keys/rowids currently have (which may contain large gaps).

But instead of attempting to introduce randomness _when reading_ we can take another ~~strategy~~ approach: Create a pseudo-random order based on `submission_id + chunk_index` when _inserting_.

We encountered a similar situation (the old random selection being O(n), and improving this by setting the order at insertion time) in the past, [when attempting to make Pablo's scheduler more fair](https://github.com/channable/imaginator/issues/1804#issuecomment-2260497346).

Doing this kind of 'hashing at insertion' is trivial to accomplish, and since SQLite supports 'expression indexes' we only need to store this extra data _once_, keeping the extra overhead reasonably low.

SQLite doesn't come with hashing functions itself, but we can either use a super-simple hashing function like the 16-bit [Fibonacci hash](https://en.wikipedia.org/wiki/Hash_function#Fibonacci_hashing)[^3], 
or cop out to Rust using SQLite's `create_function` functionality.

[^3]: A 16-bit hash is chosen to ensure that we'll never overflow the 63-bit unsigned range. Unfortunately SQLite doesn't come with 'wrapping multiplication' and on overflow ints are turned into floats. But for our purposes a 16-bit hash, i.e. uniformly distributing into `[0..65536)` is good enough (and also means less storage usage for the index!). Note that _not_ using `create_function` means that we can support external tooling easier since functions have to be added _when establishing a connection_.

```sql
SELECT submission_id, chunk_index FROM chunks ORDER_BY random_order
```

With this, consumers can pick up chunks in this random order, and it is quite fair!

However, we can do one step better: We can reduce congestion on the reserver's locks, by making sure it is very unlikely that two consumers start the random order at the same offset:

```sql
-- This query expects you to generate a random u16 `$offset` before calling it.
-- The same can be done all inside SQL using a CTE, at the sacrifice of readability.
SELECT submission_id, chunk_index FROM chunks WHERE random_order > $offset ORDER_BY random_order
UNION ALL
SELECT submission_id, chunk_index FROM chunks WHERE random_order <= $offset ORDER_BY random_order
```

When viewing the random index as 'shuffling a deck of cards', this can be seen as 'cutting the deck'.
This ensures:
- The chance of two customers attempting to pick up the same chunk concurrently is very low (1/BACKLOG_SIZE), reducing lock contention in the reserver.
- It makes the random order more non-deterministic, which is especially nice in the presence of chunks that are retried (which would otherwise 'move back on top of the deck').


Using the `Random` strategy is, besides promoting fairness, currently the most performant strategy for Opsqueue.
Therefore I think we should make it the default.

### The Metadata-based strategies: 

The more complex building blocks are based on metadata that is added at submission-insertion time.

In the past, we've considered metadata as  'some arbitrary  JSON' that a producer might add when   inserting a submission, which could be used by the consumer and possibly also for strategies.
But we can normalize this further:

1. There are two kinds of metadata. 'I want access to extra submission-specific info inside the consumer implementation' which can just be a BLOB, and 'I  want to customize the chhunk reservation strategy', hereafter called 'strategic metadata'.
2. Strategic metadata is relational in nature. Rather than an arbitrary BLOB, it is a `Map<String, Value>` for each submission, meaning we can store it as submission-key-value triples in a separate metadata table.
3. For extra efficiency, we can consider 'custom priority' yet separate from tthe other strategic metadata. This  because we probably want to make a different choice w.r.t. providing an index there than for the other kinds of metadata.
4. We don't have to support more than a single 'custom priority' field, because if people have a multi-tier priority system they can combine that into a single formula.

The strategic metadata table would look as follows:

```sql
CREATE TABLE strategic_metadata (
    submission_id INTEGER NOT NULL,
    key TEXT NOT NULL,
    value ANY NOT NULL,

    PRIMARY KEY (submission_id, key)
) STRICT, WITHOUT ROWID;

CREATE UNIQUE INDEX strategic_metadata_filter ON strategic_metadata (key, value, submission_id)
```

The extra index allows us to look up all submissions for which a particular key-value is set. This index allows us to look at the resulting submissions still in old-to-new and new-to-old fashion, by walking forwards or backwards on the index:



#### SelectOnly

In some use-cases, it makes sense to select only work adhering to a particular key-value.
One situation would be where work can be `"priority" = "high"` or `"priority" = "low"` or `"mode" = "preview"` vs `"mode" = "normal"`.

These custom selects allow people to be more fine-grained in how they write a consumer, and are especially useful when creating a consumer _worker pool_..
For example, one could envision a single queue being used but with two kinds of consumers:
- One (small but dedicated) pool _only_ doing high-prio work. This one would use the `SelectOnly` strategy.
- One (larger) pool accepting _both_ high-prio and low-prio work. This one would use one of the normal strategies, or possibly `OrElse(SelectOnly("mode", "preview", Random), SelectOnly("mode", "normal", Random))` (see below) which would mean that preview-requests are always picked up before normal requests if both exist.

The following are trivially fast:


- `SelectOnly(key, val, OldestFirst)`
- `SelectOnly(key, val, NewestFirst)`

because they can query the metadata table directly, and then join the chunks table on that.

Combining it with the randomm order however requires either a full table scan of the chunks, or adding a join table `chunks_x_metadata` which will be quite large as it will contain the product of the number of chunks times the number of metadata keys.

We could mitigate the write amplification and larger database size somewhat by storing it in a differrent SQLite 'schema' (a different database file) that we don't persist and that isn't part of the backups, and recreating it on startup. See the 'cheaper indexes' section below for details.

Once we have such index, we could use the 'cutting the deck' technique with it as well, which is really nice.

Note that it is possible nest `SelectOnly` calls, and this is still reasonably fast, as SQLite will be able to use an 'index loop join'.

#### CustomPriority

In some cases a user might want to pick a completely different order than `NewestFirst`/`OldestFirst`/`Random`.

A very simple solution for this, is to allow people to set a custom priority per Submission.
The reason to consider this as something separate from other metadata keys, is because we might want to make a different choice on whether to add an extra index or not for this type.

In earlier versions of this proposal, I thought it made sense to wrap those other orders with `CustomPriority`, but especially combining it with the random order can only be supported by creating yet another index, and even if we add an extra index so it can be used with the random order, that won´t support the 'cutting the deck' approach.

Still, it might be worth it. The consideration and approach to adding such an index is similar as for `SelectOnly`, except that by limiting users  to a single priority field we can make sure that they won't try to nest multiple priority fields and join on them, because that is impossible to make efficient.

#### PreferDistinct / MaxSimultaneous

These two are similar, in the sense that both don't immediately alter the inner strategy.
Instead, they keep track at runtime what key-value was returned for every chunk that is currently being worked on (i.e. that was reserved but not yet completed/failed),
and count the total distinct values of these.

For each key-value where these counts are larger than the given `max`, the chunks are filtered out.

In the case of `PreferDistinct`, this filtering out only happens in the query and doesn't have to happen after the query returns, because it is only a  _preference_,
so if two concurrent consumers select chunks that make a count go beyond the max by two, we don't care.

For `MaxSimultaneous` we _do_ care, so it adds an extra check after the query returns to make sure our registrations are truly fine.
So then one of the two concurrent consumers would be told 'no' and given a different chunk.

Besides this, `PreferDistinct` also returns all results that don't adhere to the preference after the normal query, i.e. it is

```sql
SELECT FROM {inner_strategy_query} INNER JOIN ON submission_id = strategic_metadata.submission_id AND strategic_metadata.key = "company_id" AND strategic_metadata.value IS NOT IN  (1, 2, 3)
UNION ALL
SELECT FROM {inner_strategy_query}
```

For `MaxSimultaneous`, only the part of the query before the `UNION ALL` would be used.

---

Robert mentioned that we might also want a 'percentage' based version of `MaxSimultaneous`, based on the number of consumers that are connected.
Such a construct can easily be added, but an important question to ask is: How should this behave when the cluster itself is scaling up or down?

I actually expect that `PreferDistinct` should be used in such a situation, and `MaxSimultaneous` is really only for the 'hard rate limit' situations where we are dealing with some external system that will be overloaded when we go over a particular limit. Those limits are not based on a percentage of our worker pool size.

### OrElse

Finally, there is the 'OrElse' strategy.
It allows you to combine two strategies one-after-the-other.

The reason to add this, is to provide a better alternative than telling consumer-implementers to switch strategies every so often.

For example, consider again:

```rust
OrElse(SelectOnly("mode", "preview", Random), SelectOnly("mode", "normal", Random))
```

compared to 'when asking for chunks, pick "preview" 50% of the time but never wait for more than XXX time and then send another reserve_request but for "normal" chunks'.

Regardless of which time duration 'XXX' is picked exactly (what would be a 'good value' for this anyway?), we create a short-polling situation with a lot of useless network traffic.

`OrElse` is the alternative. It expands to simply `{first_sttrategy} UNION ALL {fallback_strategy}`.
Note that it is fine if both strategies contain an overlap of the same chunks; the reserver will make sure that the same chunk is only picked up once.


## Cheaper Indexes

The strategies `SelectOnly` and `CustomPriority` require indexes to be efficient enough.
If we want to combine them with the `Random` strategy, we need an index (or a join table, used only as a manually-maintained cross-table index) on the _chunk_ level, meaning that it will be filled with 'chunk_count * metadata_keys_per_submission' rows.

Such an index or table will be _huge_. However, we don't have to store it permanently: We can store it in its own separate SQLite 'schema' in `/tmp` (i.e. RAM + SWAP) and recompute it from scratch on startup.

The tradeoff there is that it would make startup a bit slower: instead of Opsqueue being fully ready 'instantly', it will take a couple of seconds (or maybe 'a minute', if we're tuly dealling with a backlog of 10⁹ chunks).
Or it could already start accepting work in the meantime, but consumer reservations that specifically use `SelectOnly` or `CustomPriority` will return 'no results' until the recreation of the join table is done.

When metadata is _not_ used, those indexes/tables will be empty and therefore someone not using this functionality is also not paying for it.

##### Alternative: Don't add them

It also is possible to just _not_ create such a new join table / index for `SelectOnly`, or only do it for one of them, and instead disallow usage of `Random` inside of them (only allow `NewestFirst` and `OldestFirst`).

##### Per-key tables

One more optimization we could theoretically do, is instead of having a single `strategic_metadata` table, create a key-specific table _per key value_. 
This should be possible because the set of distinct keys that will be used inside one instance of opsqueue will be small.

Doing that would allow the SQLite query planner to make the best choicess w.r.t. what join order to use. That might make some difference in cases where someone wants to construct very complex strategies with multiple `SelectOnly`'s.
However, I recommend keeping this final optimization in mind for a possible future but not committing to it now.

# Questions:

- Is this set of building blocks a (more-or-less) complete set?
- Are the proposed Strategy implementations clear and sensible?
- Do we want to add the per-chunk indexes for `SelectOnly` and `CustomPriority`?
  - With or without the 'cheap index' idea?
  - Or do we want to restrict 'can only be used with `OldestFirst`/`NewestFirst` but not with `Random`?
- Do we want to include `MaxSimultaneous` or leave it out for now?
- Do we want a percentage-based version of `MaxSimultaneous` as well or not?
