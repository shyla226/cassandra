## Unified compaction strategy

This is a new compaction strategy that unifies tiered and leveled compaction strategies by
tuning read and write amplification, either dynamically or statically.

This is done by introducing levels that are based on the logarithm of the sstable size, with
the fanout factor **F** as the base of the logarithm, and with each level triggering a compaction as soon as it has
**T** sstables. The choice of the parameters **F** and **T**, and of a minimum sstable size, determines the behaviour
of the strategy. This should allow users to choose something equivalent to LCS, or to STCS, or to anything in between
and also beyond in terms or read and write amplification.

Further, because levels can choose different values of F and T, levels can behave differently. For example level
zero could behave like STCS but higher levels could behave more and more like LCS.

Ideally, the adaptive algorithm will make these choices transparent to the users. However, should the adaptive
algorithm not behave adequately, users will have the options of exploring the configuration of levels manually.

This strategy also introduces compaction shards. Data is partitioned in independent shards that can be compacted
in parallel. Shards will be defined by token ranges by default. For time series workloads, shards will instead
be defined by time window, similarly to TWCS (please note the time window shards are not yet implemented).

TWCS shards for old time windows will use major compactions and only the most recent time window will use size based
levels. Token range shards will instead use size based levels.

## Adaptive vs static operation

When operating in adaptive mode, the strategy will choose the configuration with the lowest theoretical read and
write costs for a given workload. This is the default configuration. In this configuration, operators will only need
to configure the target dataset size, for example 1 TB, for which the strategy should optimize.

The strategy will choose the optimal value for F and T and it will apply it to all levels.

In order to determine these two parameters, we constrain them by introducing a third parameter W, which allows
exploring choices in a one dimensional space. The parameter W, called the scaling factor, determines the values
of F and T as follows:

* If **W < 0** then **F = 2 - W** and **T = 2**. This means leveled compactions, high WA but low RA.
* If **W > 0** then set **F = 2 + W** and **T = F**. This means tiered compactions, low WA but high RA.
* If **W = 0** then **F = T = 2**. This is the middle ground, leveled and tiered compactions behave identically.

When W is negative, the larger it is, the higher the WA. When W is positive, the larger it is, the lower the WA.

To understand why this is the case, please refer to the section below describing size based levels further.

When operating in static mode, the configuration must be specified statically via the compaction options. In
this case, the user needs to set W manually in the compaction options. Different levels can have different
values of W if needed.

## Sharding

The user will specify the number of shards in the compaction options. Because we already have the target local data
size, this should be sufficient to determine the size of each shard.

For token range shards, the local vnode ranges will be split into the desired number of shards.

When flushing or when compacting, output sstables will be split along the boundaries of compaction shards as long as
they are at least as large as the minimum sstable size. If sstables are smaller than this size, then they are split
at the next boundary. The aim is to avoid sstables that are excessively small. For example, if there are four shards
and if the flush size is twice the minimum sstable size, then assuming uniform data distribution (no hot partitions),
flushing will create 2 sstables. The first sstable will be in the first shard and the second sstable will likely be
in the third shard.

Time window shards are not yet implemented but they will probably behave identically to TWCS time windows (this may
require additional configuration options to let users specify time windows).

## Size based levels

Let's explore more closely how sstables are grouped into levels within each shard.

Given:

- the fanout factor **F**
- the minimum sstable size **m**
- the survival factor **o** (which can currently be ignored as it is fixed to **1**)

then the level **L** for an sstable of size **size** is calculated as follows:

![alt text](unified_compaction_img.png)

This means that sstables are assigned to levels as follows:

|Level|Min sstable size|Max sstable size|
|---|---|---|
|0|0|**m&#x2219;(o&#x2219;F)**|
|1|**m&#x2219;(o&#x2219;F)**|**m&#x2219;(o&#x2219;F)<sup>2</sup>**|
|2|**m&#x2219;(o&#x2219;F)<sup>2</sup>**|**m&#x2219;(o&#x2219;F)<sup>3</sup>**|
|3|**m&#x2219;(o&#x2219;F)<sup>3</sup>**|**m&#x2219;(o&#x2219;F)<sup>4</sup>**|
|...|...|...|
|N|**m&#x2219;(o&#x2219;F)<sup>n</sup>**|**m&#x2219;(o&#x2219;F)<sup>n+1</sup>**|

If we define **T** as the number of sstables in a level that triggers a compaction, then:

* **T = 2** means the strategy is using a leveled merged policy. An sstable enters level n with size **>=m(oF)<sup>n</sup>**.
  When another sstable enters (also with size **>=m(oF)<sup>n</sup>**) they compact and form a new table with size
  **~2m(oF)<sup>n</sup>**, which keeps the result in the same level for **F > 2**. After this repeats at least **F-2**
  more times (i.e. F tables enter the level altogether), the compaction result grows to **>= m(oF)<sup>n+1</sup>**
  and enters the next level.
* **T >= F** means the strategy is using a tiered merge policy. Typically in the literature one simply chooses compactions by
  selecting **F** sstables in the same level and therefore **T = F**. Therefore, after one compaction, the output sstable
  enters the next level. There is the possibility that the result could be smaller than **m(oF)<sup>n+1</sup>** due
  to overwrites, which is why we need the factor **o**, which should become smaller as the overwrites increase
  in order to prevent the result remaining in the same level.

For leveled strategies, the write amplification will be proportional to **F** times the number of levels whilst
for tiered strategies it will be proportional only to the number of levels. On the other hand, the read
amplification will be proportional to the number of levels for leveled strategies and to **T** times the number
of levels for tiered strategies.

The number of levels for our size based scheme can be calculated by substituting the maximal dataset size **D** in our
equation above, giving a maximal number of levels proportional to the inverse of the natural logarithm of **F**.

Therefore when we try to control the overheads of compaction on the database, we have a space of choices for the strategy
that range from:

* leveled compaction (**T=2**) with high **F** - low number of levels, high read efficiency, high write cost,
  moving closer to the behaviour of a sorted array as **F** increases;
* compaction with **T = F = 2** where leveled is the same as tiered and we have a middle ground with logarithmically
  increasing read and write costs;
* tiered compaction (**T=F**) with high **F** - very high number of sstables, low read efficiency and low write cost,
  moving closer to an unsorted log as **F** increases.

## Adaptive algorithm

In order to determine the optimal **W** depending on the current workload, the read and write amplifications
for several values of **W** are calculated periodically.

The read amplification is calculated by multiplying the maximum number of levels for the target data size, times
1 for leveled compaction (**W < 0**) or times **T - 1** for tiered compaction (**W >= 0**).

The write amplification is calculated by multiplying the maximum number of levels for the target data size, times
1 for tiered compaction (**W >= 0**) or times **F - 1** for leveled compaction (**W < 0**).

Note that the target data size is the size of a shard, so we divide the dataset size by the number of shards specified
by the user.

To calculate the cost of a compaction choice, we use the theoretical read and write amplifications just defined.
For each choice of W we calculate the cost of read queries and of inserts. We sum the two costs and pick the
choice with the minimum cost, provided that
* it is better than the current choice by a gain factor, currently 15%.
* the cost of the current choice is above an arbitrary minimum value. The minimum value is unfortunately necessary
  because if both reads and inserts converge to zero then the algorithm is no longer able to calculate any costs.

The cost of read queries is calculated by multiplying the number of partitions read per second times the latency for
reading a partition per sstable. This latency includes everything, not just disk access. We measure the total latency
of single partition commands and divide by the number of sstables that were accessed. Therefore the cache miss rate
should already have been taken into account. This value is then multiplied by
**Math.min(1 + bloom_filter_false_positive_ratio * RA / o, RA)**.
So we assume that we'll read at least one sstable, the one with the data, possibly more because of bloom filter false
positives or because of data that has survived due to the lack of compactions (via the survival factor, even though
this is currently fixed to one).

The cost of inserts is calculated by adding the flush and compaction costs. The flush cost is calculated by multiplying
the rate of bytes inserted (kb / sec) times the flush latency, which is an average of the time it takes to flush 1kb
of data. The compaction cost is calculated by multiplying the rate of bytes inserted (kb / sec)
times the compaction latency and the write amplification. The compaction latency is an average of the time
that it takes to compact 1kb of data. The write amplification is the number of compactions that the current strategy
choice will perform. The rate of bytes inserted acts as a weight to determine how significant the flushing and
compaction costs will be in the near future.

The partitions read or bytes inserted per second are exponential moving averages over a period of one minute.

The costs for each possible value of W is calculated periodically, by default every 2 minutes. If there exists a W
with a lower cost for reads and inserts, then W is updated and the strategy will alter its behavior.

## Configuration

These parameters are common to both static and adaptive behavior.

* dataset_size_in_gb. This is the target dataset size, by default 1 TB. This is used to calculate the number of levels
  and therefore the theoretical read and write amplification. It doesn't need to be very accurate but it is recommended
  that it should be set to a a value that is close to the target local dataset size, within a few GBs.

* num_shards. This is the number of shards, by default 5. It is recommended that users set this value. More shards
  means more parallelism and smaller sstables at the higher levels but it also means a slightly higher write amplification
  if sstable segments end up in level 0 of the next shard. Also, shards need to be large enough for size based levels to
  be effective. Shards should probably be between 50 and 250 GB.

* min_sstable_size_in_mb. This is the minimum sstable size in MB, by default 50. If set to zero then the flush size will
  be used, rounded up to the next 50 MB. The flush size is a moving average of the measured flush sizes. Smaller values
  of this parameter mean that even when flushing we have sstables generated in different shards. If this value is bigger
  or equal to the flush size, then all flushed sstables will likely be in the first shard, and then will end up in
  successive shards as compactions splits larger and larger sstables across shards. This not only means a slightly higher
  write amplification but also that the first shard may become a bottleneck.

* adaptive. This parameter determines if the strategy is using the adaptive or the static behavior, by default it's true
  so the strategy will adjust its configuration according to the read and write workload.

### Adaptive Configuration

* adaptive_starting_w. This is the starting value for W, by default 0. If we import sstables or stream sstables before
  the node starts accepting requests, we may need to set this to something higher, for example 32. This is required in
  order to reduce write amplification for compacting the initial sstables that are imported or streamed. The adaptive
  algorithm needs a write based workload in order to choose higher values for W automatically.

* adaptive_min_w. This is the minimum value of W, by default -10.

* adaptive_max_w. This is the maximum value of W, by default 36.

* adaptive_interval_sec. This is how often the adaptive algorithm validates its choice. By default 120 seconds.

* adaptive_gain. This is a percentage that indicate by how much a different choice of W should reduce the overall cost
  of the new compaction choice. By default it is 0.15. Values too low may make the algorithm unstable.

* adaptive_min_cost. This is the minimum cost of a compaction choice. Below this cost, the algorithm does not attempt
  to improve its choice. By default it is 1000. Given that costs depend on latencies, and latencies are expressed in
  milliseconds, we can think of this value as a latency expressed in milliseconds.

### Static Configuration

* static_scaling_factors. This is a list of integers separated by a comma. The first value will be used to set the value of
  **W** for the first level, the second for the second level, and so on. The last value in this list will also be used for
  all remaining levels. So a single value in this list means that all levels will use the same value. By default it's
  "**2, -8**", which should be equivalent to using STCS with a minimum threshold of 4 on the first level and LCS with a
  fanout factor of 10 at the second level and higher. Attention though, STCS can compact up to 32 sstables together if
  it falls behind, whereas this config will always compact 4 sstables at a time. The new strategy does not skip levels.
  Perhaps this value should be "**30, -8**" by default, which would allow up to 32 sstables in level 0 and then switch to
  LCS-like behaviour.

