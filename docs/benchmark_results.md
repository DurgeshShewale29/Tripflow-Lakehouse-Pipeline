# Silver layout benchmark

## Files

| copy | files | size_bytes |
| --- | ---: | ---: |
| baseline | 200 | 296130558 |
| optimized | 3 | 249346465 |

## Queries

Q3 uses the optimized copy for both timings. baseline_s is the join with no broadcast hint; optimized_s is broadcast(zones).

| query | baseline_s | optimized_s | speedup_x |
| --- | ---: | ---: | ---: |
| Q1 | 2.606 | 0.839 | 3.11 |
| Q2 | 1.910 | 0.842 | 2.27 |
| Q3 | 6.200 | 2.428 | 2.55 |

Total baseline: 10.716 s

Total optimized: 4.109 s
