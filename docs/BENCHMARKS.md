# Benchmark (scale=medium, reps=3)

```
scenario                 method                 rows  wire MB  mem MB  median ms   best ms     MB/s
---------------------------------------------------------------------------------------------------
om_bulk_one_chain        flight               87,360        -    12.7       23.4      23.3      541
om_bulk_one_chain        http_arrow_zstd      87,360      5.7    12.7       28.5      28.0      444
om_bulk_one_chain        http_arrow_none      87,360     12.7    12.7       22.9      22.6      552
om_bulk_one_chain        proto_columnar       87,360     12.0       -      506.2     505.4        -
om_bulk_one_chain        proto_row            87,360     17.1       -      980.9     980.9        -
om_bulk_one_chain        rest_json            87,360     32.8       -     6735.3    6735.3        -
om_filtered_pushdown     flight               29,348        -     1.2       13.5      13.5       89
om_filtered_pushdown     http_arrow_zstd      29,348      0.5     1.2       12.0      11.5      101
om_filtered_pushdown     http_arrow_none      29,348      1.2     1.2       11.4      11.4      105
om_filtered_pushdown     proto_columnar       29,348      1.4       -       67.8      66.3        -
om_filtered_pushdown     proto_row            29,348      1.9       -      117.4     117.4        -
om_filtered_pushdown     rest_json            29,348      3.5       -     2049.9    2049.9        -
om_all_underlyings_year  flight            1,747,200        -    70.4       66.9      65.5     1052
om_all_underlyings_year  http_arrow_zstd   1,747,200     24.8    70.4      114.9     114.5      613
om_all_underlyings_year  http_arrow_none   1,747,200     70.4    70.4       71.5      71.4      985
om_all_underlyings_year  proto_columnar    1,747,200     66.9       -     3741.2    3734.3        -
om_all_underlyings_year  proto_row         1,747,200     96.6       -     6475.6    6475.6        -
om_all_underlyings_year  rest_json         1,747,200    201.8       -   117128.9  117128.9        -
georev_full_year         flight              320,000        -    38.3       42.1      41.2      911
georev_full_year         http_arrow_zstd     320,000     10.5    38.3       58.2      56.4      658
georev_full_year         http_arrow_none     320,000     38.3    38.3       47.0      46.4      815
georev_full_year         proto_columnar      320,000     34.1       -     1545.9    1537.2        -
georev_full_year         proto_row           320,000     46.9       -     2781.9    2781.9        -
georev_full_year         rest_json           320,000    112.3       -    23324.7   23324.7        -
georev_projection        flight              320,000        -    14.1       20.3      20.3      695
georev_projection        http_arrow_zstd     320,000      5.1    14.1       23.4      23.3      603
georev_projection        http_arrow_none     320,000     14.1    14.1       20.7      20.7      681
georev_projection        proto_columnar      320,000     12.2       -      469.4     464.6        -
georev_projection        proto_row           320,000     15.7       -      850.9     850.9        -
georev_projection        rest_json           320,000     31.8       -    21080.8   21080.8        -
```
