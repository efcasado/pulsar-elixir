# Measurements

Apple M1 Pro, 10 available cores, 32 GiB RAM; macOS, OTP 29.0, Elixir 1.20.0.
Baseline: #225 at `78ce07d51e653f4a358a9248641914b4f11d9c3a`.
The library delta is the iodata prototype; the harness is identical on both revisions.

Each run uses 0.5 s warmup, 2 s timing, 0.2 s memory and 0.2 s reduction measurement
per scenario, with one benchmark process. Runs execute sequentially in baseline,
prototype, prototype, baseline order. Values below are the median of the two runs'
reported medians, throughputs and allocations, respectively. Negative latency change
means faster. Parentheses show the two runs' range of medians, not a confidence
interval. Raw JSON includes p95/p99, sample counts and reductions for each run.

These are serial loopback measurements on a development machine with other Docker
services running. They do not establish production throughput or tail-latency guarantees.
See [methodology](../../iodata.md) for the scope and memory-measurement limitations.

| Transport/codec/input | Baseline µs | Iodata µs | Latency change | Entries/s, baseline → iodata | Producer KiB allocated, baseline → iodata |
| --- | ---: | ---: | ---: | ---: | ---: |
| sink/none/entropy/100x470 | 195.65 (187.42–203.88) | 139.56 (138.08–141.04) | -28.7% | 4901 → 6998 | 149.02 → 151.55 |
| sink/none/entropy/1x1048576 | 141.35 (132.83–149.88) | 53.17 (53.00–53.33) | -62.4% | 6902 → 17688 | 4.34 → 4.41 |
| sink/none/entropy/1x470 | 9.48 (9.42–9.54) | 9.23 (9.08–9.38) | -2.6% | 98427 → 105859 | 4.30 → 4.37 |
| sink/none/json/100x470 | 144.38 (143.79–144.96) | 160.88 (139.58–182.17) | +11.4% | 6636 → 6144 | 149.02 → 151.55 |
| sink/none/json/1x1048576 | 142.25 (134.04–150.46) | 52.40 (51.71–53.08) | -63.2% | 6762 → 18015 | 4.34 → 4.41 |
| sink/none/json/1x470 | 9.50 (9.42–9.58) | 9.25 (9.12–9.38) | -2.6% | 102286 → 102811 | 4.30 → 4.37 |
| sink/zstd/entropy/100x470 | 243.88 (240.17–247.58) | 261.50 (235.12–287.88) | +7.2% | 3963 → 3753 | 150.05 → 151.90 |
| sink/zstd/entropy/1x1048576 | 424.80 (395.48–454.12) | 265.45 (239.52–291.38) | -37.5% | 2232 → 3711 | 6.31 → 6.28 |
| sink/zstd/entropy/1x470 | 12.04 (11.96–12.12) | 11.83 (11.67–12.00) | -1.7% | 79785 → 81266 | 4.68 → 4.91 |
| sink/zstd/json/100x470 | 202.88 (200.62–205.12) | 201.25 (201.08–201.42) | -0.8% | 4818 → 4848 | 150.05 → 151.90 |
| sink/zstd/json/1x1048576 | 1638.09 (1626.04–1650.15) | 1652.98 (1639.50–1666.46) | +0.9% | 608 → 594 | 5.39 → 5.41 |
| sink/zstd/json/1x470 | 15.23 (15.21–15.25) | 14.85 (14.71–15.00) | -2.5% | 62805 → 65018 | 4.68 → 4.91 |
| tcp/none/entropy/100x470 | 329.15 (318.75–339.54) | 324.10 (306.79–341.42) | -1.5% | 2939 → 3014 | 149.02 → 151.55 |
| tcp/none/entropy/1x1048576 | 360.83 (347.33–374.33) | 274.13 (264.96–283.29) | -24.0% | 2550 → 3343 | 4.34 → 4.41 |
| tcp/none/entropy/1x470 | 63.19 (62.71–63.67) | 64.54 (64.50–64.58) | +2.1% | 14659 → 14236 | 4.30 → 4.37 |
| tcp/none/json/100x470 | 343.06 (336.29–349.83) | 336.39 (306.12–366.65) | -1.9% | 2817 → 2907 | 149.02 → 151.55 |
| tcp/none/json/1x1048576 | 402.31 (379.88–424.75) | 268.97 (266.60–271.33) | -33.1% | 2148 → 3261 | 4.34 → 4.41 |
| tcp/none/json/1x470 | 61.06 (60.67–61.46) | 58.60 (58.21–59.00) | -4.0% | 15022 → 16379 | 4.30 → 4.37 |
| tcp/zstd/entropy/100x470 | 487.27 (460.46–514.08) | 436.49 (404.27–468.71) | -10.4% | 1840 → 2243 | 150.05 → 151.90 |
| tcp/zstd/entropy/1x1048576 | 980.30 (958.02–1002.58) | 815.26 (813.77–816.75) | -16.8% | 1003 → 1273 | 6.31 → 6.28 |
| tcp/zstd/entropy/1x470 | 65.90 (64.71–67.08) | 64.21 (61.42–67.00) | -2.6% | 13969 → 14846 | 4.68 → 4.91 |
| tcp/zstd/json/100x470 | 422.48 (397.88–447.08) | 395.94 (363.12–428.75) | -6.3% | 2308 → 2477 | 150.05 → 151.90 |
| tcp/zstd/json/1x1048576 | 1996.71 (1988.54–2004.87) | 1855.23 (1842.17–1868.29) | -7.1% | 499 → 531 | 5.39 → 5.41 |
| tcp/zstd/json/1x470 | 67.90 (67.33–68.46) | 67.83 (66.21–69.46) | -0.1% | 13770 → 13989 | 4.68 → 4.91 |
| tls/none/entropy/100x470 | 338.12 (334.88–341.38) | 337.17 (333.58–340.75) | -0.3% | 2944 → 2921 | 149.02 → 151.55 |
| tls/none/entropy/1x1048576 | 1759.19 (1577.65–1940.73) | 1516.44 (1510.79–1522.08) | -13.8% | 562 → 654 | 4.34 → 4.41 |
| tls/none/entropy/1x470 | 69.10 (68.58–69.62) | 68.67 (67.21–70.12) | -0.6% | 13709 → 13898 | 4.30 → 4.37 |
| tls/none/json/100x470 | 333.60 (328.71–338.50) | 322.88 (322.88–322.88) | -3.2% | 2889 → 3069 | 149.02 → 151.55 |
| tls/none/json/1x1048576 | 1573.32 (1567.58–1579.06) | 1518.69 (1513.62–1523.75) | -3.5% | 626 → 650 | 4.34 → 4.41 |
| tls/none/json/1x470 | 68.75 (68.25–69.25) | 67.62 (67.21–68.04) | -1.6% | 13409 → 14227 | 4.30 → 4.37 |
| tls/zstd/entropy/100x470 | 469.00 (450.08–487.92) | 486.68 (459.69–513.67) | +3.8% | 2066 → 2025 | 150.05 → 151.90 |
| tls/zstd/entropy/1x1048576 | 1990.65 (1944.33–2036.96) | 1888.65 (1814.96–1962.33) | -5.1% | 496 → 511 | 6.31 → 6.28 |
| tls/zstd/entropy/1x470 | 74.88 (72.71–77.04) | 73.67 (71.54–75.79) | -1.6% | 12417 → 12913 | 4.68 → 4.91 |
| tls/zstd/json/100x470 | 420.30 (372.94–467.67) | 364.27 (363.83–364.71) | -13.3% | 2319 → 2622 | 150.05 → 151.90 |
| tls/zstd/json/1x1048576 | 2076.68 (2052.81–2100.54) | 2095.80 (2037.09–2154.52) | +0.9% | 477 → 471 | 5.39 → 5.41 |
| tls/zstd/json/1x470 | 76.48 (75.29–77.67) | 74.15 (73.71–74.58) | -3.1% | 12192 → 13121 | 4.68 → 4.91 |
