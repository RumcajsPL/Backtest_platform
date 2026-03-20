BACKTESTING_RESULTS.md — Comprehensive Backtesting History & Status
```
1. Overview
```
This document consolidates all backtesting results for the DAX instrument across multiple timeframes.
Two independent series have been completed and are now frozen for production:

1‑minute series – 13 rolling 3‑month windows, filters: DPO + Choppiness + CCI + ADX.

15‑minute series – 4 × ~9‑month windows, filters: DPO + MACD (safe zone only; exploration zone discarded).

Adapted Policy: Do NOT cross‑compare technical results between different timeframes as each series has its own parameter ranges, filter sets, and market dynamics. All calibrations are per‑series only.
However we are going to compare them for the E2E trading potential to decide the specific setup(s) to keep.

Future work will extend to 5‑minute and 10‑minute strategy timeframes, using the same systematic methodology documented here.
```
2. 1‑Minute Series (Overnight Runs, 8–15 hours)
```
2.1 Calibration History
Calibration	YAML	Run ID	Status	auto_go	Best WFO	Notes
A	V1_02	547c3161	✅ Complete	2	0.734	generic zones, safe zone dead
B	V1_03	6fcf82b9	✅ Complete	3	0.766	focused zone confirmed
C	V1_04	63f3cc3d	✅ Complete	9	0.810	risk_perc sweet spot 0.21–0.29
D	V1_05	e7d5f678	✅ Complete	8	0.826	risk_perc 0.20–0.35, sigmoid 128
E	–	–	❌ Not run	–	–	skipped
F	V1_1min	cd67ceb0	✅ Complete	7	0.8058	risk_perc 0.20–0.30, samples=300, sigmoid 103
2.2 Final Parameter Ranges (Focused Zone)
Parameter	Type	Min	Max	Step	Notes
atr_length	int	10	24	1	5–9 underperforming
atr_multiplier	float	1.9	2.7	0.1	1.8 and >2.7 dead
rr_target	float	2.6	3.2	0.1	>3.2 dead; 2.6 floor confirmed
risk_percentile	float	0.20	0.30	0.01	sweet spot 0.21–0.29; >0.30 dead
dpo_length	int	10	30	1	–
dpo_smooth	int	1	26	1	–
dpo_threshold	float	0.10	0.25	0.01	mixed signals, range productive
choppiness_length	int	8	26	1	–
choppiness_threshold	float	54.0	65.0	0.1	near‑insensitive
cci_length	int	10	30	1	–
cci_overbought	int	51	150	2	–
cci_oversold	int	-150	-51	2	–
adx_length	int	8	25	1	–
adx_threshold	float	22.0	30.0	0.2	<22 dead
2.3 Sigmoid Scale History
Run	stdev	recommended	used	notes
547c3161 (A)	361	181	310	slight inflation
6fcf82b9 (B)	326	163	310	inflation ~1.9×
63f3cc3d (C)	257	128	163	inflation ~1.27×
e7d5f678 (D)	207	103	128	–
cd67ceb0 (F)	199	99.5	103	essentially correct
Conclusion: Sigmoid scale should be set to stdev × 0.5. For future 1‑minute runs, compute from Stage 4 net P&L stdev.

2.4 Window Structure (13 × 3‑month)
Window	Period	Regime
W01	2023-01-02 → 2023-03-31	DAX recovery – directional
W02	2023-04-03 → 2023-06-30	
W03	2023-07-03 → 2023-09-29	
W04	2023-10-02 → 2023-12-29	
W05	2024-01-02 → 2024-03-28	ECB rate cycle – choppy
W06	2024-04-01 → 2024-06-28	
W07	2024-07-01 → 2024-09-30	
W08	2024-10-01 → 2024-12-31	
W09	2025-01-02 → 2025-03-31	Range‑bound + momentum bursts
W10	2025-04-01 → 2025-06-30	Structural stress window
W11	2025-07-01 → 2025-09-30	
W12	2025-10-01 → 2025-12-31	Overlaps production slice
W13	2026-01-02 → 2026-02-28	Partial – INSUFFICIENT_TRADES expected
Note: W10 is a key diagnostic window; only candidate 9dc5db154fe1 survived positively.

2.5 Top Candidates from Calibration F
Candidate	WFO	windows_eval	frac_pos	median_ret	collapsed	ruin_prob
2a891e2cce6c	0.8058	9/13	0.667	92.35	0	0.000
65df7121489f	0.7941	8/13	0.750	30.87	0	0.000
27f9db483be6	0.7795	7/13	0.714	76.68	0	0.000
61875464b3aa	0.7731	8/13	0.750	27.01	0	0.000
a3451a370263	0.7637	7/13	0.714	27.57	0	0.000
All top candidates have ruin probability 0.000 and no spikes. They are ready for paper trading.

2.6 Frozen 1‑Minute Configuration
See full YAML in configs/backtesting/backtest_V1_1min.yaml (calibration F).
Key settings:

```yaml
random_search:
  samples_per_zone: 300
  min_significant_trades: 30
genetic:
  population_size: 80
  generations: 40
  stagnation_generations: 12
walk_forward:
  windows: (13 windows as above)
_SIGMOID_SCALE = 103  # set manually in consistency_scorer.py
max_workers: 2
```

3. 15‑Minute Series (Daytime Runs, 3–6 hours)

3.1 Calibration History
Calibration	YAML	Run ID	Status	Honest auto_go	Best honest WFO	Notes
A (v1.3)	V1_06_v1.3	6b137540	✅ Complete	0	0.747 (5 win)	phantom problem, min_trades=20
B (v2.0)	V1_06_v2.0	2d50b27e	✅ Complete	5	0.882 (3 win)	2 phantom, min_trades=15
C (v3.0)	V1_06_v3.0	1fd58c85	✅ Complete	6	0.960 (4 win)	2 phantom (2‑window), W03 unlocked
D (v4.0)	V1_06_v4.0	7bfe6300	✅ Complete	1	0.800 (4 win)	4×9‑month windows, win_rate 0.15
E (v5.0)	V1_15min	820379f6	✅ Complete	2	0.772 (3 win)	CCI exploration dead, sigmoid 370
F (v6.0)	V1_15min	beb3b42a	✅ Complete	2	0.769 (3 win)	CCI removed, DPO+MACD only – exploration dead
G	V1_15min	3990fa3c	✅ Complete	1	0.7688	exploration disabled, safe zone only, samples=300, sigmoid 410
3.2 Final Parameter Ranges (Safe Zone)
Parameter	Type	Min	Max	Step	Notes
atr_length	int	8	18	1	–
atr_multiplier	float	1.6	1.9	0.1	>2.0 dead; 1.5 floor raised after zero winners
rr_target	float	6.0	9.5	0.1	<6.0 dead
risk_percentile	float	0.85	1.10	0.01	0.83–1.10 productive
dpo_length	int	14	30	1	–
dpo_smooth	int	6	20	1	–
dpo_threshold	float	0.10	0.30	0.02	–
macd_fast	int	6	12	1	must be < macd_slow
macd_slow	int	14	30	1	must be > macd_fast
macd_signal	int	2	12	1	min=2 (signal=1 crashes)
3.3 Sigmoid Scale History
Run	stdev	recommended	used	notes
6b137540 (A)	628	314	310	essentially exact
2d50b27e (B)	566	283	310	slight inflation
1fd58c85 (C)	598	299	310	negligible inflation
7bfe6300 (D)	706	353	310	under‑scaled
820379f6 (E)	740	370	370	correct
beb3b42a (F)	820	410	410	correct
3990fa3c (G)	852	426	410	slightly under, but acceptable
Conclusion: Set _SIGMOID_SCALE = stdev × 0.5. For future runs, compute from Stage 4 net P&L stdev.

3.4 Window Structure (4 × ~9‑month)
Window	Period	Regime
W01	2023-01-02 → 2023-09-29	DAX recovery – directional
W02	2023-10-02 → 2024-06-28	ECB rate cycle – choppy (stress test)
W03	2024-07-01 → 2025-03-31	H2 2024 productive + H1 2025 dead absorbed
W04	2025-04-01 → 2026-02-28	Most recent regime, partial 2026 Q1
3.5 Top Candidate from Calibration G
Candidate	WFO	windows_eval	frac_pos	median_ret	collapsed	ruin_prob
4afa4d0b5f36	0.7688	3/4	1.000	1184.84	0	0.000
This candidate is ready for paper trading.

3.6 Frozen 15‑Minute Configuration
See full YAML in configs/backtesting/backtest_V1_15min.yaml (calibration G).
Key settings:

```yaml
random_search:
  samples_per_zone: 300
  min_significant_trades: 12
genetic:
  population_size: 70
  generations: 40
  stagnation_generations: 10
walk_forward:
  windows: (4 windows as above)
_SIGMOID_SCALE = 410  # set manually in consistency_scorer.py
max_workers: 4
```

4. 10‑Minute Series (Daytime Runs, 3–6 hours)
4.1 Calibration History
Calibration	YAML	Run ID	Status	auto_go	Best WFO	Notes
A (raw)	backtest_V1_10min.yaml	bc633082-9fcd-4030-acc1-7b975398d0f8	✅ Complete	1	0.7742	exploration zone only, 400 samples, min_trades=30, sigmoid 310 (wrong).
B (focused)	backtest_V1_10min_B.yaml	7ce7beb1-5940-443d-aef2-dd351b5fee2a	✅ Complete	0	0.9083	exploration only, min_trades=20, sigmoid 163, constraints relaxed. 10 borderline.
C (planned)	backtest_V1_10min_C.yaml	–	Planned	–	–	switch to 4×9‑month windows, sigmoid 181, further relax trades/week.
4.2 Raw Run A Findings (bc633082)
Stage 1 pass rate: 33/400 (8%). Rejections: INSUFFICIENT_TRADES (210), win_rate (107), trades_per_week (38). Average trades/week = 0.66.

WFO: 30 scored, 28 collapsed. Only three candidates had ≥2 windows evaluated.

Top candidate: 8bbed2b1eaa6 (exploration) – WFO=0.7742 (5 windows), no collapse, no spike → auto_go.

Sigmoid: stdev(net_pnl)=326.48 → recommended scale 163 (was 310). Updated for run B.

4.3 Run B Findings (7ce7beb1)
Stage 1 pass rate: 69/400 (17%). Rejections: INSUFFICIENT_TRADES (188), trades_per_week (80), win_rate (40). Average trades/week = 0.68.

WFO: 30 scored, 25 collapsed. Five non‑collapsed candidates with 1–7 windows evaluated.

Top candidate: 7012af148a04 (exploration) – WFO=0.9083 (1 window), no collapse, spike in atr_mult/dpo_len/ma_slope → borderline.

No auto_go in run B; all 10 final verdicts borderline.

Sigmoid: stdev(net_pnl)=362.21 → recommended scale 181 (used 163). Will be updated for run C.

4.4 Planned Run C Changes
Switch to 4×9‑month windows (match 15min structure) to increase trade counts per window.

Update _SIGMOID_SCALE to 181 in consistency_scorer.py.

Lower min_trades_per_week to 0.3 (from 0.5) to boost Stage 1 pass rate.

Keep exploration zone only, parameters unchanged from run B.

4.5 Window Structure for Run C
Window	Period	Regime
W01	2023-01-02 → 2023-09-29	DAX recovery
W02	2023-10-02 → 2024-06-28	ECB rate cycle
W03	2024-07-01 → 2025-03-31	H2 2024 productive + H1 2025 dead
W04	2025-04-01 → 2026-02-28	Most recent regime
4.6 Frozen 10‑Minute Configuration
To be finalised after run C.

5. 5‑Minute Series (Overnight Runs, 10–14 hours)
5.1 Calibration History
Calibration	YAML	Run ID	Status	auto_go	Best WFO	Notes
A (raw)	backtest_V1_5min.yaml	(not logged)	✅ Complete	7	0.9734	safe zone productive (6 auto_go), exploration marginal (1 auto_go). sigmoid 163 (under‑scaled).
B (focused)	backtest_V1_5min_B.yaml	–	Planned	–	–	safe zone only, 500 samples, min_trades=20, constraints relaxed, sigmoid 172.
5.2 Raw Run A Findings
Stage 1 pass rate: 74/500 (14.8%). Safe zone passed 24% (60/250), exploration 5.6% (14/250). Rejections: win_rate (163), trades_per_week (107), expectancy (78), INSUFFICIENT_TRADES (77).

Average trades/week: 2.19 (≈360 trades over 38 months). 3‑month windows average ~28 trades.

WFO: 30 scored, 13 collapsed. Safe zone produced 6 auto_go, 2 borderline. Exploration produced 1 auto_go, 1 borderline.

Top safe candidate: 38af78ada974 – WFO=0.9734 (1 window), no spike, no collapse.

Top exploration candidate: e3a30e9d8a69 – WFO=0.8981 (3 windows), no spike, no collapse.

Sigmoid: stdev(net_pnl)=343.64 → recommended scale 172 (used 163). Will be updated for run B.

5.3 Planned Run B Changes
Disable exploration zone – focus all 500 samples on safe zone.

Increase samples_per_zone to 500.

Lower min_significant_trades to 20 (from 30).

Relax constraints based on Stage 1 distributions:

min_win_rate: 0.15 (from 0.18)

min_expectancy: -3.0 (from -2.0)

min_trades_per_week: 1.0 (from 1.5)

max_losing_streak: 40 (from 35)

Update sigmoid scale to 172 in consistency_scorer.py.

Increase GA budget: population 70, generations 40, stagnation 12 (from 60/30/10).

Keep safe zone parameter ranges unchanged.

5.4 Window Structure (13 × 3‑month, same as 1‑minute series)
See §2.4 for window definitions.

5.5 Frozen 5‑Minute Configuration
To be finalised after run B.

6. Common Insights (Applicable to All Timeframes)
```

6.1 Phantom WFO Verdicts
The WFO scorer assigns near‑perfect scores to candidates with too few evaluated windows (≤2).

Fix (V2‑VERDICT‑GATE): Reject any candidate with windows_evaluated < 3 – verdict becomes INSUFFICIENT_COVERAGE.

Always check windows_evaluated before trusting a high WFO score.

6.2 risk_percentile Behaviour
Unit = percentage of account equity (0.45 = 0.45%, not 45%).

Acts as a trade filter, not position sizer: signal rejected if ATR‑based risk > threshold.

TF‑dependent calibration is mandatory:

1min: 0.20–0.30

15min: 0.85–1.10

6.3 Sigmoid Scale Calibration
_SIGMOID_SCALE should be set to stdev(net_pnl) × 0.5 from Stage 4.

Within‑run relative ranking is unaffected by the scale, but absolute fitness values change.

V2 action: Make it a per‑run config parameter.

6.4 WFO Window Sizing
Each window must average at least min_significant_trades trades.

1min: 30 (comfortable)

15min: 12 (absolute floor)

Longer windows reduce starvation; 4×9‑month structure solved 15min coverage.

6.5 MACD Filter Crash Fix
pta.macd() fails for short series; fix applied in macd_filter.py:

python
min_required = self.slow_length + self.signal_length + 1
if len(close) < min_required: return NaN
Do NOT set macd_signal < 2; enforce minimum 2 in all zone definitions.
```

7. Frozen Production Configurations
```
Both series are now considered mature and frozen. No further parameter changes are planned. The YAML files are the source of truth.

1‑minute: configs/backtesting/backtest_V1_1min.yaml (calibration F)

15‑minute: configs/backtesting/backtest_V1_15min.yaml (calibration G)

Any future 1‑minute runs (if needed) should use the same ranges with possibly different random seeds. 15‑minute runs are not required but can be repeated with seed variation.
```

8. Next Steps for V3 (Intelligent Backtesting Orchestrator)
```
The methodology proven on 1min and 15min will be extended to 5‑minute and 10‑minute timeframes. The orchestrator should:

Automate the calibration cycle – systematically narrow parameter ranges based on top‑candidate clusters.

Select filters adaptively – test combinations of DPO, MACD, Choppiness, CCI, ADX, etc., to find the best set for each timeframe.

Optimise window length – compute required window length from min_significant_trades and observed trade frequency.

Auto‑calibrate sigmoid scale – after Stage 4, compute stdev and set scale for next run.

Enforce windows_evaluated >= 3 verdict gate before considering any candidate for production.

Maintain separate result trees for each timeframe – never mix data across TFs.

The current BACKTESTING_RESULTS.md serves as the knowledge base for the orchestrator’s initial search heuristics.
```

*Document generated: 2026-03-18*
Next review: after completing 5min & 10min series.
```