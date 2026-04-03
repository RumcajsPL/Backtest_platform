BACKTESTING_TRACKER.md — Comprehensive Backtesting History & Status

# Executive Summary – Paper Trading Candidates
This section consolidates the final, frozen candidates across all timeframes (1‑min, 5‑min, 10‑min, 15‑min).
All listed candidates have passed all pipeline gates: they carry an auto_go verdict, ruin_prob = 0.000, no window collapse, and no sensitivity spikes (except where noted).
They are ready for paper trading.
Three complementary views are provided to help select the best match for different trading objectives.

Estimated summary tables on the base of strategy timeline breakdown:
1. Best Profitability
markdown
| Timeframe | Candidate ID | WFO Score | Windows Eval | Net P&L (med pts) | Win Rate (trade) | Max DD (pts) | Ruin Prob | Rec Hours (UTC) | Rec Days | Est Filtered P&L (pts) |
|-----------|--------------|-----------|--------------|-------------------|------------------|--------------|-----------|-----------------|---------|------------------------|
| **15‑min** | `4afa4d0b5f36` | 0.7688 | 3 | 1184.84 | 0.2414 | -185.99 | 0.000 | 08,09,10,17 | Wed, Fri | **2022.2** (hours) |
| **1‑min**  | `2a891e2cce6c` | 0.8058 | 9 | 92.35 | 0.3000 | -71.08 | 0.000 | 09,16,20 | Thu, Fri | **605.0** (hours) |
| **1‑min**  | `27f9db483be6` | 0.7795 | 7 | 76.68 | 0.2904 | -120.01 | 0.000 | 09,10,11,15,19,20 | Thu | **743.9** (hours) |
| **5‑min**  | `58af52e348f5` | 0.8270 | 3 | 121.04 | 0.2751 | -69.85 | 0.000 | All positive hours* | Mon, Wed, Thu | **1168.2** (hours) |
| **5‑min**  | `7ffbc5e3522c` | 0.6869 | 9 | 83.15 | 0.2477 | -88.50 | 0.000 | 10,13,14,15,17,20 | Tue, Wed, Fri | **1533.8** (hours) |
| **1‑min**  | `61875464b3aa` | 0.7731 | 8 | 27.01 | 0.3255 | -548.79 | 0.000 | 09,11,12,13,16,19,20 | Mon, Thu, Fri | **1067.3** (hours) |

*Positive hours for 58af52e348f5: 09,10,11,13,14,16,18,19 (avoid 08,12,15,17,20).
2. Best for Capital Accumulation
markdown
| Timeframe | Candidate ID | WFO Score | Windows Eval | Net P&L (med pts) | Win Rate (trade) | Ruin Prob | Rec Hours (UTC) | Rec Days | Est Filtered P&L (pts) | Notes |
|-----------|--------------|-----------|--------------|-------------------|------------------|-----------|-----------------|---------|------------------------|-------|
| **1‑min**  | `65df7121489f` | 0.7941 | 8 | 30.87 | 0.3098 | 0.000 | 09,10,13,16,19,20 | Thu, Fri | **840.4** (hours) | Turns negative full P&L to positive |
| **1‑min**  | `61875464b3aa` | 0.7731 | 8 | 27.01 | 0.3255 | 0.000 | 09,11,12,13,16,19,20 | Mon, Thu, Fri | **1067.3** (hours) | Strong hours, high win rate |
| **1‑min**  | `a3451a370263` | 0.7637 | 7 | 27.57 | 0.3329 | 0.000 | 12,13,20 | Thu | **424.4** (hours) | Also consider 16,19 for extra gain |
| **5‑min**  | `7ffbc5e3522c` | 0.6869 | 9 | 83.15 | 0.2477 | 0.000 | 10,13,14,15,17,20 | Tue, Wed, Fri | **1533.8** (hours) | |
| **10‑min** | `240166da287e` | 0.8886 | 4 | 510.88 | 0.2800 | 0.000 | All positive hours* | (any) | **2708.2** (hours) | All days positive |

*Positive hours for 240166da287e: 08,10,11,12,15,16,17,19,20 (avoid 09,13,14,18).
3. Safety (Lowest Risk)
markdown
| Timeframe | Candidate ID | WFO Score | Windows Eval | Net P&L (med pts) | Win Rate (trade) | Ruin Prob | Rec Hours (UTC) | Rec Days | Est Filtered P&L (pts) | Risk Notes |
|-----------|--------------|-----------|--------------|-------------------|------------------|-----------|-----------------|---------|------------------------|------------|
| **1‑min**  | `65df7121489f` | 0.7941 | 8 | 30.87 | 0.3098 | 0.000 | 09,10,13,16,19,20 | Thu, Fri | **840.4** (hours) | High win rate, many regimes tested |
| **1‑min**  | `2a891e2cce6c` | 0.8058 | 9 | 92.35 | 0.3000 | 0.000 | 09,16,20 | Thu, Fri | **605.0** (hours) | Most windows, consistent |
| **10‑min** | `240166da287e` | 0.8886 | 4 | 510.88 | 0.2800 | 0.000 | All positive hours | (any) | **2708.2** (hours) | Low drawdown, high P&L |
| **5‑min**  | `7ffbc5e3522c` | 0.6869 | 9 | 83.15 | 0.2477 | 0.000 | 10,13,14,15,17,20 | Tue, Wed, Fri | **1533.8** (hours) | Good window coverage |
| **15‑min** | `4afa4d0b5f36` | 0.7688 | 3 | 1184.84 | 0.2414 | 0.000 | 08,09,10,17 | Wed, Fri | **2022.2** (hours) | Perfect per‑window, short history |

1. Best Profitability
markdown
| Timeframe | Candidate ID | WFO Score | Windows Evaluated | Fraction Positive | Net P&L (med pts) | Win Rate (trade‑level) | Max Drawdown (pts) | Ruin Prob | Collapse | Spikes |
|-----------|--------------|-----------|-------------------|-------------------|-------------------|------------------------|--------------------|-----------|----------|--------|
| **15‑min** | `4afa4d0b5f36` | 0.7688 | 3 | 1.000 | **1184.84** | 0.2414 | -185.99 | 0.000 | No | No |
| **1‑min**  | `2a891e2cce6c` | 0.8058 | 9 | 0.667 | 92.35 | 0.3000 | -71.08 | 0.000 | No | No |
| **1‑min**  | `27f9db483be6` | 0.7795 | 7 | 0.714 | 76.68 | 0.2904 | -120.01 | 0.000 | No | No |
| **5‑min**  | `58af52e348f5` | 0.8270 | 3 | 1.000 | 121.04 | 0.2751 | -69.85 | 0.000 | No | No |
| **5‑min**  | `7ffbc5e3522c` | 0.6869 | 9 | 0.778 | 83.15 | 0.2477 | -88.50 | 0.000 | No | No |
2. Best for Capital Accumulation
markdown
| Timeframe | Candidate ID | WFO Score | Windows Evaluated | Fraction Positive | Net P&L (med pts) | Win Rate (trade‑level) | Ruin Prob | Collapse | Spikes | Notes |
|-----------|--------------|-----------|-------------------|-------------------|-------------------|------------------------|-----------|----------|--------|-------|
| **1‑min**  | `65df7121489f` | 0.7941 | 8 | 0.750 | 30.87 | 0.3098 | 0.000 | No | No | Highest win rate among 1‑min |
| **1‑min**  | `61875464b3aa` | 0.7731 | 8 | 0.750 | 27.01 | 0.3255 | 0.000 | No | No | Consistent, slightly lower P&L |
| **1‑min**  | `a3451a370263` | 0.7637 | 7 | 0.714 | 27.57 | 0.3329 | 0.000 | No | No | 7 profitable windows |
| **5‑min**  | `7ffbc5e3522c` | 0.6869 | 9 | 0.778 | 83.15 | 0.2477 | 0.000 | No | No | Most windows evaluated |
| **10‑min** | `240166da287e` | 0.8886 | 4 | 1.000 | 510.88 | 0.2800 | 0.000 | No | No | Perfect coverage, high WFO |
3. Safety (Lowest Risk)
markdown
| Timeframe | Candidate ID | WFO Score | Windows Evaluated | Fraction Positive | Net P&L (med pts) | Win Rate (trade‑level) | Ruin Prob | Collapse | Spikes | Risk Notes |
|-----------|--------------|-----------|-------------------|-------------------|-------------------|------------------------|-----------|----------|--------|------------|
| **1‑min**  | `65df7121489f` | 0.7941 | **8/13** | 0.750 | 30.87 | 0.3098 | 0.000 | No | No | High win rate, many regimes tested |
| **1‑min**  | `2a891e2cce6c` | 0.8058 | **9/13** | 0.667 | 92.35 | 0.3000 | 0.000 | No | No | Most windows, slightly lower win rate |
| **10‑min** | `240166da287e` | 0.8886 | **4/4** | 1.000 | 510.88 | 0.2800 | 0.000 | No | No | All windows passed, but fewer regimes |
| **5‑min**  | `7ffbc5e3522c` | 0.6869 | **9/13** | 0.778 | 83.15 | 0.2477 | 0.000 | No | No | Good window coverage |
| **15‑min** | `4afa4d0b5f36` | 0.7688 | **3/4** | 1.000 | 1184.84 | 0.2414 | 0.000 | No | No | Perfect per‑window, but short test history |



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

4.7 Calibration C Results (548dacea-165b-4f3a-bca5-6944bf40c838)

- Stage 1 pass rate: 101/400 (25.3%). Average trades/week = 0.68.
- WFO: 30 scored, 19 collapsed (63%). Top candidate `4228c5a263f1` achieved WFO=0.9717 but only 1 evaluated window.
- Verdicts: 2 auto_go, 8 borderline. Both auto_go had windows_evaluated = 1 → phantom verdicts.
- Trade starvation remains the dominant issue; even with 9‑month windows many candidates fail to reach 20 trades.
- Sigmoid scale was under‑scaled (used 181 vs recommended 258). Net P&L stdev = 516.16.

4.8 Planned Calibration D (backtest_V1_10min_D.yaml)

Changes:
- Enable safe zone with narrowed ranges derived from top‑10 WFO candidates.
- Update sigmoid scale to 258.
- Lower min_significant_trades to 15, min_trades_per_week to 0.2.
- Increase GA population to 80, generations to 45, stagnation to 14.
- Increase random search samples to 500 total (250 per zone).
- Verdict gate (windows_evaluated ≥ 3) must be enforced in pipeline before this run.

Goal: Obtain at least one honest auto_go candidate with ≥3 evaluated windows and no collapse/spikes.

4.9 Calibration D (Data Collection – backtest_V1_10min_D_datacollect.yaml)

**Purpose:** Collect trade frequency and performance data for two filter sets on 10min to inform parameter narrowing for final calibration (E).

**Settings:**
- Stages: random_search + walk_forward only (no GA, MC, sensitivity).
- Random search: 500 samples per zone (safe + exploration), total 1000.
- Constraints: very loose (`e2e_test` scenario) to allow nearly all candidates to pass.
- WFO: all 1000 candidates evaluated across 4 × 9‑month windows.
- Sigmoid scale set to 258 (code change required).
- Safe zone: DPO+MACD, ranges slightly widened from 15min series.
- Exploration zone: DPO+MA+Bollinger, original wide ranges.

**Expected outputs:**
- Distribution of trades/week per filter set.
- Parameter regions with trade frequency ≥ 1.0 and positive expectancy.
- Data to guide final parameter narrowing for Run E.

**Runtime:** ~10–12 hours.

4.12 Calibration D – Data Collection Results (3d64c26d-40ce-46c8-90b6-7d7aba7481d5)
Purpose: Compare trade frequency and performance of safe zone (DPO+MACD) vs exploration zone (DPO+MA+Bollinger) on 4×9‑month windows.

Settings:

Scenario: e2e_test (very loose constraints)

Random search: 500 samples per zone (1000 total)

Stages: random search + walk‑forward only

Sigmoid scale: not used in e2e_test, but computed later

Results:

Stage 1 pass rate: 631/1000 (63%) – safe 400/500 (80%), exploration 231/500 (46%)

Average trades/week (Stage 1): 0.66

WFO scored: 631 candidates

Collapsed: 449 (71%) – safe 309 (77%), exploration 140 (61%)

Top safe candidate: d75d1b49b5c3 – WFO=0.9630, 3 windows evaluated, no collapse

Top exploration candidate: 91f20af410da – WFO=0.9631, 1 window evaluated

Recommended sigmoid scale: 242.1 (stdev=484.25 × 0.5)

Key Conclusions:

Safe zone (DPO+MACD) is superior: higher pass rate, higher trade frequency, better WFO scores.

Exploration zone will be disabled for final calibration.

Productive parameter ranges for safe zone have been identified from top candidates.

4.13 Calibration E (Final) – Planned
Configuration: backtest_V1_10min_E.yaml (to be created)

Changes from Calibration D:

Safe zone only.

Narrowed parameter ranges based on top‑30 safe candidates (see table above).

Enable GA, MC, sensitivity.

Enforce windows_evaluated ≥ 3 in verdict gate (code update required).

Update _SIGMOID_SCALE to 242.1.

Keep constraints: min_trades_per_week=0.3, min_win_rate=0.11, etc.

Expected Runtime: ~10–12 hours

Goal: Obtain at least one auto_go candidate with ≥3 windows evaluated, no collapse, and no sensitivity spikes.

4.14 Calibration F – Final (822f1889-1810-4a22-8393-eaa4792a759e)
Configuration: backtest_V1_10min_F.yaml (safe zone only, narrowed ranges from E).

Random search: 500 LHS samples, min_significant_trades=20.

GA: population 70, generations 40, stagnation 12.

WFO: 4×9‑month windows.

Sigmoid scale: 313 (code setting; recommended 233 from this run).

Results:

Stage 1 pass rate: 485/500 (97%).

Average trades/week: 0.84.

WFO scored: 30 candidates, 26 collapsed.

Verdicts: 1 auto_go, 9 borderline.

Honest auto_go candidate:

240166da287e – WFO=0.8886, windows_evaluated=4, no collapse, no spikes, ruin_prob=0.000.

Observations:

Trade frequency remains modest but sufficient for the 9‑month windows.

The narrowed safe zone produced a robust candidate with full window coverage.

The parameter region is now well‑defined and ready for production.

4.15 Frozen 10‑Minute Configuration
Final YAML: configs/backtesting/backtest_V1_10min_F.yaml (as in Calibration F).
Sigmoid scale (code): set to 233 in consistency_scorer.py for any future runs.
Production candidate: 240166da287e – YAML available in outputs/backtesting/trading_yamls/822f1889_240166da287e_strategy.yaml.

5. 5‑Minute Series (Overnight Runs, 10–14 hours)

5.1 Calibration History
Calibration | YAML | Run ID | Status | auto_go | Best WFO | Notes
--- | --- | --- | --- | --- | --- | ---
A (raw) | backtest_V1_5min.yaml | (not logged) | ✅ Complete | 7 | 0.9734 | Safe zone productive (6 auto_go), exploration marginal (1 auto_go). sigmoid 163 (under‑scaled).
B (focused) | backtest_V1_5min_B.yaml | 4b87d038-... | ✅ Complete | 4 | 0.9009 | Safe zone only, 500 samples, constraints relaxed, sigmoid 172.
C (final) | backtest_V1_5min_final.yaml | b8b6f21a-... | ✅ Complete | 3 | 0.8912 | Widened safe‑zone ranges, sigmoid 204, 2 robust auto_go (≥3 windows).

5.2 Final Parameter Ranges (Safe Zone – Frozen)
Parameter | Type | Min | Max | Step | Notes
--- | --- | --- | --- | --- | ---
atr_length | int | 8 | 20 | 1 | –
atr_multiplier | float | 1.6 | 2.8 | 0.1 | –
rr_target | float | 3.0 | 6.0 | 0.1 | –
risk_percentile | float | 0.40 | 0.80 | 0.02 | –
dpo_length | int | 18 | 30 | 1 | –
dpo_smooth | int | 8 | 18 | 1 | –
dpo_threshold | float | 0.10 | 0.25 | 0.02 | –
cci_length | int | 12 | 24 | 1 | –
cci_overbought | int | 90 | 110 | 2 | –
cci_oversold | int | -110 | -90 | 2 | –
macd_fast | int | 8 | 14 | 1 | –
macd_slow | int | 20 | 30 | 1 | –
macd_signal | int | 6 | 12 | 1 | –

5.3 Sigmoid Scale
The frozen configuration uses `_SIGMOID_SCALE = 204` (set in `consistency_scorer.py`). This value is slightly higher than the stdev‑derived recommendation (which would be stdev × 0.5, typically ~170). The slight over‑scaling did not prevent the discovery of robust auto_go candidates. For future 5‑minute runs, the recommended practice is to compute `_SIGMOID_SCALE = stdev(net_pnl) × 0.5` after a Stage‑1‑only run.

5.4 Window Structure (13 × 3‑month, same as 1‑minute series)
See §2.4 for window definitions.

5.5 Final Auto_Go Candidates (Ready for Paper Trading)
| Candidate | WFO | Windows evaluated | Median return | Spike |
|-----------|-----|-------------------|---------------|-------|
| 58af52e348f5 | 0.8270 | 3 | 121.04 | No |
| 7ffbc5e3522c | 0.6869 | 9 | 83.15 | No |

**Note:** Candidate `b21253cd5805` also received auto_go but had only 1 WFO window and is **excluded** from paper trading (phantom verdict). The pipeline’s verdict gate does not currently enforce a minimum of 3 evaluated windows – this will be addressed in a future update.

5.6 Frozen 5‑Minute Configuration
**YAML file:** `configs/backtesting/backtest_V1_5min_final.yaml` (full content below)

**Key settings:**
- `scenario: capital_accumulation`
- Safe zone only (exploration and discovery disabled)
- Parameter ranges as defined above
- `random_search.samples_per_zone: 500`, `min_significant_trades: 20`
- `genetic.population_size: 70`, `generations: 40`, `stagnation_generations: 12`
- `_SIGMOID_SCALE` in `consistency_scorer.py`: **204**
- All other settings as in the YAML below

**Trading YAMLs** for the auto_go candidates are located in `outputs/backtesting/trading_yamls/` of run `b8b6f21a-9c8f-4738-a418-950217463540`.

The 5‑minute series is now **complete and frozen**.

**YAML file location:** `configs/backtesting/backtest_V1_5min_final.yaml`

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

8. Next Steps specifically for V3 (Intelligent Backtesting Orchestrator) but to review in V2
```
The methodology proven on 1min and 15min will be extended to 5‑minute and 10‑minute timeframes. The orchestrator should:

Automate the calibration cycle – systematically narrow parameter ranges based on top‑candidate clusters.

Select filters adaptively – test combinations of DPO, MACD, Choppiness, CCI, ADX, etc., to find the best set for each timeframe.

Optimise window length – compute required window length from min_significant_trades and observed trade frequency.

Auto‑calibrate sigmoid scale – after Stage 4, compute stdev and set scale for next run.

Enforce windows_evaluated >= 3 verdict gate before considering any candidate for production.

Maintain separate result trees for each timeframe – never mix data across TFs.

The current BACKTESTING_TRACKER.md serves as the knowledge base for the orchestrator’s initial search heuristics.
```