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

## Breakdowns strategy runs for each of paper trading
- 1min candidates: 3 months(3M), 6 months(6M), 38 months (full) strategy run breakdowns
- 5min candidates: 3 months(3M), 6 months(6M), 38 months (full) strategy run breakdowns
- 10min candidates: 6 months(6M), 38 months (full) strategy run breakdowns
- 15min candidates: 6 months(6M), 38 months (full) strategy run breakdowns

# report_20260324_2a891e2cce6c_3M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	56
Win Rate	30.36%
Total P&L	-5.97 pts
Profit Factor	0.9900
Max Drawdown	-234.05 pts
Largest Win	42.91 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	24	37.5%	+88.0	+3.67	+42.9	-16.9
London	32	25.0%	-94.0	-2.94	+41.9	-27.1
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
09:00	1	100.0%	+8.2	+8.16
10:00	2	0.0%	-31.8	-15.92
11:00	6	33.3%	+25.6	+4.26
12:00	10	20.0%	-39.4	-3.94
13:00	3	33.3%	-0.6	-0.20
14:00	7	28.6%	-12.8	-1.84
15:00	3	0.0%	-43.0	-14.34
16:00	1	100.0%	+42.9	+42.91
17:00	4	25.0%	+4.3	+1.09
18:00	5	40.0%	+14.7	+2.94
19:00	10	30.0%	-1.4	-0.14
20:00	4	50.0%	+27.4	+6.86
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	14	35.7%	+27.9	+2.00
Tuesday	14	14.3%	-104.3	-7.45
Wednesday	15	26.7%	-36.5	-2.43
Thursday	2	100.0%	+75.9	+37.97
Friday	11	36.4%	+30.9	+2.81

# report_20260324_2a891e2cce6c_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	128
Win Rate	32.81%
Total P&L	+282.25 pts
Profit Factor	1.2400
Max Drawdown	-234.05 pts
Largest Win	55.38 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	61	36.1%	+217.4	+3.56	+54.1	-18.0
London	67	29.9%	+64.9	+0.97	+55.4	-27.1
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
09:00	5	40.0%	+18.2	+3.63
10:00	10	40.0%	+85.5	+8.55
11:00	12	33.3%	+43.5	+3.63
12:00	15	26.7%	-20.1	-1.34
13:00	8	25.0%	-18.1	-2.26
14:00	8	25.0%	-30.9	-3.86
15:00	9	22.2%	-13.3	-1.48
16:00	6	66.7%	+110.5	+18.42
17:00	10	10.0%	-72.6	-7.26
18:00	21	28.6%	-13.9	-0.66
19:00	17	47.1%	+132.9	+7.82
20:00	7	42.9%	+60.5	+8.64
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	39	33.3%	+106.7	+2.74
Tuesday	31	32.3%	+98.2	+3.17
Wednesday	27	22.2%	-112.7	-4.17
Thursday	10	60.0%	+142.8	+14.28
Friday	21	33.3%	+47.2	+2.25

# report_20260324_2a891e2cce6c_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	810
Win Rate	30.00%
Total P&L	+394.46 pts
Profit Factor	1.0600
Max Drawdown	-475.25 pts
Largest Win	55.38 pts
Largest Loss	-84.59 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	461	30.4%	+269.7	+0.58	+54.1	-84.6
London	349	29.5%	+124.8	+0.36	+55.4	-56.0
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	7	0.0%	-100.8	-14.40
09:00	27	40.7%	+137.7	+5.10
10:00	55	27.3%	-20.0	-0.36
11:00	71	28.2%	+23.3	+0.33
12:00	77	37.7%	+258.0	+3.35
13:00	47	34.0%	+146.6	+3.12
14:00	28	14.3%	-187.8	-6.71
15:00	37	21.6%	-132.1	-3.57
16:00	68	41.2%	+362.5	+5.33
17:00	110	22.7%	-296.4	-2.69
18:00	125	27.2%	-86.2	-0.69
19:00	117	33.3%	+185.0	+1.58
20:00	41	34.1%	+104.8	+2.56
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	192	27.6%	-68.3	-0.36
Tuesday	177	29.4%	-5.1	-0.03
Wednesday	166	27.1%	-94.2	-0.57
Thursday	136	34.6%	+274.0	+2.01
Friday	139	33.1%	+288.1	+2.07

## report_20260324_4afa4d0b5f36_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	14
Win Rate	28.57%
Total P&L	+782.01 pts
Profit Factor	2.4100
Max Drawdown	-246.56 pts
Largest Win	428.06 pts
Largest Loss	-77.85 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	9	33.3%	+669.1	+74.35	+428.1	-77.8
NY	5	20.0%	+112.9	+22.58	+300.3	-61.9
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	100.0%	+428.1	+428.06
09:00	2	50.0%	+226.1	+113.06
10:00	4	25.0%	+144.8	+36.20
14:00	1	0.0%	-62.1	-62.11
15:00	1	0.0%	-67.7	-67.74
16:00	1	0.0%	-61.4	-61.42
17:00	1	100.0%	+300.3	+300.27
18:00	1	0.0%	-26.9	-26.86
19:00	2	0.0%	-99.1	-49.55
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	3	33.3%	+152.3	+50.76
Tuesday	2	0.0%	-139.1	-69.53
Wednesday	4	50.0%	+687.4	+171.84
Thursday	2	0.0%	-123.4	-61.68
Friday	3	33.3%	+204.8	+68.26

## report_20260324_4afa4d0b5f36_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	87
Win Rate	24.14%
Total P&L	+2670.17 pts
Profit Factor	1.8700
Max Drawdown	-733.13 pts
Largest Win	428.06 pts
Largest Loss	-129.49 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	48	25.0%	+1596.3	+33.26	+428.1	-129.5
NY	39	23.1%	+1073.9	+27.54	+366.4	-123.4
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	5	40.0%	+439.5	+87.89
09:00	11	18.2%	+235.8	+21.44
10:00	10	50.0%	+1128.7	+112.87
11:00	4	0.0%	-240.7	-60.17
12:00	1	0.0%	-42.8	-42.79
13:00	5	0.0%	-197.1	-39.43
14:00	3	33.3%	+153.8	+51.26
15:00	9	22.2%	+119.1	+13.23
16:00	7	28.6%	+154.0	+22.00
17:00	9	22.2%	+218.2	+24.24
18:00	14	28.6%	+738.0	+52.72
19:00	3	0.0%	-139.7	-46.57
20:00	6	16.7%	+103.4	+17.24
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	24	8.3%	-309.9	-12.91
Tuesday	15	26.7%	+328.6	+21.90
Wednesday	21	33.3%	+1402.8	+66.80
Thursday	13	23.1%	+310.0	+23.85
Friday	14	35.7%	+938.7	+67.05

## report_20260324_7ffbc5e3522c_3M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	39
Win Rate	28.21%
Total P&L	+146.86 pts
Profit Factor	1.1800
Max Drawdown	-362.04 pts
Largest Win	118.89 pts
Largest Loss	-44.68 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	21	38.1%	+377.1	+17.96	+118.9	-42.5
NY	18	16.7%	-230.3	-12.79	+89.7	-44.7
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	100.0%	+19.3	+19.31
09:00	3	0.0%	-70.5	-23.52
10:00	3	66.7%	+191.0	+63.68
11:00	1	0.0%	-42.5	-42.48
12:00	4	0.0%	-114.4	-28.61
13:00	4	75.0%	+291.9	+72.97
14:00	5	40.0%	+102.4	+20.48
16:00	1	0.0%	-33.4	-33.42
17:00	4	0.0%	-117.6	-29.40
18:00	2	50.0%	+61.4	+30.70
19:00	8	12.5%	-143.1	-17.89
20:00	3	33.3%	+2.4	+0.80
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	8	12.5%	-121.4	-15.18
Tuesday	8	25.0%	+63.6	+7.95
Wednesday	9	44.4%	+156.9	+17.43
Thursday	3	0.0%	-115.0	-38.35
Friday	11	36.4%	+162.9	+14.81

## report_20260324_7ffbc5e3522c_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	76
Win Rate	28.95%
Total P&L	+438.48 pts
Profit Factor	1.2500
Max Drawdown	-378.40 pts
Largest Win	129.63 pts
Largest Loss	-82.53 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	46	32.6%	+408.7	+8.89	+127.4	-82.5
NY	30	23.3%	+29.7	+0.99	+129.6	-44.7
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	2	50.0%	-27.2	-13.60
09:00	7	14.3%	-50.7	-7.24
10:00	6	33.3%	+41.0	+6.84
11:00	2	0.0%	-68.7	-34.33
12:00	8	12.5%	-112.6	-14.07
13:00	7	71.4%	+483.6	+69.08
14:00	9	33.3%	+119.9	+13.32
15:00	5	40.0%	+23.4	+4.67
16:00	1	0.0%	-33.4	-33.42
17:00	7	42.9%	+223.7	+31.96
18:00	6	33.3%	+90.5	+15.08
19:00	12	8.3%	-224.3	-18.69
20:00	4	25.0%	-26.7	-6.68
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	13	15.4%	-162.4	-12.49
Tuesday	13	38.5%	+320.3	+24.63
Wednesday	16	37.5%	+211.6	+13.22
Thursday	11	18.2%	-106.1	-9.65
Friday	23	30.4%	+175.2	+7.62

## report_20260324_7ffbc5e3522c_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	440
Win Rate	24.77%
Total P&L	+491.38 pts
Profit Factor	1.0600
Max Drawdown	-712.46 pts
Largest Win	138.25 pts
Largest Loss	-127.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	226	27.4%	+912.7	+4.04	+138.3	-127.1
NY	214	22.0%	-421.3	-1.97	+129.6	-63.5
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	13	30.8%	-0.0	-0.00
09:00	20	15.0%	-102.4	-5.12
10:00	32	37.5%	+349.7	+10.93
11:00	38	21.1%	-92.8	-2.44
12:00	40	22.5%	-80.3	-2.01
13:00	41	31.7%	+453.9	+11.07
14:00	28	32.1%	+332.5	+11.88
15:00	14	28.6%	+52.0	+3.71
16:00	26	19.2%	-94.5	-3.64
17:00	37	29.7%	+318.0	+8.60
18:00	55	23.6%	-58.6	-1.07
19:00	75	16.0%	-614.0	-8.19
20:00	21	28.6%	+27.7	+1.32
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	98	24.5%	-105.0	-1.07
Tuesday	80	30.0%	+708.1	+8.85
Wednesday	95	25.3%	+83.9	+0.88
Thursday	78	20.5%	-205.4	-2.63
Friday	89	23.6%	+9.7	+0.11

## report_20260324_27f9db483be6_3M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	41
Win Rate	19.51%
Total P&L	-173.72 pts
Profit Factor	0.6600
Max Drawdown	-204.64 pts
Largest Win	54.00 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	16	25.0%	-11.5	-0.72	+49.6	-18.7
London	25	16.0%	-162.2	-6.49	+54.0	-27.1
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
10:00	2	0.0%	-32.1	-16.06
11:00	6	16.7%	-33.8	-5.63
12:00	8	25.0%	-7.2	-0.90
13:00	2	0.0%	-36.3	-18.17
14:00	5	20.0%	-15.9	-3.19
15:00	2	0.0%	-36.8	-18.41
17:00	1	0.0%	-18.7	-18.66
18:00	3	0.0%	-45.3	-15.10
19:00	8	25.0%	+18.8	+2.36
20:00	4	50.0%	+33.6	+8.39
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	11	18.2%	-63.5	-5.77
Tuesday	10	10.0%	-108.2	-10.82
Wednesday	11	9.1%	-94.4	-8.58
Thursday	2	100.0%	+85.9	+42.97
Friday	7	28.6%	+6.4	+0.92

## report_20260324_27f9db483be6_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	110
Win Rate	29.09%
Total P&L	+182.13 pts
Profit Factor	1.1400
Max Drawdown	-204.64 pts
Largest Win	69.80 pts
Largest Loss	-29.72 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	45	33.3%	+144.1	+3.20	+65.2	-29.7
London	65	26.2%	+38.0	+0.58	+69.8	-27.1
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	0.0%	-21.6	-21.56
09:00	3	33.3%	+28.2	+9.40
10:00	12	41.7%	+147.6	+12.30
11:00	14	35.7%	+117.7	+8.41
12:00	13	15.4%	-106.3	-8.18
13:00	10	10.0%	-122.7	-12.27
14:00	6	16.7%	-37.9	-6.32
15:00	6	33.3%	+33.0	+5.50
16:00	4	25.0%	-10.2	-2.56
17:00	6	0.0%	-90.7	-15.12
18:00	16	31.2%	+22.8	+1.43
19:00	14	42.9%	+133.9	+9.56
20:00	5	60.0%	+88.4	+17.67
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	34	29.4%	+45.5	+1.34
Tuesday	24	20.8%	-93.4	-3.89
Wednesday	24	20.8%	-30.5	-1.27
Thursday	10	70.0%	+278.0	+27.80
Friday	18	27.8%	-17.4	-0.97

## report_20260324_27f9db483be6_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	668
Win Rate	29.04%
Total P&L	+324.43 pts
Profit Factor	1.0500
Max Drawdown	-480.71 pts
Largest Win	69.80 pts
Largest Loss	-104.53 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	319	27.6%	+203.9	+0.64	+69.8	-47.0
NY	349	30.4%	+120.5	+0.35	+65.2	-104.5
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	7	14.3%	-48.2	-6.89
09:00	26	30.8%	+106.7	+4.10
10:00	46	28.3%	+63.5	+1.38
11:00	78	30.8%	+200.2	+2.57
12:00	60	31.7%	+94.3	+1.57
13:00	54	24.1%	-48.0	-0.89
14:00	22	13.6%	-174.9	-7.95
15:00	26	26.9%	+10.3	+0.40
16:00	42	31.0%	-55.9	-1.33
17:00	83	26.5%	-133.4	-1.61
18:00	93	26.9%	-53.4	-0.57
19:00	98	34.7%	+285.0	+2.91
20:00	33	36.4%	+78.2	+2.37
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	165	27.3%	-51.5	-0.31
Tuesday	145	27.6%	-60.6	-0.42
Wednesday	137	26.3%	-85.2	-0.62
Thursday	101	35.6%	+474.0	+4.69
Friday	120	30.8%	+47.7	+0.40

## report_20260324_58af52e348f5_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	32
Win Rate	34.38%
Total P&L	+553.79 pts
Profit Factor	1.7800
Max Drawdown	-198.87 pts
Largest Win	153.23 pts
Largest Loss	-50.79 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	23	30.4%	+278.5	+12.11	+138.6	-50.8
London	9	44.4%	+275.3	+30.59	+153.2	-41.4
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
09:00	1	100.0%	+49.2	+49.19
10:00	1	0.0%	-40.8	-40.78
11:00	2	50.0%	+113.6	+56.78
12:00	3	0.0%	-108.5	-36.17
13:00	1	100.0%	+128.9	+128.89
14:00	1	100.0%	+133.0	+132.98
17:00	2	50.0%	+97.5	+48.75
18:00	8	37.5%	+216.5	+27.07
19:00	10	20.0%	-41.1	-4.11
20:00	3	33.3%	+5.5	+1.84
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	7	28.6%	+69.5	+9.93
Tuesday	4	25.0%	+46.3	+11.57
Wednesday	8	75.0%	+534.1	+66.76
Thursday	3	33.3%	+46.8	+15.61
Friday	10	10.0%	-142.9	-14.29

## report_20260324_58af52e348f5_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	189
Win Rate	27.51%
Total P&L	+929.14 pts
Profit Factor	1.2400
Max Drawdown	-515.11 pts
Largest Win	153.23 pts
Largest Loss	-67.01 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	137	27.0%	+565.0	+4.12	+138.6	-50.8
London	52	28.8%	+364.2	+7.00	+153.2	-67.0
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	2	0.0%	-44.4	-22.20
09:00	1	100.0%	+49.2	+49.19
10:00	6	33.3%	+81.7	+13.62
11:00	12	25.0%	+36.4	+3.03
12:00	15	20.0%	-88.1	-5.88
13:00	10	50.0%	+331.6	+33.16
14:00	5	20.0%	+22.2	+4.45
15:00	1	0.0%	-24.4	-24.37
16:00	12	25.0%	+48.9	+4.07
17:00	20	20.0%	-65.4	-3.27
18:00	36	30.6%	+396.9	+11.02
19:00	57	28.1%	+201.3	+3.53
20:00	12	25.0%	-16.6	-1.38
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	52	30.8%	+471.4	+9.06
Tuesday	32	15.6%	-243.6	-7.61
Wednesday	35	42.9%	+776.1	+22.17
Thursday	31	29.0%	+301.3	+9.72
Friday	39	17.9%	-376.1	-9.64

## report_20260324_240166da287e_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	14
Win Rate	28.57%
Total P&L	+395.13 pts
Profit Factor	2.0000
Max Drawdown	-324.32 pts
Largest Win	258.97 pts
Largest Loss	-57.50 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	8	37.5%	+435.7	+54.46	+259.0	-57.5
NY	6	16.7%	-40.5	-6.75	+126.9	-45.6
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	0.0%	-43.6	-43.64
09:00	2	50.0%	+151.7	+75.86
10:00	1	0.0%	-48.0	-48.01
11:00	1	100.0%	+199.4	+199.42
12:00	1	0.0%	-25.3	-25.30
14:00	1	0.0%	-57.5	-57.50
15:00	1	100.0%	+259.0	+258.97
16:00	1	0.0%	-38.9	-38.89
17:00	1	0.0%	-45.6	-45.57
19:00	3	33.3%	+78.0	+26.02
20:00	1	0.0%	-34.1	-34.12
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	4	25.0%	-26.0	-6.50
Tuesday	5	20.0%	+109.1	+21.82
Wednesday	1	100.0%	+199.4	+199.42
Thursday	2	50.0%	+155.5	+77.76
Friday	2	0.0%	-42.9	-21.44

## report_20260324_240166da287e_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	100
Win Rate	28.00%
Total P&L	+2163.47 pts
Profit Factor	2.0200
Max Drawdown	-384.47 pts
Largest Win	280.29 pts
Largest Loss	-123.53 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	64	26.6%	+1223.2	+19.11	+259.0	-123.5
NY	36	30.6%	+940.3	+26.12	+280.3	-45.6
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	3	66.7%	+303.8	+101.26
09:00	10	10.0%	-167.9	-16.79
10:00	10	30.0%	+208.0	+20.80
11:00	7	57.1%	+656.7	+93.81
12:00	15	26.7%	+293.0	+19.54
13:00	9	22.2%	-55.8	-6.20
14:00	6	0.0%	-218.9	-36.49
15:00	4	25.0%	+204.3	+51.06
16:00	14	35.7%	+662.9	+47.35
17:00	5	40.0%	+153.3	+30.67
18:00	4	0.0%	-102.1	-25.53
19:00	8	37.5%	+204.1	+25.51
20:00	5	20.0%	+22.1	+4.42
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	22	27.3%	+402.8	+18.31
Tuesday	25	32.0%	+679.8	+27.19
Wednesday	17	17.6%	+120.4	+7.08
Thursday	24	29.2%	+695.2	+28.96
Friday	12	33.3%	+265.3	+22.11

## report_20260324_61875464b3aa_3M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	725
Win Rate	32.55%
Total P&L	+98.69 pts
Profit Factor	1.0200
Max Drawdown	-548.79 pts
Largest Win	54.21 pts
Largest Loss	-84.59 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	335	33.4%	+255.9	+0.76	+54.2	-56.0
NY	390	31.8%	-157.2	-0.40	+52.3	-84.6
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	9	11.1%	-96.5	-10.72
09:00	33	33.3%	+28.4	+0.86
10:00	43	32.6%	-0.2	-0.01
11:00	72	33.3%	+99.5	+1.38
12:00	71	40.8%	+239.5	+3.37
13:00	54	38.9%	+200.5	+3.71
14:00	22	13.6%	-171.5	-7.80
15:00	31	29.0%	-43.7	-1.41
16:00	58	39.7%	+152.0	+2.62
17:00	91	23.1%	-411.5	-4.52
18:00	105	26.7%	-245.1	-2.33
19:00	104	35.6%	+159.5	+1.53
20:00	32	46.9%	+187.9	+5.87
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	175	33.1%	+165.5	+0.95
Tuesday	159	31.4%	-138.6	-0.87
Wednesday	145	29.0%	-173.9	-1.20
Thursday	116	37.1%	+171.9	+1.48
Friday	130	33.1%	+73.8	+0.57

## report_20260324_61875464b3aa_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	118
Win Rate	34.75%
Total P&L	+208.98 pts
Profit Factor	1.1700
Max Drawdown	-236.40 pts
Largest Win	52.29 pts
Largest Loss	-32.31 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	66	36.4%	+169.6	+2.57	+52.0	-27.1
NY	52	32.7%	+39.4	+0.76	+52.3	-32.3
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	0.0%	-21.6	-21.56
09:00	5	40.0%	+7.4	+1.47
10:00	9	55.6%	+127.6	+14.17
11:00	11	45.5%	+95.6	+8.69
12:00	16	37.5%	+46.8	+2.93
13:00	11	18.2%	-95.1	-8.64
14:00	4	25.0%	-11.4	-2.85
15:00	9	33.3%	+20.3	+2.25
16:00	4	75.0%	+83.0	+20.75
17:00	8	0.0%	-120.1	-15.01
18:00	19	26.3%	-69.8	-3.68
19:00	16	37.5%	+71.9	+4.49
20:00	5	60.0%	+74.4	+14.88
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	34	41.2%	+213.1	+6.27
Tuesday	30	33.3%	+33.3	+1.11
Wednesday	24	12.5%	-210.4	-8.77
Thursday	9	66.7%	+120.5	+13.39
Friday	21	38.1%	+52.4	+2.50

## report_20260324_61875464b3aa_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	725
Win Rate	32.55%
Total P&L	+98.69 pts
Profit Factor	1.0200
Max Drawdown	-548.79 pts
Largest Win	54.21 pts
Largest Loss	-84.59 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	335	33.4%	+255.9	+0.76	+54.2	-56.0
NY	390	31.8%	-157.2	-0.40	+52.3	-84.6
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	9	11.1%	-96.5	-10.72
09:00	33	33.3%	+28.4	+0.86
10:00	43	32.6%	-0.2	-0.01
11:00	72	33.3%	+99.5	+1.38
12:00	71	40.8%	+239.5	+3.37
13:00	54	38.9%	+200.5	+3.71
14:00	22	13.6%	-171.5	-7.80
15:00	31	29.0%	-43.7	-1.41
16:00	58	39.7%	+152.0	+2.62
17:00	91	23.1%	-411.5	-4.52
18:00	105	26.7%	-245.1	-2.33
19:00	104	35.6%	+159.5	+1.53
20:00	32	46.9%	+187.9	+5.87
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	175	33.1%	+165.5	+0.95
Tuesday	159	31.4%	-138.6	-0.87
Wednesday	145	29.0%	-173.9	-1.20
Thursday	116	37.1%	+171.9	+1.48
Friday	130	33.1%	+73.8	+0.57

## report_20260324_a3451a370263_3M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	49
Win Rate	28.57%
Total P&L	-58.95 pts
Profit Factor	0.8600
Max Drawdown	-194.28 pts
Largest Win	38.41 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	28	35.7%	+17.7	+0.63	+38.4	-27.1
NY	21	19.0%	-76.6	-3.65	+37.4	-14.7
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
09:00	1	100.0%	+6.6	+6.56
10:00	1	0.0%	-12.7	-12.67
11:00	8	25.0%	-17.1	-2.14
12:00	6	50.0%	+48.3	+8.04
13:00	4	50.0%	+29.9	+7.48
14:00	7	28.6%	-19.8	-2.83
15:00	1	0.0%	-17.5	-17.48
17:00	4	50.0%	+42.3	+10.58
18:00	5	0.0%	-60.1	-12.01
19:00	7	0.0%	-62.5	-8.92
20:00	5	40.0%	+3.6	+0.72
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	8	37.5%	+13.4	+1.68
Tuesday	13	15.4%	-82.9	-6.38
Wednesday	12	16.7%	-68.6	-5.72
Thursday	3	100.0%	+84.6	+28.20
Friday	13	30.8%	-5.5	-0.42

## report_20260324_a3451a370263_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	121
Win Rate	39.67%
Total P&L	+467.04 pts
Profit Factor	1.4900
Max Drawdown	-194.28 pts
Largest Win	47.79 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	66	39.4%	+241.4	+3.66	+47.4	-27.1
NY	55	40.0%	+225.6	+4.10	+47.8	-16.5
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	100.0%	+43.4	+43.37
09:00	5	20.0%	-53.1	-10.63
10:00	7	57.1%	+100.0	+14.28
11:00	14	28.6%	-5.6	-0.40
12:00	12	50.0%	+86.1	+7.18
13:00	12	33.3%	+17.6	+1.47
14:00	8	37.5%	+14.4	+1.80
15:00	7	42.9%	+38.7	+5.53
16:00	4	100.0%	+95.6	+23.89
17:00	9	33.3%	+28.1	+3.13
18:00	22	27.3%	-21.3	-0.97
19:00	13	38.5%	+51.9	+3.99
20:00	7	57.1%	+71.4	+10.20
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	30	36.7%	+62.8	+2.09
Tuesday	31	38.7%	+110.8	+3.57
Wednesday	22	27.3%	-32.2	-1.46
Thursday	12	83.3%	+269.1	+22.42
Friday	26	34.6%	+56.5	+2.17

## report_20260324_a3451a370263_full 
All underlying data. Click sections to expand.

Metric	Value
Total Trades	739
Win Rate	33.29%
Total P&L	+266.81 pts
Profit Factor	1.0500
Max Drawdown	-452.49 pts
Largest Win	47.79 pts
Largest Loss	-47.00 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
NY	380	34.5%	+214.9	+0.57	+47.8	-42.9
London	359	32.0%	+51.9	+0.14	+47.4	-47.0
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	10	20.0%	-18.0	-1.80
09:00	40	25.0%	-114.4	-2.86
10:00	47	31.9%	-36.7	-0.78
11:00	89	30.3%	+12.7	+0.14
12:00	64	37.5%	+98.1	+1.53
13:00	52	40.4%	+229.5	+4.41
14:00	24	25.0%	-76.2	-3.17
15:00	33	30.3%	-43.1	-1.31
16:00	52	48.1%	+240.6	+4.63
17:00	82	28.0%	-110.8	-1.35
18:00	107	25.2%	-219.7	-2.05
19:00	104	39.4%	+208.0	+2.00
20:00	35	42.9%	+96.8	+2.77
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	177	31.1%	-89.5	-0.51
Tuesday	172	30.8%	-62.9	-0.37
Wednesday	138	32.6%	+53.1	+0.38
Thursday	118	39.8%	+277.4	+2.35
Friday	134	34.3%	+88.7	+0.66

## report_20260324_65df7121489f_3M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	58
Win Rate	24.14%
Total P&L	-146.87 pts
Profit Factor	0.7200
Max Drawdown	-216.86 pts
Largest Win	38.21 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	26	26.9%	-51.1	-1.97	+38.2	-27.1
NY	32	21.9%	-95.7	-2.99	+33.0	-18.0
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	0.0%	-5.2	-5.19
09:00	1	100.0%	+7.5	+7.54
10:00	3	0.0%	-48.3	-16.10
11:00	6	33.3%	+13.9	+2.31
12:00	7	42.9%	+49.1	+7.01
13:00	3	33.3%	-5.5	-1.84
14:00	2	0.0%	-30.8	-15.39
15:00	3	0.0%	-31.8	-10.60
16:00	1	0.0%	-18.0	-18.00
17:00	7	28.6%	-3.1	-0.45
18:00	9	22.2%	-30.5	-3.38
19:00	11	9.1%	-66.0	-6.00
20:00	4	50.0%	+21.9	+5.47
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	11	45.5%	+79.0	+7.19
Tuesday	14	14.3%	-110.7	-7.91
Wednesday	17	11.8%	-118.0	-6.94
Thursday	3	66.7%	+44.9	+14.97
Friday	13	23.1%	-42.2	-3.24

## report_20260324_65df7121489f_6M
All underlying data. Click sections to expand.

Metric	Value
Total Trades	150
Win Rate	32.67%
Total P&L	+185.60 pts
Profit Factor	1.1400
Max Drawdown	-246.60 pts
Largest Win	48.91 pts
Largest Loss	-27.10 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	66	37.9%	+287.1	+4.35	+48.9	-27.1
NY	84	28.6%	-101.5	-1.21	+35.2	-18.5
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	1	0.0%	-5.2	-5.19
09:00	4	50.0%	+27.8	+6.95
10:00	10	50.0%	+122.0	+12.20
11:00	13	30.8%	+8.6	+0.66
12:00	15	46.7%	+122.8	+8.19
13:00	9	33.3%	-2.0	-0.22
14:00	4	25.0%	-8.5	-2.12
15:00	10	30.0%	+21.5	+2.15
16:00	6	50.0%	+42.4	+7.06
17:00	16	18.8%	-103.7	-6.48
18:00	30	30.0%	-41.7	-1.39
19:00	22	27.3%	+13.9	+0.63
20:00	10	30.0%	-12.3	-1.23
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	47	38.3%	+185.9	+3.95
Tuesday	35	31.4%	+16.9	+0.48
Wednesday	24	16.7%	-115.5	-4.81
Thursday	15	53.3%	+147.1	+9.80
Friday	29	27.6%	-48.7	-1.68

## report_20260324_65df7121489f_full
All underlying data. Click sections to expand.

Metric	Value
Total Trades	849
Win Rate	30.98%
Total P&L	-253.47 pts
Profit Factor	0.9600
Max Drawdown	-592.19 pts
Largest Win	48.91 pts
Largest Loss	-84.59 pts
Session	Trades	Win Rate	Total P&L	Avg P&L	Largest Win	Largest Loss
London	344	29.7%	-98.5	-0.29	+48.9	-56.0
NY	505	31.9%	-155.0	-0.31	+40.5	-84.6
Hour (UTC)	Trades	Win Rate	Total P&L	Avg P&L
08:00	7	14.3%	-36.9	-5.26
09:00	32	40.6%	+158.9	+4.97
10:00	58	31.0%	+32.7	+0.56
11:00	75	24.0%	-186.7	-2.49
12:00	75	30.7%	-35.9	-0.48
13:00	49	38.8%	+157.5	+3.21
14:00	19	15.8%	-94.1	-4.95
15:00	29	24.1%	-94.1	-3.24
16:00	63	42.9%	+244.8	+3.89
17:00	118	27.1%	-280.1	-2.37
18:00	141	24.8%	-366.2	-2.60
19:00	133	35.3%	+130.0	+0.98
20:00	50	40.0%	+116.5	+2.33
Day	Trades	Win Rate	Total P&L	Avg P&L
Monday	213	29.6%	-90.6	-0.43
Tuesday	181	29.8%	-267.1	-1.48
Wednesday	159	25.8%	-335.8	-2.11
Thursday	140	38.6%	+363.3	+2.59
Friday	156	32.7%	+76.7	+0.49

*Document generated: 2026-03-18*
Next review: after completing 5min & 10min series.
```

