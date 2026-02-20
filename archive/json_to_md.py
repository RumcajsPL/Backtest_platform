import json
from pathlib import Path

INPUT_JSON = Path("output/reports/WBWS/execution_20251213_222929.json")
OUTPUT_MD = Path("output/reports/WBWS/execution_20251213_222929.md")


def pct(value):
    return f"{value:.2f}%"


with open(INPUT_JSON, "r", encoding="utf-8") as f:
    data = json.load(f)

md = []

# --------------------------------------------------
# Header
# --------------------------------------------------
meta = data["report_metadata"]
cfg = data["configuration"]

md.append(f"# WBWS Signal Report — {cfg['asset']['symbol']}")
md.append("")
md.append(f"**Configuration:** {cfg['name']}")
md.append(f"**Generated at:** {meta['generated_at']}")
md.append("")

# --------------------------------------------------
# Configuration
# --------------------------------------------------
asset = cfg["asset"]
md.append("## Configuration")
md.append("")
md.append("### Asset")
md.append(f"- **Symbol:** {asset['symbol']}")
md.append(f"- **Name:** {asset['name']}")
md.append(f"- **Exchange:** {asset['exchange']}")
md.append(f"- **Currency:** {asset['currency']}")
md.append("")

data_cfg = cfg["data"]
md.append("### Data Source")
md.append(f"- **File:** {data_cfg['file']}")
md.append(f"- **Timeframe:** {data_cfg['timeframe']}")
md.append(f"- **Format:** {data_cfg['format']}")
md.append("")

md.append(f"### Indicator")
md.append(f"- **Higher Timeframe:** {cfg['indicator']['htf_period']}")
md.append("")

# --------------------------------------------------
# Data preprocessing
# --------------------------------------------------
prep = data["data_preprocessing"]

md.append("## Data Preprocessing")
md.append("")
md.append(f"- **Rows:** {prep['rows']}")
md.append(f"- **Period:** {prep['period']['start']} → {prep['period']['end']}")
md.append("")
md.append("### Validation Steps")
for step in prep["preprocessing_steps"]:
    md.append(f"- {step}")
md.append("")

# --------------------------------------------------
# Execution summary
# --------------------------------------------------
exec_ = data["execution"]

md.append("## Execution Summary")
md.append("")
md.append(f"- **Total Bars:** {exec_['total_bars']}")
md.append(f"- **Buy Signals:** {exec_['signals']['buy']}")
md.append(f"- **Sell Signals:** {exec_['signals']['sell']}")
md.append(f"- **Total Signals:** {exec_['signals']['total']}")
md.append("")

# --------------------------------------------------
# Signal distribution table
# --------------------------------------------------
sig = data["signal_analysis"]

md.append("## Signal Distribution")
md.append("")
md.append("| Type | Count | Percentage |")
md.append("|------|------:|-----------:|")
md.append(f"| Buy | {sig['buy_signals']['count']} | {pct(sig['buy_signals']['percentage'])} |")
md.append(f"| Sell | {sig['sell_signals']['count']} | {pct(sig['sell_signals']['percentage'])} |")
md.append(f"| **Total** | **{sig['total_signals']['count']}** | **{pct(sig['total_signals']['percentage'])}** |")
md.append("")

# --------------------------------------------------
# Candle distribution
# --------------------------------------------------
candles = data["candle_distribution"]

md.append("## Candle Classification")
md.append("")
md.append(f"Classification rate: **{pct(candles['classification_rate'])}**")
md.append("")
md.append("| Type | Count | Percentage |")
md.append("|------|------:|-----------:|")

for k, v in candles["distribution"].items():
    name = k.replace("_", " ").title()
    md.append(f"| {name} | {v['count']} | {pct(v['percentage'])} |")

md.append("")

# --------------------------------------------------
# HTF analysis
# --------------------------------------------------
htf = data["htf_analysis"]

md.append("## HTF Bias (60min)")
md.append("")
md.append("| State | Count | Percentage |")
md.append("|-------|------:|-----------:|")
md.append(f"| Bullish | {htf['htf_bull_bars']['count']} | {pct(htf['htf_bull_bars']['percentage'])} |")
md.append(f"| Bearish | {htf['htf_bear_bars']['count']} | {pct(htf['htf_bear_bars']['percentage'])} |")
md.append(f"| Neutral | {htf['htf_neutral_bars']['count']} | {pct(htf['htf_neutral_bars']['percentage'])} |")
md.append("")

# --------------------------------------------------
# Reversal patterns
# --------------------------------------------------
rev = data["reversal_patterns"]

md.append("## Reversal Pattern Efficiency")
md.append("")
md.append("| Pattern | Count | Converted | Conversion Rate |")
md.append("|--------|------:|----------:|----------------:|")
md.append(
    f"| 2D → 2U | {rev['reversals_2d_to_2u']['count']} | "
    f"{rev['reversals_2d_to_2u']['converted_to_buy']} Buy | "
    f"{pct(rev['reversals_2d_to_2u']['conversion_rate'])} |"
)
md.append(
    f"| 2U → 2D | {rev['reversals_2u_to_2d']['count']} | "
    f"{rev['reversals_2u_to_2d']['converted_to_sell']} Sell | "
    f"{pct(rev['reversals_2u_to_2d']['conversion_rate'])} |"
)

# --------------------------------------------------
# Write file
# --------------------------------------------------
OUTPUT_MD.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_MD, "w", encoding="utf-8") as f:
    f.write("\n".join(md))

print(f"Markdown report generated: {OUTPUT_MD}")