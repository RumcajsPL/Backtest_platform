# tests/analyze_execution_impact.py
import pandas as pd
import numpy as np

def analyze_trade_sensitivity(trades_df):
    """Analyze how many trades are sensitive to execution prices"""
    
    sensitive_trades = []
    
    for i, trade in trades_df.iterrows():
        current_pnl = trade['pnl_points']
        
        # Simulate realistic execution impact
        # SL: 0.5-2.0 pts worse, TP: 0-1 pt better
        if trade['exit_reason'] == 'STOP_LOSS':
            execution_impact = -np.random.uniform(0.5, 2.0)
        elif trade['exit_reason'] == 'TAKE_PROFIT':
            execution_impact = np.random.uniform(0, 1.0)
        else:
            execution_impact = -np.random.uniform(0.3, 1.0)
        
        new_pnl = current_pnl + execution_impact
        
        # Check if win/loss status changes
        current_is_win = current_pnl > 0
        new_is_win = new_pnl > 0
        
        if current_is_win != new_is_win:
            sensitive_trades.append({
                'trade_id': trade.get('trade_id', i),
                'current_pnl': current_pnl,
                'new_pnl': new_pnl,
                'execution_impact': execution_impact,
                'exit_reason': trade.get('exit_reason'),
                'direction': trade.get('direction')
            })
    
    print(f"Total trades: {len(trades_df)}")
    print(f"Sensitive trades (win/loss could flip): {len(sensitive_trades)}")
    print(f"Percentage: {len(sensitive_trades)/len(trades_df)*100:.1f}%")
    
    if sensitive_trades:
        print("\nSample sensitive trades:")
        for t in sensitive_trades[:5]:
            print(f"  Trade {t['trade_id']}: {t['current_pnl']:+.2f} → {t['new_pnl']:+.2f} ({t['execution_impact']:+.2f})")
    
    return sensitive_trades

# Load your trades CSV
trades_df = pd.read_csv('outputs\signals\progressive\signals_progressive_20260107_220354.csv', 
                       parse_dates=['entry_time', 'exit_time'])
sensitive = analyze_trade_sensitivity(trades_df)