from src.strategies.config.config_schema import StrategyConfig
from src.strategies.orchestrator import StrategyOrchestrator
from src.strategies.core.cache_manager import CacheManager
from pathlib import Path

config = StrategyConfig.from_yaml(Path('configs/strategies/strategy_template.yaml'))
cm = CacheManager()
result = StrategyOrchestrator(config, cache_manager=cm).run(mode_override='core')

trades = result.trade_result
print('trade_result type:', type(trades))
print('trade_result attrs:', [a for a in dir(trades) if not a.startswith('_')])

if hasattr(trades, 'trades'):
    trades = trades.trades

print('trade list length:', len(trades) if trades else 0)

if trades:
    t = trades[0]
    print('trade type:', type(t))
    print('trade attrs:', [a for a in dir(t) if not a.startswith('_')])
    if hasattr(t, '__dict__'):
        print('trade dict:', t.__dict__)
    elif hasattr(t, '_asdict'):
        print('trade asdict:', t._asdict())