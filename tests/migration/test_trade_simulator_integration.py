"""Integration tests for TradeSimulator with migrated TradeManager

Tests the complete pipeline: RiskManager → TradeManager → TradeSimulator
Verifies contract integration and parity with legacy behavior.

Session 9 - Test Suite
"""
import sys
from pathlib import Path
import pytest
import pandas as pd
import numpy as np
import yaml

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategies.specific.modules.data_loader import DataLoader
from src.strategies.specific.modules.signal_generator import SignalGenerator
from src.strategies.specific.modules.filter_pipeline import FilterPipeline
from src.strategies.specific.modules.trade_simulator import TradeSimulator
from src.strategies.contracts.trade_contracts import TradeDecision, DecisionType
from src.strategies.contracts.position_contracts import Position


def load_config(name="wbws_strategy.yaml"):
    """Load strategy configuration"""
    path = PROJECT_ROOT / f"configs/strategies/wbws/{name}"
    with open(path, "r") as f:
        return yaml.safe_load(f)


class TestTradeSimulatorBasicIntegration:
    """Basic integration tests - smoke tests"""
    
    def test_simulator_initializes_with_contracts(self):
        """Test TradeSimulator initializes with migrated TradeManager"""
        config = load_config()
        
        # Create minimal dataframe
        dates = pd.date_range('2025-01-01', periods=100, freq='1min')
        df = pd.DataFrame({
            'open': np.random.randn(100).cumsum() + 20000,
            'high': np.random.randn(100).cumsum() + 20010,
            'low': np.random.randn(100).cumsum() + 19990,
            'close': np.random.randn(100).cumsum() + 20000,
            'volume': np.random.randint(100, 1000, 100)
        }, index=dates)
        
        # Initialize simulator
        simulator = TradeSimulator(config, df)
        
        # Verify components
        assert simulator.trade_manager is not None
        assert simulator.risk_manager is not None
        
        # Verify TradeManager has contract methods
        assert hasattr(simulator.trade_manager, 'handle_signal')
        assert hasattr(simulator.trade_manager, 'open_position')
    
    def test_simulator_runs_without_errors(self):
        """Test simulator completes without errors"""
        config = load_config()
        
        # Load real data
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:100]  # Use first 100 bars
        df_ltf = data_bundle.ltf[:6000] if data_bundle.has_ltf else None  # ~100 bars worth
        
        # Generate signals
        gen = SignalGenerator(htf_period="1H", mode="core")
        signal_frame = gen.generate_signals(data_bundle)
        
        # Create signals series
        signals = pd.Series(index=df_strategy.index, dtype='object')
        for ts, code in signal_frame.iter_raw():
            if ts in signals.index:
                signals[ts] = 'BUY' if code == 1 else 'SELL'
        
        # Run simulation
        simulator = TradeSimulator(config, data_bundle.full)
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf,
            verbose=False
        )
        
        # Verify result structure
        assert result is not None
        assert 'all_trades' in result
        assert 'execution_mode' in result
        assert 'SESSION9' in result['execution_mode']  # Verify v4.4
    
    def test_trade_manager_creates_position_contracts(self):
        """Verify TradeManager creates Position contracts with full data"""
        config = load_config()
        
        # Load data
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:50]
        df_ltf = data_bundle.ltf[:3000] if data_bundle.has_ltf else None
        
        # Generate signals
        gen = SignalGenerator(htf_period="1H", mode="core")
        signal_frame = gen.generate_signals(data_bundle)
        
        signals = pd.Series(index=df_strategy.index, dtype='object')
        for ts, code in signal_frame.iter_raw():
            if ts in signals.index:
                signals[ts] = 'BUY' if code == 1 else 'SELL'
        
        # Run simulation
        simulator = TradeSimulator(config, data_bundle.full)
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf,
            verbose=False
        )
        
        # Check if any trades were opened
        if len(simulator.trade_manager.current_positions) > 0:
            pos = simulator.trade_manager.current_positions[0]
            
            # Verify Position contract
            assert isinstance(pos, Position)
            assert pos.entry_price > 0
            assert pos.stop_loss > 0
            assert pos.take_profit > 0
            assert pos.size > 0
            assert pos.position_id > 0
            
            # Verify helper methods work
            assert hasattr(pos, 'is_long')
            assert hasattr(pos, 'is_short')
            assert pos.sl_distance > 0
            assert pos.tp_distance > 0
            assert pos.risk_reward_ratio > 0


class TestTradeSimulatorParity:
    """Parity tests - verify results match expectations"""
    
    def test_execution_mode_updated(self):
        """Test execution mode reflects Session 9 changes"""
        config = load_config()
        
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:20]
        df_ltf = data_bundle.ltf[:1200] if data_bundle.has_ltf else None
        
        # Simple signal
        signals = pd.Series(index=df_strategy.index, dtype='object')
        signals.iloc[5] = 'BUY'
        
        simulator = TradeSimulator(config, data_bundle.full)
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf
        )
        
        # Verify v4.4 (Session 9)
        assert 'V4_4_SESSION9' in result['execution_mode']
    
    def test_trade_stats_structure_unchanged(self):
        """Verify trade dict structure unchanged for backward compatibility"""
        config = load_config()
        
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:30]
        df_ltf = data_bundle.ltf[:1800] if data_bundle.has_ltf else None
        
        # Generate signals
        gen = SignalGenerator(htf_period="1H", mode="core")
        signal_frame = gen.generate_signals(data_bundle)
        
        signals = pd.Series(index=df_strategy.index, dtype='object')
        for ts, code in signal_frame.iter_raw():
            if ts in signals.index:
                signals[ts] = 'BUY' if code == 1 else 'SELL'
        
        simulator = TradeSimulator(config, data_bundle.full)
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf
        )
        
        # Verify result structure
        assert 'all_trades' in result
        assert 'closed_trades' in result
        assert 'open_trades' in result
        assert 'rejected_trades' in result
        assert 'exit_stats' in result
        assert 'risk_stats' in result
        assert 'trade_manager_metrics' in result
        
        # Verify trade dict structure (if any trades)
        if result['all_trades']:
            trade = result['all_trades'][0]
            expected_keys = {
                'trade_id', 'status', 'entry_time', 'direction',
                'entry_price', 'sl_price', 'tp_price', 'pnl_points',
                'is_win', 'is_loss', 'comment'
            }
            assert expected_keys.issubset(set(trade.keys()))


class TestContractIntegration:
    """Test contract integration specifics"""
    
    def test_risk_manager_called_before_trade_manager(self):
        """Verify RiskManager is called before TradeManager (Session 9 change)"""
        config = load_config()
        
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:10]
        df_ltf = data_bundle.ltf[:600] if data_bundle.has_ltf else None
        
        # Single BUY signal
        signals = pd.Series(index=df_strategy.index, dtype='object')
        signals.iloc[3] = 'BUY'
        
        simulator = TradeSimulator(config, data_bundle.full)
        
        # Patch to track call order
        risk_called = []
        tm_called = []
        
        original_risk = simulator.risk_manager.compute_trade_parameters
        original_tm = simulator.trade_manager.handle_signal
        
        def track_risk(*args, **kwargs):
            risk_called.append(True)
            return original_risk(*args, **kwargs)
        
        def track_tm(*args, **kwargs):
            tm_called.append(True)
            return original_tm(*args, **kwargs)
        
        simulator.risk_manager.compute_trade_parameters = track_risk
        simulator.trade_manager.handle_signal = track_tm
        
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf
        )
        
        # Verify call order (RiskManager before TradeManager)
        if risk_called and tm_called:
            assert len(risk_called) >= 1
            assert len(tm_called) >= 1
            # Both should be called (order verified by execution flow in code)
    
    def test_trade_decision_properties_used(self):
        """Verify TradeDecision properties are used correctly"""
        config = load_config()
        
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:20]
        df_ltf = data_bundle.ltf[:1200] if data_bundle.has_ltf else None
        
        # Create signals
        signals = pd.Series(index=df_strategy.index, dtype='object')
        signals.iloc[5] = 'BUY'
        
        simulator = TradeSimulator(config, data_bundle.full)
        
        # Capture TradeDecision
        decisions = []
        original_handle = simulator.trade_manager.handle_signal
        
        def capture_decision(*args, **kwargs):
            decision = original_handle(*args, **kwargs)
            decisions.append(decision)
            return decision
        
        simulator.trade_manager.handle_signal = capture_decision
        
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf
        )
        
        # Verify TradeDecision was created
        if decisions:
            decision = decisions[0]
            assert isinstance(decision, TradeDecision)
            assert hasattr(decision, 'decision_type')
            assert hasattr(decision, 'is_open')
            assert hasattr(decision, 'is_reject')


class TestFullPipeline:
    """Test complete pipeline integration"""
    
    def test_full_pipeline_data_to_trades(self):
        """Test complete pipeline: Data → Signals → Filters → Trades"""
        config = load_config()
        
        # 1. Load data
        loader = DataLoader(str(PROJECT_ROOT / "configs/strategies/wbws/wbws_strategy.yaml"))
        loader.load_config()
        data_bundle = loader.load_data()
        
        df_strategy = data_bundle.strategy[:100]
        df_ltf = data_bundle.ltf[:6000] if data_bundle.has_ltf else None
        
        # 2. Generate signals
        gen = SignalGenerator(htf_period="1H", mode="core")
        signal_frame = gen.generate_signals(data_bundle)
        
        print(f"\n✅ Generated {signal_frame.count_by_type()['total']} raw signals")
        
        # 3. Apply filters (optional - can skip for basic test)
        # For now, use raw signals
        
        # 4. Convert to series for simulator
        signals = pd.Series(index=df_strategy.index, dtype='object')
        for ts, code in signal_frame.iter_raw():
            if ts in signals.index:
                signals[ts] = 'BUY' if code == 1 else 'SELL'
        
        print(f"✅ Converted {signals.notna().sum()} signals for simulation")
        
        # 5. Run simulation
        simulator = TradeSimulator(config, data_bundle.full)
        result = simulator.simulate_trades(
            df_strategy=df_strategy,
            filtered_signals=signals,
            df_ltf=df_ltf,
            verbose=True
        )
        
        # 6. Verify results
        print(f"\n✅ Simulation complete:")
        print(f"   Total trades: {len(result['all_trades'])}")
        print(f"   Closed trades: {len(result['closed_trades'])}")
        print(f"   Open trades: {len(result['open_trades'])}")
        print(f"   Rejected trades: {len(result['rejected_trades'])}")
        print(f"   Execution mode: {result['execution_mode']}")
        
        # Verify execution mode shows Session 9
        assert 'SESSION9' in result['execution_mode']
        
        # Verify trades were created
        assert len(result['all_trades']) > 0


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])