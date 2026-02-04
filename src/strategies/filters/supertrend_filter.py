# src/strategies/filters/supertrend_filter.py
import pandas as pd
import pandas_ta_classic as pta
import logging

logger = logging.getLogger(__name__)

class SupertrendFilter:
    def __init__(self, atr_length: int = 10, factor: float = 3.0, enabled: bool = True):
        self.atr_length = int(atr_length)
        self.factor = float(factor)
        self.enabled = enabled
                
        if self.atr_length < 1:
            raise ValueError("ATR length >= 1")
        if self.factor <= 0:
            raise ValueError("Factor > 0")
    
    def apply_filter(self, df: pd.DataFrame, is_long: bool = True) -> pd.Series:
        if not self.enabled:
            return pd.Series(True, index=df.index)
        
        required = ['high', 'low', 'close']
        if not all(col in df.columns for col in required):
            logger.warning(f"Supertrend requires {required}")
            return pd.Series(False, index=df.index)
        
        st = pta.supertrend(high=df['high'], low=df['low'], close=df['close'], 
                            length=self.atr_length, multiplier=self.factor)
        
        # Build names matching library's str(multiplier) → '3.0'
        st_col = f"SUPERT_{self.atr_length}_{self.factor}"
        dir_col = f"SUPERTd_{self.atr_length}_{self.factor}"
        
        # Early check with actual columns
        if st.empty or st_col not in st.columns or dir_col not in st.columns:
            actual_cols = list(st.columns) if not st.empty else 'empty'
            logger.warning(
                f"Supertrend returned empty or missing columns "
                f"(expected st_col={st_col}, dir_col={dir_col}; actual={actual_cols})"
            )
            return pd.Series(False, index=df.index)
        
        # Optional debug (comment out after testing)
        # logger.debug(f"Supertrend columns: {actual_cols}")
        
        # Core filter logic using active SUPERT band
        if is_long:
            condition = (st[dir_col] == 1) & (df['close'] > st[st_col])
        else:
            condition = (st[dir_col] == -1) & (df['close'] < st[st_col])
        
        # NaN bars (early data) → reject
        condition = condition.fillna(False)
        
        # Align and return bool Series
        result = pd.Series(False, index=df.index)
        result[condition.index] = condition
        return result.astype(bool)