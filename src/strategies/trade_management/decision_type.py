from enum import Enum, auto

class DecisionType(Enum):
    NONE = auto()
    OPEN = auto()
    CLOSE = auto()
    REVERSE = auto()
    MODIFY = auto()   # reserved for future use