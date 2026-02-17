"""
Display Engine Module
Handles color coding, formatting, and layout
"""
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    END = '\033[0m'

class DisplayEngine:
    def __init__(self):
        self.colors = Colors
        
    def color_text(self, text: str, color: str) -> str:
        """Apply color to text"""
        return f"{color}{text}{self.colors.END}"
    
    def print_header(self, title: str, width: int = 80):
        """Print a section header"""
        print(f"\n{self.color_text(title, self.colors.BOLD + self.colors.CYAN)}")
        print("=" * width)
    
    def print_section(self, title: str, width: int = 60):
        """Print a subsection"""
        print(f"\n{self.color_text(title, self.colors.BOLD + self.colors.BLUE)}")
        print("-" * width)
    
    def print_metric(self, label: str, value: str, color: str = ""):
        """Print a metric with optional color"""
        if color:
            print(f"{label:<30} {color}{value}{self.colors.END}")
        else:
            print(f"{label:<30} {value}")
    
    def print_table(self, headers: list, rows: list, col_widths: list = None):
        """Print a formatted table"""
        if col_widths is None:
            col_widths = [20] + [15] * (len(headers) - 1)
        
        # Print headers with proper spacing
        header_line = ""
        for i, header in enumerate(headers):
            if i < len(col_widths):
                header_line += f"{self.color_text(header, self.colors.BOLD):<{col_widths[i]}}"
        print(header_line)
        
        # Print separator
        print("-" * sum(col_widths))
        
        # Print rows
        for row in rows:
            row_line = ""
            for i, cell in enumerate(row):
                if i < len(col_widths):
                    row_line += f"{str(cell):<{col_widths[i]}}"
            print(row_line)
    
    def format_percentage(self, value: float, decimals: int = 1) -> str:
        """Format percentage with color based on value"""
        formatted = f"{value:.{decimals}f}%"
        
        if value > 0:
            return self.color_text(formatted, self.colors.GREEN)
        elif value < 0:
            return self.color_text(formatted, self.colors.RED)
        else:
            return formatted
    
    def format_pnl(self, value: float, decimals: int = 2) -> str:
        """Format P&L with color"""
        formatted = f"{value:+,.{decimals}f}"
        
        if value > 0:
            return self.color_text(formatted, self.colors.GREEN)
        elif value < 0:
            return self.color_text(formatted, self.colors.RED)
        else:
            return formatted
    
    def get_emoji_indicator(self, condition: bool, true_emoji: str = "✅", false_emoji: str = "❌") -> str:
        """Get emoji indicator based on condition"""
        return true_emoji if condition else false_emoji