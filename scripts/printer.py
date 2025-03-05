from datetime import datetime

class CustomPrinter:
    def __init__(self):
        self.original_print = print
    
    def custom_print(self, *args, **kwargs):
        timestamp = datetime.now().strftime("[%y-%m-%d %H:%M:%S]")
        self.original_print(timestamp, *args, **kwargs)
    
    def __enter__(self):
        import builtins
        builtins.print = self.custom_print
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        import builtins
        builtins.print = self.original_print