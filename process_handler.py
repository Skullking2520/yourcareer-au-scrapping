# process_handler.py
import json
import signal
import sys

class ProcessHandler:
    def __init__(self, progress_sheet, init_value, position, shutdown_callback=None):
        self.progress_sheet = progress_sheet
        self.position = position
        self.init_value = init_value
        self.shutdown_callback = shutdown_callback
        self.progress = self.load_progress()
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def load_progress(self):
        try:
            progress_json = self.progress_sheet.acell(self.position).value
            if not progress_json:
                progress = self.init_value
            else:
                progress = json.loads(progress_json)
            return progress
        except Exception:
            print("Failed to load progress, finishing program")
            return {"finished": True}

    def save_progress(self, progress):
        try:
            self.progress_sheet.update(self.position, [[json.dumps(progress)]])
        except Exception:
            print("Failed to save progress.")

    def signal_handler(self, signum, frame):
        print(f"Signal {signum} occurred! Saving before shutdown...")
        self.save_progress(self.progress)
        if self.shutdown_callback:
            self.shutdown_callback()
        sys.exit(0)