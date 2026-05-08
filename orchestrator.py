import schedule
import time
import subprocess
import sys
from datetime import datetime

def run_job():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting COVID-19 Data Pipeline...")
    
    process = subprocess.run(
        [sys.executable, "scripts/process_covid_data.py"],
        capture_output=True,
        text=True
    )
    
    if process.returncode == 0:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Pipeline completed successfully.")
    else:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Pipeline failed. Error:")
        print(process.stderr)

# Schedule to run every 1 minute
schedule.every(1).minutes.do(run_job)

if __name__ == "__main__":
    print("Starting orchestrator...")
    run_job() 
    
    print("Running in background. Press Ctrl+C to exit.")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping orchestrator.")