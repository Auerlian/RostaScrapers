import subprocess
import sys

scripts = [
    "scrape_pasta_evangelists.py",
    "scrape_comptoir_bakery.py",
    "scrape_caravan_coffee_school.py",
]

for script in scripts:
    print(f"Running {script}...")
    result = subprocess.run([sys.executable, script], check=False)
    if result.returncode != 0:
        print(f"{script} failed with code {result.returncode}")