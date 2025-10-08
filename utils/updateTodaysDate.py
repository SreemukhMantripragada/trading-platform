from datetime import datetime
import pytz
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')

# Get today's date in Asia/Kolkata timezone in YYYYMMDD format
todays_date = datetime.now(tz=IST_TIMEZONE).strftime('%Y%m%d')

# Path to the .env file
env_path = 'infra/.env'

# Read existing lines
with open(env_path, 'r') as file:
    lines = file.readlines()

# Update or add the TODAYS_DATE variable
found = False
for i, line in enumerate(lines):
    if line.startswith('TODAYS_DATE='):
        lines[i] = f'TODAYS_DATE={todays_date}\n'
        found = True
        break
if not found:
    lines.append(f'TODAYS_DATE={todays_date}\n')

# Write back to the file
with open(env_path, 'w') as file:
    file.writelines(lines)