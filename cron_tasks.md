# Cron Tasks Configuration
# Format: minute hour day month weekday command
*/5 * * * * curl -s http://localhost:5000/health
0 * * * * tar -czf backup/state_$(date +%Y%m%d_%H%M).tar.gz state/
0 9 * * * python generate_report.py
