YES_DATE=$(date --date=' 1 days ago' '+%Y-%m-%d')
BEFORE_YES_DATE=$(date --date=' 2 days ago' '+%Y-%m-%d')
python metrica_logs_api.py -counter_id 68971321 -source hits -mode regular -start_date BEFORE_YES_DATE -end_date YES_DATE
