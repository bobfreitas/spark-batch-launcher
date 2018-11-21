# spark-batch-launcher
Python wrapper to launch Spark batch jobs

This can be used to launch a set of Spark batch jobs on a regular interval.
It would be run via crontab with something like:

*/30 	* 	*	* 	*  /home/hadoop/spark-hourly/bin/run_hourly.py "EMAIL_TO" >> /home/hadoop/spark-hourly/logs/run_hourly-$(date +\%Y-\%m-\%d:\%H:\%M:\%S)-log.txt 2>&1
