#!/usr/bin/python -u

import sys
import os
import subprocess
import pytz
import smtplib, string
import time
from datetime import datetime, timedelta
import pytz

PIDFILE="/home/hadoop/run_hourly.pid"
SMTP_SERVER = "email-smtp.us-west-2.amazonaws.com:587"
FROM_ADDR = "myemail@somedomain.com"
USER_NAME = "SUBSTITUTE_SMTP_USERNAME"
PASSWORD = "SUBSTITUTE_SMTP_PASSWORD"
GENERAL_SUBJECT = "Spark hourly job failed"
toaddr = FROM_ADDR

timezone = pytz.timezone("US/Pacific")

def log(msg):
    dt = datetime.now(timezone)
    print "{} {}".format(str(dt), msg)

def send_email(toaddr, subject, msg):
    body = string.join((
        "From: %s" % FROM_ADDR,
        "To: %s" % toaddr,
        "Subject: %s" % subject ,
        "",
        msg
        ), "\r\n")
    server = smtplib.SMTP(SMTP_SERVER)
    server.starttls()
    server.login(USER_NAME, PASSWORD)
    server.sendmail(FROM_ADDR, toaddr, body)
    server.quit()


def pid_is_running(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return
    else:
        return pid


def write_pidfile_or_die(pidfile, toaddr):
    if os.path.exists(pidfile):
        pid = int(open(pidfile).read())
        if pid_is_running(pid):
            msg = "ERROR: Found existing pidfile! Process {0} is still running.".format(pid)
            log(msg)
            send_email(toaddr, GENERAL_SUBJECT, msg)
            sys.exit(1)
        else:
            os.remove(pidfile)
    open(pidfile, 'w').write(str(os.getpid()))
    return pidfile


def spark_cmd(job_name, arg1, arg2):
    cmd=("/usr/bin/spark-submit "
    "--class com.somedomain.spark.hourly.HourlyJob "
    "--name HourlyJob-{} "
    "--master yarn "
    "--deploy-mode cluster "
    "/home/hadoop/spark-hourly/lib/spark-hourly-1.0.0-bundle.jar "
    "-arg1={} "
    "-arg2={} "
    ).format(job_name, arg1, arg2)
    return cmd


def main():
    log("Starting run_hourly.py")
    # run Spark as separate jobs to increase parallelism
    dt = datetime.now(timezone)
    cmd1 = spark_cmd("job1", "arg1", "arg2")
    cmd2 = spark_cmd("job2", "arg1", "arg2")
    # add as many jobs as needed
    log("Spark command1: " + cmd1)
    log("Spark command2: " + cmd2)
    env = dict(os.environ)
    processes = []
    p = subprocess.Popen(cmd1, shell=True, env=env)
    processes.append(p)
    p = subprocess.Popen(cmd2, shell=True, env=env)
    processes.append(p)
    # add as many jobs as needed
    exitcodes = [p.wait() for p in processes]
    overallStatus = 0
    for code in exitcodes:
        if code != 0:
            overallStatus += 1
            msg = "ERROR: Spark returned non-zero error code: " + str(code)
            log(msg)
            if overallStatus == 1:
                send_email(toaddr, GENERAL_SUBJECT, msg)
    log("Finished run_hourly.py")
    sys.exit(overallStatus)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: run_hourly.py <to email>\n')
        sys.exit(1)
    s3_bucket = sys.argv[1]
    toaddr = sys.argv[2]
    write_pidfile_or_die(PIDFILE, toaddr)
    status = main()
    os.remove(PIDFILE)
    sys.exit(status)


