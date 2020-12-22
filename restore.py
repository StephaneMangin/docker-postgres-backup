#!/usr/bin/python

import os
import subprocess
import sys
from datetime import datetime

BACKUP_DIR = os.environ["BACKUP_DIR"]
BACKUP_TYPE = os.environ["BACKUP_TYPE"]
BACKUP_PATH = os.environ["BACKUP_PATH"]
BACKUP_DBNAMES = os.environ["BACKUP_DBNAMES"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
DB_HOST = "localhost"
MAIL_TO = os.environ.get("MAIL_TO")
MAIL_FROM = os.environ.get("MAIL_FROM")


file_name = sys.argv[1]
backup_file = os.path.join(BACKUP_DIR, file_name)

if not BACKUP_PATH.endswith("/"):
    S3_PATH = BACKUP_PATH + "/"


def cmd(command):
    try:
        subprocess.check_output([command], shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.stderr.write("\n".join([
            "Command execution failed. Output:",
            "-"*80,
            e.output,
            "-"*80,
            ""
        ]))
        raise


def backup_exists():
    return os.path.exists(backup_file)


def restore_backup():
    if not backup_exists():
        sys.stderr.write("Backup file doesn't exists!\n")
        sys.exit(1)

    dbname = "_".join(file_name.split("_")[:-1])

    # restore postgres-backup
    cmd("env PGPASSWORD=%s pg_restore -Fc -h %s -U %s -d %s %s" % (
        POSTGRES_PASSWORD,
        DB_HOST, 
        POSTGRES_USER,
        dbname,
        backup_file,
    ))


def download_backup_s3():
    cmd("aws s3 cp %s%s %s" % (S3_PATH, file_name, backup_file))


def download_backup_minio():
    cmd("mc s3 cp %s%s %s" % (S3_PATH, file_name, backup_file))


def log(msg):
    print("[%s]: %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))


def main():
    start_time = datetime.now()
    if backup_exists():
        log("Backup file already exists in filesystem %s" % backup_file)
    else:
        log("Downloading database dump")
        if BACKUP_TYPE == 's3':
            download_backup_s3()
        elif BACKUP_TYPE == 'minio':
            download_backup_minio()

    log("Restoring database")
    restore_backup()

    log("Restore complete, took %.2f seconds" % (datetime.now() - start_time).total_seconds())


if __name__ == "__main__":
    main()
