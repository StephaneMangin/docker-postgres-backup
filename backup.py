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
WEBHOOK = os.environ.get("WEBHOOK")
WEBHOOK_METHOD = os.environ.get("WEBHOOK_METHOD") or "GET"
KEEP_BACKUP_DAYS = int(os.environ.get("KEEP_BACKUP_DAYS", 7))

dt = datetime.now()

if BACKUP_PATH and not BACKUP_PATH.endswith("/"):
    BACKUP_PATH = BACKUP_PATH + "/"


def cmd(command):
    try:
        subprocess.check_output([command], shell=True, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        sys.stderr.write("\n".join([
            "Command execution failed. Output:",
            "-" * 80,
            e.output,
            "-" * 80,
            ""
        ]))
        raise


def take_backup(dbname, backup_file):
    if os.path.exists(backup_file):
        sys.stderr.write("Backup file already exists!\n")
        sys.exit(1)

    # trigger postgres-backup
    cmd("env PGPASSWORD=%s pg_dump -Fc -h %s -U %s %s > %s" % (
        POSTGRES_PASSWORD, DB_HOST, POSTGRES_USER, dbname, backup_file
    ))


def upload_backup_minio(backup_file):
    cmd("mc cp %s %s" % (backup_file, BACKUP_PATH))


def upload_backup_s3(backup_file):
    cmd("aws s3 cp --storage-class=STANDARD_IA %s %s" % (backup_file, BACKUP_PATH))


def prune_local_backup_files():
    cmd("find %s -type f -prune -mtime +%i -exec rm -f {} \\;" % (BACKUP_DIR, KEEP_BACKUP_DAYS))


def send_email_s3(to_address, from_address, subject, body):
    """
    Super simple, doesn't do any escaping
    """
    cmd(
        """aws --region us-east-1 ses send-email 
        --from %(from)s 
        --destination 
        '{"ToAddresses":["%(to)s"]}' 
        --message '{
            "Subject":{
                "Data":"%(subject)s",
                "Charset":"UTF-8"
            },
            "Body":{
                "Text":{
                    "Data":"%(body)s",
                    "Charset":"UTF-8"
                }
            }
        }'""" % {
            "to": to_address,
            "from": from_address,
            "subject": subject,
            "body": body,
        })


def log(msg):
    print("[%s]: %s" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg))


def main():
    start_time = datetime.now()
    log("Dumping databases...")
    if not BACKUP_PATH:
        log("No path defined ! Nothing to do.")
        return

    for dbname in BACKUP_DBNAMES.split(","):

        file_name = dbname + "_" + dt.strftime("%Y-%m-%d")
        backup_file = os.path.join(BACKUP_DIR, file_name)

        log("Dumping database %s" % dbname)
        take_backup()
        log("Uploading to S3 from %s" % backup_file)
        upload_backup_s3()
        log("Pruning local backup copies")
        prune_local_backup_files()

        if MAIL_TO and MAIL_FROM:
            log("Sending mail to %s" % MAIL_TO)
            send_email_s3(
                MAIL_TO,
                MAIL_FROM,
                "Backup complete: %s" % dbname,
                "Took %.2f seconds" % (datetime.now() - start_time).total_seconds(),
            )

        if WEBHOOK:
            log("Making HTTP %s request to webhook: %s" % (WEBHOOK_METHOD, WEBHOOK))
            cmd("curl -X %s %s" % (WEBHOOK_METHOD, WEBHOOK))

    log("Backup complete, took %.2f seconds" % (datetime.now() - start_time).total_seconds())


if __name__ == "__main__":
    main()
