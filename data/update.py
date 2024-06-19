import argparse
import sys
import logging

from data import update_cams_all, append_schedules_by_term, delete_schedules_by_term

logging.basicConfig(
    filename="gcis.log", level=logging.INFO, format="%(asctime)-15s %(message)s"
)
logger = logging.getLogger()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-a", "--add", help="append new schedules by term with format, e.g., Spring2022"
    )
    parser.add_argument(
        "-d", "--delete", help="delete old schedules by term with format, e.g., Fall2022"
    )
    args = parser.parse_args()

    if args.add:
        term = args.add[:-4].upper() + " " + args.add[-4:]

        q1 = input(f"Have you added {term} to the app yet? (y/N)")
        if q1 not in "yY":
            logger.info("Aborted!")
            sys.exit(0)

        q2 = input(f"Have you updated term list in queries.py?(y/N)")
        if q2 not in 'yY':
            logger.info("Aborted!")
            sys.exit(0)
            
        
        q3 = input(f"Are you sure to append new schedules for {term}?(y/N): ")
        if q3 in "yY":
            logger.info(f"[INFO] Appending {term} schedules...")
            append_schedules_by_term(term)
        else:
            logger.info("Aborted!")
            sys.exit(0)
    elif args.delete:
        term = args.delete[:-4].upper() + " " + args.delete[-4:]
        q = input(f"Are you sure to delete old schedules for {term}?(y/N): ")
        if q in "yY":
            logger.info(f"[INFO] Deleting {term} schedules...")
            delete_schedules_by_term(term)
        else:
            logger.info("Aborted!")
            sys.exit(0)
    else:
        logger.info("[INFO] Update CAMS data...")
        print('********* Make sure ou see [INFO] Done! **********')
        update_cams_all()
