import argparse
import sys
import logging

from data import update_cams_all, append_schedules_by_term

logging.basicConfig(
    filename="gcis.log", level=logging.INFO, format="%(asctime)-15s %(message)s"
)
logger = logging.getLogger()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--term", help="append new schedules by term with format, e.g., Spring2022"
    )

    args = parser.parse_args()

    if args.term:
        term = args.term[:-4].upper() + " " + args.term[-4:]
        q = input(f"Are you sure to append new schedules for {term}?(y/N): ")
        if q in ["y", "Y"]:
            logger.info(f"[INFO] Appending {term} schedules...")
            append_schedules_by_term(term)
        else:
            logger.info("Aborted!")
            sys.exit(0)
    else:
        logger.info("[INFO] Update CAMS data...")
        update_cams_all()
