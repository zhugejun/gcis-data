import sys
import argparse
import logging

from data import reset_database, reset_gcis_by_term


logging.basicConfig(
    filename="gcis.log", level=logging.INFO, format="%(asctime)-15s %(message)s"
)
logger = logging.getLogger()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t", "--term", help="RESET gcis database by term with format, e.g., Spring2022"
    )

    args = parser.parse_args()

    if args.term:
        term = args.term[:-4].upper() + " " + args.term[-4:]
        q = input(f"Are you sure to reset the schedules for {term}?(y/N): ")
        if q in ["y", "Y"]:
            logger.info(f"[INFO] Reset {term} schedules...")
            reset_gcis_by_term(term)
        else:
            logger.info("Aborted!")
            sys.exit(0)
    else:
        q = input("Are you sure to reset the database?(y/N): ")
        if q in ["y", "Y"]:
            print("Resetting the database...")
            reset_database()
        else:
            print("Aborted!")
            sys.exit(0)
