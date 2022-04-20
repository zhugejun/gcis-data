import sys

from data import reset_database


if __name__ == "__main__":

    q = input("Are you sure to reset the database?(y/N): ")
    if q in ["y", "Y"]:
        print("Resetting the database...")
        reset_database()
    else:
        print("Aborted!")
        sys.exit(0)
