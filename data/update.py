import argparse
import sys

from data import update_cams_all, reset_gcis_by_term

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--term', help='RESET gcis database by term')

    args = parser.parse_args()

    print('[INFO] Update CAMS data...')
    update_cams_all()

    if args.term:
        term = args.term[:-4].upper() + ' ' + args.term[-4:]
        q = input(f"Are you sure to reset the schedules for {term} ?(y/N): ")
        if q in ['y', 'Y']:
            print(f'[INFO] Reset {term} schedules...')
            reset_gcis_by_term(term)
        else:
            print('Aborted!')
            sys.exit(0)
