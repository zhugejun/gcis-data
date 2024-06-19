"""load data to GCIS"""
import datetime
import logging
import pandas as pd

from queries import SCHEDULE_QUERY, TERM_QUERY
from queries import CAMPUS_QUERY, LOCATION_QUERY, COURSE_QUERY, INSTRUCTOR_QUERY
from queries import generate_schedule_query_by_term_ids
from helper import get_data_from_cams, get_data_from_db, run_query_from_db
from helper import insert_data_into_db, df_to_sql, is_table_existing, delete_table

logging.basicConfig(
    filename="gcis.log", level=logging.INFO, format="%(asctime)-15s %(message)s"
)
logger = logging.getLogger()


# no dependent foreignkeys
def load_terms_from_cams(restart=True, tbl_name="scheduling_term"):
    logger.info("[INFO] Loading term data...")
    terms = get_data_from_cams(TERM_QUERY)
    query = df_to_sql(
        df=terms,
        col_names=["year", "semester", "active"],
        col_types=["num", "str", "str"],
        tbl_name=tbl_name,
        tbl_cols=["year", "semester", "active"],
    )
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_campuses_from_cams(restart=True, tbl_name="scheduling_campus"):
    logger.info("[INFO] Loading campus data...")
    campuses = get_data_from_cams(CAMPUS_QUERY)
    query = df_to_sql(
        df=campuses,
        col_names=["name"],
        col_types=["str"],
        tbl_name=tbl_name,
        tbl_cols=["name"],
    )
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_locations_from_cams(restart=True, tbl_name="scheduling_location"):
    logger.info("[INFO] Loading location data...")
    locations = get_data_from_cams(LOCATION_QUERY)
    query = df_to_sql(
        df=locations,
        col_names=["building", "room"],
        col_types=["str", "str"],
        tbl_name=tbl_name,
        tbl_cols=["building", "room"],
    )
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_instructors_from_cams(restart=True, tbl_name="scheduling_instructor"):
    logger.info("[INFO] Loading instructor data...")
    instructors = get_data_from_cams(INSTRUCTOR_QUERY)

    # added by Nancy, these are success coaches
    # !!!!! last name, first name !!!!!!
    success_coaches = pd.DataFrame(
        [
            # ["RATHFON", "BECKI"],
            ["DIAZ", "LEWANDA"],      # verified 03/08/2024
            # ["TOVAR", "CASSANDRA"],
            # ["REESE", "MARLINA"],
            # ["DEREK", "DEYONGE"],
            # ["DEEN", "LINDSAY"],
            ["RANGEL", "CATHLEEN"],    # verified 03/08/2024
            # ["SULLIVAN", "ZACHARY"],
            ["MARCOM", "KEILAH"],      # verified 03/08/2024
            ["RANDOLPH", "JAYCE"],     # verified 03/08/2024
            # ["CASTON", "ALEX"], 
            ["WOODEN", "BRANDY"],      # verified 03/08/2024
            ["MARTIN", "ALIVIA"],      # verified 03/08/2024
            ["LANGFORD", "RACHEL"],    # verified 03/08/2024
            # ["ZALEWSKI", "CHANTAL"],
            # ["MACKENZIE", "KATHERINE"],
            ["MONGE", "ALBERT"],       # added 03/08/2024
        ],
        columns=["last_name", "first_name"],
    )

    # added by Debbie, these are non-GC faculty
    non_gc_faculty = pd.DataFrame(
        [
            ["BALLINGER", "JAMES"],
            ["WOLFE", "JOLE"],
            ["CUMMINGS", "RACHEL"],
            ["WOOD", "AMANDA"],
            ["THOMPSON", "KATE"],
        ],
        columns=["last_name", "first_name"],
    )

    instructors = pd.concat([instructors, success_coaches, non_gc_faculty])
    instructors.sort_values("last_name", inplace=True)
    instructors.drop_duplicates(inplace=True)
    instructors.reset_index(drop=True, inplace=True)

    query = df_to_sql(
        df=instructors,
        col_names=["first_name", "last_name"],
        col_types=["str", "str"],
        tbl_name=tbl_name,
        tbl_cols=["first_name", "last_name"],
    )
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreign keys
def load_courses_from_cams(restart=True, tbl_name="scheduling_course"):
    logger.info("[INFO] Loading course data...")

    courses = get_data_from_cams(COURSE_QUERY)
    courses.drop_duplicates(inplace=True)  # drop duplicates
    courses.reset_index(drop=True, inplace=True)
    courses = courses[["subject", "number", "credit", "name"]]
    query = df_to_sql(
        df=courses,
        col_names=["subject", "number", "credit", "name"],
        col_types=["str", "str", "num", "str"],
        tbl_name=tbl_name,
        tbl_cols=["subject", "number", "credit", "name"],
    )

    if restart and not is_table_existing(tbl_name):
        restart = False

    if query:
        delete_table(tbl_name)
        insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


def generate_query_for_schedules_from_cams(query=SCHEDULE_QUERY, for_cams=False):
    """generate sql query based on schedules from CAMS

    Args:
        query (str, optional): SQL query to pull data from CAMS. Defaults to SCHEDULE_QUERY.
        for_cams (bool, optional): whether the schedules are for CAMS table on the server. Defaults to False.

    Returns:
        str: SQL query to insert into server table
    """

    if for_cams:
        logger.info("[INFO] working on table scheduling_cams")
        tbl_name = "scheduling_cams"
    else:
        logger.info("[INFO] working on table scheduling_schedule")
        tbl_name = "scheduling_schedule"

    schedules = get_data_from_cams(query)

    # term id
    terms = get_data_from_db(
        "select id as term_id, year, semester from scheduling_term"
    )

    # campus id
    campuses = get_data_from_db(
        "select id as campus_id, name as campus from scheduling_campus"
    )

    # location id
    locations = get_data_from_db(
        "select id as location_id, concat(building, room) as location from scheduling_location"
    )

    # instructor id
    instructors = get_data_from_db(
        "select id as instructor_id, concat(upper(last_name), ', ', upper(first_name)) as instructor\
        from scheduling_instructor"
    )

    # course id
    courses = get_data_from_db(
        "select sc.id as course_id, sc.subject, sc.number, sc.name\
        from scheduling_course sc"
    )

    if not for_cams:
        # insert by id
        # schedules["insert_by_id"] = 'NULL'
        # update by id
        # schedules["update_by_id"] = 'NULL'
        # insert_date as today
        schedules["insert_date"] = datetime.datetime.now()
        # update_date as today
        schedules["update_date"] = datetime.datetime.now()
        schedules["is_deleted"] = "false"
        # schedules["deleted_at"] = None
        # schedules["deleted_by"] = None

        # note as None
        # schedules["notes"] = None

    # ------- get ids for columns above --------#
    # no need to check because campus could be null
    schedules = schedules.merge(campuses, how="left", on="campus")
    schedules = schedules.merge(locations, how="left", on="location")
    schedules = schedules.merge(instructors, how="left", on="instructor")

    # term cannot be null
    schedules = schedules.merge(terms, how="left", on=["year", "semester"])
    num_missing = schedules["term_id"].isnull().sum()
    if num_missing:
        logger.error("Some term(s) is missing...")
        return None

    # add name when joining since some course with honor
    schedules = schedules.merge(courses, how="left", on=[
                                "subject", "number", "name"])
    num_missing = schedules["course_id"].isnull().sum()
    if num_missing:
        missing_courses = schedules.loc[
            schedules["course_id"].isnull(), ["subject", "number", "name"]
        ].drop_duplicates()
        logger.error("Missing courses in GCIS...")
        for _, row in missing_courses.iterrows():
            logger.info(f"{row['subject']}{row['number']} - {row['name']}")
        return None

    schedules_cols = [
        "term_id",
        "course_id",
        "section",
        "status",
        "capacity",
        "instructor_id",
        "campus_id",
        "location_id",
        "days",
        "start_time",
        "stop_time",
        "insert_date",
        # "insert_by_id",
        "update_date",
        # "update_by_id",
        "is_deleted",
    ]
    col_types = [
        "num",
        "num",
        "str",
        "str",
        "num",
        "num",
        "num",
        "num",
        "str",
        "str",
        "str",
        "str",
        # "num",
        "str",
        # "num",
        "str",
    ]

    if for_cams:
        schedules_cols = schedules_cols[:11]
        col_types = col_types[:11]

    schedules = schedules[schedules_cols]

    query = df_to_sql(
        df=schedules,
        col_names=schedules_cols,
        col_types=col_types,
        tbl_name=tbl_name,
        tbl_cols=schedules_cols,
    )

    query = query.replace("'None'", "NULL")
    query = query.replace("N\A", "NULL")
    query = query.replace("nan", "NULL")

    # query[-1] = ";"
    return query


def load_schedules_for_db(restart=True):
    query = generate_query_for_schedules_from_cams()

    if restart and not is_table_existing("scheduling_schedule"):
        restart = False

    if query:
        insert_data_into_db(query, restart=restart,
                            tbl_name="scheduling_schedule")


def load_schedules_for_cams(restart=True):
    query = generate_query_for_schedules_from_cams(for_cams=True)

    # talb enot exist cannot delete
    if not is_table_existing("scheduling_cams"):
        restart = False

    if query:
        if restart:
            logger.info("[INFO] Deleting scheduling cams table...")
        else:
            logger.info("[INFO] Appending data to cams database...")
            # delete_table('scheduling_cams')
        insert_data_into_db(query, restart=restart, tbl_name="scheduling_cams")
        print("*********** [INFO] Done! ***********")
        logger.info("[INFO] Success!")
        return True
    return False


def set_cams_update_datetime():
    logger.info("[INFO] setting CAMS update date and time")
    query = "update scheduling_dates set cams_update_at=now();"
    run_query_from_db(query)


# list of steps to do when reseting
def reset_database(restart=True):
    logger.info("[INFO] delete all tables with foreign keys...")
    delete_table("scheduling_cams")
    delete_table("scheduling_schedule")
    delete_table("scheduling_course")

    logger.info("[INFO] load data to database from cams...")
    load_terms_from_cams(restart=restart)
    load_campuses_from_cams(restart=restart)
    load_locations_from_cams(restart=restart)
    load_instructors_from_cams(restart=restart)
    load_courses_from_cams(restart=restart)
    load_schedules_for_db(restart=restart)
    load_schedules_for_cams(restart=restart)
    set_cams_update_datetime()


def update_cams_all(restart=True):

    logger.info("[INFO] Updating CAMS schedules")

    logger.info("[INFO] load data to database from cams...")
    done = load_schedules_for_cams(restart=restart)
    if done:
        set_cams_update_datetime()
        msg = "CAMS schedules have been updated successfully."
    else:
        msg = "Failed to update CAMS schedules. Please check the logs."
    print(msg)


def get_term_id_from_db(term):
    """term: str; format: Spring 2022
    return: term_id from db
    """
    term = term.upper()
    query = f"select id as term_id from scheduling_term where concat(Semester, ' ', Year) = '{term}'"
    df = get_data_from_db(query)
    if not df.empty:
        return list(df["term_id"].values)[0]
    else:
        raise Exception(f"{term} does not exist in the app.")


def delete_schedules_by_term_id(term_id):
    query = f"delete from scheduling_schedule where term_id={term_id}"
    logger.info("[INFO] Deleting rows from schduling_schedule...")
    run_query_from_db(query)


def reset_gcis_by_term(term):
    logger.info("[INFO] load data to database from cams...")
    # load related data to the app
    tids = get_data_from_cams(
        f"select TermCalendarID from TermCalendar where TextTerm='{term}'"
    )

    # query to get schedules from cams
    schedule_query_from_cams = generate_schedule_query_by_term_ids(
        term_ids=list(tids.TermCalendarID.values)
    )

    # prepare the query to insert schedules to gcis
    schedule_query_for_db = generate_query_for_schedules_from_cams(
        query=schedule_query_from_cams
    )

    logger.info(f"[INFO] Deleting old schedules for {term}")
    # delete related gcis schedules
    term_id = get_term_id_from_db(term)
    delete_schedules_by_term_id(term_id)

    # load schedules to gcis
    logger.info(f"[INFO] Inserting new schedules for {term}")
    insert_data_into_db(
        schedule_query_for_db, restart=False, tbl_name="scheduling_schedule"
    )

    set_cams_update_datetime()


def append_schedules_by_term(term):
    logger.info("[INFO] insert new term schedules to database from cams...")
    # load related data to the app
    tids = get_data_from_cams(
        f"select TermCalendarID from TermCalendar where TextTerm='{term}'"
    )

    # query to get schedules from cams
    schedule_query_from_cams = generate_schedule_query_by_term_ids(
        term_ids=list(tids.TermCalendarID.values)
    )

    # prepare the query to insert schedules to gcis
    schedule_query_for_db = generate_query_for_schedules_from_cams(
        query=schedule_query_from_cams
    )

    # load schedules to gcis
    # logger.info(f"[INFO] Inserting new schedules for {term}")
    insert_data_into_db(
        schedule_query_for_db, restart=False, tbl_name="scheduling_schedule"
    )
    print(f"{term} schedules have been added successfully.")


def delete_schedules_by_term(term):
    logger.info(f"[INFO] Deleting old schedules for {term}")
    # delete related gcis schedules
    term_id = get_term_id_from_db(term)
    delete_schedules_by_term_id(term_id)
    print(f"{term} schedules have been deleted.")
    print("Next steps: ")
    print("- Update term ids in `queries.py` file")
    print("- Run `python update.py` to update CAMS schedules in the app.")
    print("- Change the term to inactive in admin panel.")



