"""load data to GCIS"""
import datetime
import sys
import logging

from queries import SCHEDULE_QUERY, TERM_QUERY, SUBJECT_QUERY
from queries import CAMPUS_QUERY, LOCATION_QUERY, COURSE_QUERY, INSTRUCTOR_QUERY
from queries import generate_schedule_query_by_term_ids
from helper import get_data_from_cams, get_data_from_db, delete_data_from_db
from helper import insert_data_into_db, df_to_sql, is_table_existing, delete_table

logging.basicConfig(filename='gcis.log', level=logging.INFO,
                    format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


# no dependent foreignkeys
def load_terms_from_cams(restart=True, tbl_name='scheduling_term'):
    print('[INFO]Loading term data...')
    terms = get_data_from_cams(TERM_QUERY)
    query = df_to_sql(df=terms,
                      col_names=['year', 'semester', 'active'],
                      col_types=['num', 'str', 'str'],
                      tbl_name=tbl_name,
                      tbl_cols=['year', 'semester', 'active'])
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_subjects_from_cams(restart=True, tbl_name='main_subject'):
    print('[INFO]Loading subject data...')
    subjects = get_data_from_cams(SUBJECT_QUERY)
    query = df_to_sql(df=subjects,
                      col_names=['name'], col_types=['str'],
                      tbl_name=tbl_name, tbl_cols=['name'])
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_campuses_from_cams(restart=True, tbl_name='scheduling_campus'):
    print('[INFO]Loading campus data...')
    campuses = get_data_from_cams(CAMPUS_QUERY)
    query = df_to_sql(df=campuses,
                      col_names=['name'], col_types=['str'],
                      tbl_name=tbl_name, tbl_cols=['name'])
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_locations_from_cams(restart=True, tbl_name='scheduling_location'):
    print('[INFO]Loading location data...')
    locations = get_data_from_cams(LOCATION_QUERY)
    query = df_to_sql(df=locations,
                      col_names=['building', 'room'], col_types=['str', 'str'],
                      tbl_name=tbl_name, tbl_cols=['building', 'room'])
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# no dependent foreignkeys
def load_instructors_from_cams(restart=True, tbl_name='scheduling_instructor'):
    print('[INFO]Loading instructor data...')
    instructors = get_data_from_cams(INSTRUCTOR_QUERY)

    # added by Nancy, these are success coaches
    instructors = instructors.append(
        {'last_name': 'RATHFON', 'first_name': 'BECKI'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'DIAZ', 'first_name': 'LEWANDA'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'TOVAR', 'first_name': 'CASSANDRA'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'REESE', 'first_name': 'MARLINA'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'DEREK', 'first_name': 'DEYONGE'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'DEEN', 'first_name': 'LINDSAY'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'RANGEL', 'first_name': 'CATHLEEN'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'SULLIVAN', 'first_name': 'ZACHARY'}, ignore_index=True)

    # added by Debbie, these are non-GC faculty
    instructors = instructors.append(
        {'last_name': 'BALLINGER', 'first_name': 'JAMES'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'WOLFE', 'first_name': 'JOLE'}, ignore_index=True)
    instructors = instructors.append(
        {'last_name': 'CUMMINGS', 'first_name': 'RACHEL'}, ignore_index=True)

    instructors.sort_values('last_name', inplace=True)
    instructors.drop_duplicates(inplace=True)
    instructors.reset_index(drop=True, inplace=True)

    query = df_to_sql(df=instructors,
                      col_names=['first_name', 'last_name'],
                      col_types=['str', 'str'],
                      tbl_name=tbl_name,
                      tbl_cols=['first_name', 'last_name'])
    if restart and not is_table_existing(tbl_name):
        restart = False
    insert_data_into_db(query, restart=restart, tbl_name=tbl_name)


# depends on subject_id
def load_courses_from_cams(restart=True, tbl_name='scheduling_course'):
    print('[INFO]Loading course data...')
    # at this time, subject table is already loaded
    # courses = get_data_from_cams(COURSE_QUERY)
    # subjects = get_data_from_db('select id as subject_id, name as subject from main_subject')
    # courses = courses.merge(subjects, how='inner', on='subject')[['subject_id', 'number', 'credit', 'name']]
    # query = df_to_sql(df=courses,
    #                   col_names=['subject_id', 'number', 'credit', 'name'],
    #                   col_types=['num', 'str', 'num', 'str'],
    #                   tbl_name=tbl_name,
    #                   tbl_cols=['subject_id', 'number', 'credit', 'name'])
    query = generate_query_for_courses_from_cams(tbl_name=tbl_name)
    if restart and not is_table_existing(tbl_name):
        restart = False
    if query:
        delete_table(tbl_name)
        insert_data_into_db(query, restart=True, tbl_name=tbl_name)


def generate_query_for_courses_from_cams(tbl_name='scheduling_course'):
    # print('[INFO]Loading course data...')
    # at this time, subject table is already loaded
    courses = get_data_from_cams(COURSE_QUERY)
    courses.drop_duplicates(inplace=True)  # to add some courses from SRMaster
    subjects = get_data_from_db(
        'select id as subject_id, name as subject from main_subject')
    courses = courses.merge(subjects, how='left', on='subject')

    # for testing purpose
    # courses.loc[courses['subject']=='GOVT', 'subject_id'] = np.nan

    num_missing = courses['subject_id'].isnull().sum()
    if num_missing:
        missing_subjects = courses.loc[courses.subject_id.isnull(
        )].subject.values
        print(f'Missing subjects in GCIS: {list(set(missing_subjects))}')
        return None
    else:
        courses = courses[['subject_id', 'number', 'credit', 'name']]
        query = df_to_sql(df=courses,
                          col_names=['subject_id', 'number', 'credit', 'name'],
                          col_types=['num', 'str', 'num', 'str'],
                          tbl_name=tbl_name,
                          tbl_cols=['subject_id', 'number', 'credit', 'name'])
    return query


def generate_query_for_schedules_from_cams(query=SCHEDULE_QUERY, for_cams=False):

    if for_cams:
        print('[INFO] working on table scheduling_cams')
        tbl_name = 'scheduling_cams'
    else:
        print('[INFO] working on table scheduling_schedule')
        tbl_name = 'scheduling_schedule'

    schedules = get_data_from_cams(query)

    # term id
    terms = get_data_from_db(
        "select id as term_id, year, semester from scheduling_term")

    # campus id
    campuses = get_data_from_db(
        "select id as campus_id, name as campus from scheduling_campus")

    # location id
    locations = get_data_from_db(
        "select id as location_id, concat(building, room) as location from scheduling_location")

    # instructor id
    instructors = get_data_from_db("select id as instructor_id, concat(upper(last_name), ', ', upper(first_name)) as instructor\
        from scheduling_instructor")

    # course id
    courses = get_data_from_db("select sc.id as course_id, ms.name as subject, sc.number, sc.name\
        from scheduling_course sc\
        join main_subject ms on sc.subject_id = ms.id")

    if not for_cams:
        # insert by id
        schedules['insert_by_id'] = 1
        # update by id
        schedules['update_by_id'] = 1
        # insert_date as today
        schedules['insert_date'] = datetime.datetime.now()
        # update_date as today
        schedules['update_date'] = datetime.datetime.now()
        # note as None
        schedules['notes'] = None

    #------- get ids for columns above --------#
    # no need to check because campus could be null
    schedules = schedules.merge(campuses, how='left', on='campus')
    schedules = schedules.merge(locations, how='left', on='location')
    schedules = schedules.merge(instructors, how='left', on='instructor')

    # term cannot be null
    schedules = schedules.merge(terms, how='left', on=['year', 'semester'])
    num_missing = schedules['term_id'].isnull().sum()
    if num_missing:
        print('Some term(s) is missing...')
        return None

    # add name when joining since some course with honor
    schedules = schedules.merge(courses, how='left', on=[
                                'subject', 'number', 'name'])
    num_missing = schedules['course_id'].isnull().sum()
    if num_missing:
        missing_courses = schedules.loc[schedules['course_id'].isnull(), [
            'subject', 'number', 'name']].drop_duplicates()
        print('[Error] Missing Course(s) in GCIS...')
        # logger.error('Mising courses in GCIS...')
        for i, row in missing_courses.iterrows():
            print(f"{row['subject']}{row['number']} - {row['name']}")
            logger.info(f"{row['subject']}{row['number']} - {row['name']}")
        return None

    schedules_cols = ['term_id', 'course_id', 'section',
                      'status', 'capacity', 'instructor_id',
                      'campus_id', 'location_id', 'days', 'start_time',
                      'stop_time', 'insert_date', 'insert_by_id',
                      'update_date', 'update_by_id']
    col_types = ['num', 'num', 'str',
                 'str', 'num', 'num',
                 'num', 'num', 'str', 'str',
                 'str', 'str', 'num',
                 'str', 'num']

    if for_cams:
        schedules_cols = schedules_cols[:11]
        col_types = col_types[:11]

    schedules = schedules[schedules_cols]

    query = df_to_sql(df=schedules,
                      col_names=schedules_cols,
                      col_types=col_types,
                      tbl_name=tbl_name,
                      tbl_cols=schedules_cols)

    query = query.replace("'None'", "NULL")
    query = query.replace("N\A", "NULL")
    query = query.replace("nan", "NULL")

    return query


def load_schedules_for_db(restart=True):
    query = generate_query_for_schedules_from_cams()

    if restart and not is_table_existing('scheduling_schedule'):
        restart = False

    if query:
        insert_data_into_db(query, restart=restart,
                            tbl_name='scheduling_schedule')


def load_schedules_for_cams(restart=True):
    query = generate_query_for_schedules_from_cams(for_cams=True)

    # talb enot exist cannot delete
    if not is_table_existing('scheduling_cams'):
        restart = False

    if query:
        if restart:
            print('[INFO] Deleting scheduling cams table...')
        else:
            print('[INFO] Appending data to cams database...')
            # delete_table('scheduling_cams')
        insert_data_into_db(query, restart=restart, tbl_name='scheduling_cams')
        print('[INFO] Success!')


# schedules depend on ids
def load_schedules_from_cams(restart=True, for_cams=False):

    if not for_cams:
        print('[INFO]Loading GCIS schedule data...')
        tbl_name = 'scheduling_schedule'
    else:
        print('[INFO]Loading CAMS schedule data...')
        tbl_name = 'scheduling_cams'

    try:

        # pulling data from cams
        schedules = get_data_from_cams(SCHEDULE_QUERY)

        # term id
        terms = get_data_from_db(
            "select id as term_id, year, semester from scheduling_term")
        # terms.columns = ['Term_id', 'year', 'semester']

        # campus id
        campuses = get_data_from_db(
            "select id as campus_id, name as campus from scheduling_campus")
        # assert len(set(campuses['campus'])) == len(campuses)

        # location id
        locations = get_data_from_db(
            "select id as location_id, concat(building, room) as location from scheduling_location")
        # assert len(set(locations['location'])) == len(locations)

        # instructor id
        instructors = get_data_from_db("select id as instructor_id, concat(upper(last_name), ', ', upper(first_name)) as instructor\
            from scheduling_instructor")
        l = list(instructors['instructor'])
        d = set([x for x in l if l.count(x) > 1])
        assert len(set(instructors['instructor'])) == len(
            instructors), f'duplicated instructors: {d}'

        # course id
        courses = get_data_from_db("select sc.id as course_id, ms.name as subject, sc.number, sc.name\
            from scheduling_course sc\
            join main_subject ms on sc.subject_id = ms.id")

        if not for_cams:
            # insert by id
            schedules['insert_by_id'] = 1
            # update by id
            schedules['update_by_id'] = 1
            # insert_date as today
            schedules['insert_date'] = datetime.datetime.now()
            # update_date as today
            schedules['update_date'] = datetime.datetime.now()
            # note as None
            schedules['notes'] = None

        #------- get ids for columns above --------#
        schedules = schedules.merge(campuses, how='left', on='campus')
        schedules = schedules.merge(terms, how='left', on=['year', 'semester'])
        schedules = schedules.merge(locations, how='left', on='location')
        schedules = schedules.merge(instructors, how='left', on='instructor')

        # add name when joining since some course with honor
        schedules = schedules.merge(courses, how='left', on=[
                                    'subject', 'number', 'name'])

        schedules_cols = ['term_id', 'course_id', 'section',
                          'status', 'capacity', 'instructor_id',
                          'campus_id', 'location_id', 'days', 'start_time',
                          'stop_time', 'insert_date', 'insert_by_id',
                          'update_date', 'update_by_id']
        col_types = ['num', 'num', 'str',
                     'str', 'num', 'num',
                     'num', 'num', 'str', 'str',
                     'str', 'str', 'num',
                     'str', 'num']

        if for_cams:
            schedules_cols = schedules_cols[:11]
            col_types = col_types[:11]

        schedules = schedules[schedules_cols]
        # remove dupllicates??
        # schedules.drop_duplicates(inplace=True)

        query = df_to_sql(df=schedules,
                          col_names=schedules_cols,
                          col_types=col_types,
                          tbl_name=tbl_name,
                          tbl_cols=schedules_cols)

        query = query.replace("'None'", "NULL")
        query = query.replace("N\A", "NULL")
        query = query.replace("nan", "NULL")
    #    query = query.replace("term_id", "Term_id")

        if restart and not is_table_existing(tbl_name):
            restart = False

        insert_data_into_db(query, restart=restart, tbl_name=tbl_name)
    except Exception as e:
        print(e)


# list of steps to do when reseting
def reset_database(restart=True):
    print('delete all tables with foreign keys...')
    delete_table('scheduling_cams')
    delete_table('scheduling_schedule')
    delete_table('scheduling_course')

    print('load data to database from cams...')
    load_terms_from_cams(restart=restart)
    load_subjects_from_cams(restart=restart)
    load_campuses_from_cams(restart=restart)
    load_locations_from_cams(restart=restart)
    load_instructors_from_cams(restart=restart)
    load_courses_from_cams(restart=restart)
    load_schedules_for_db(restart=restart)
    load_schedules_for_cams(restart=restart)


def update_cams_all(restart=True):

    logger.info('[INFO] Updating CAMS schedules')

    print('[INFO] load data to database from cams...')
    load_schedules_for_cams(restart=restart)


def get_term_id_from_db(term):
    """term: str; format: Spring 2022
        return: term_id from db
    """
    term = term.upper()
    query = f"select id as term_id from scheduling_term where concat(Semester, ' ', Year) = '{term}'"
    df = get_data_from_db(query)
    if not df.empty:
        return list(df['term_id'].values)[0]
    else:
        raise Exception(f'{term} does not exist in the app.')


def delete_schedules_by_term_id(term_id):
    query = f"delete from scheduling_schedule where term_id={term_id}"
    print('[INFO] Deleting rows from schduling_schedule...')
    delete_data_from_db(query)

# TODO


def update_cams_by_term(term):
    pass


def reset_gcis_by_term(term):

    print('[INFO] load data to database from cams...')
    # load related data to the app
    tids = get_data_from_cams(
        f"select TermCalendarID from TermCalendar where TextTerm='{term}'")

    # query to get schedules from cams
    schedule_query_from_cams = generate_schedule_query_by_term_ids(
        term_ids=list(tids.TermCalendarID.values))

    # prepare the query to insert schedules to gcis
    schedule_query_for_db = generate_query_for_schedules_from_cams(
        query=schedule_query_from_cams)

    print(f'[INFO] Deleting old schedules for {term}')
    # delete related gcis schedules
    term_id = get_term_id_from_db(term)
    delete_schedules_by_term_id(term_id)

    # load schedules to gcis
    print(f'[INFO] Inserting new schedules for {term}')
    insert_data_into_db(schedule_query_for_db, restart=False,
                        tbl_name='scheduling_schedule')
