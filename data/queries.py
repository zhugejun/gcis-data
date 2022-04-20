# a list of queries will be used in data.py

TERM_IDS = [553, 554, 559, 561, 560]
#          Sp22, su22, fa22, sp23, su23


def generate_schedule_query_by_term_ids(term_ids=None):
    if not term_ids:
        term_ids = TERM_IDS

    return f"select year(TermStartDate) as [year]\
    , case when TextTerm like '%Spr%' then 'SPRING'\
           when TextTerm like '%Fal%' then 'FALL'\
           when TextTerm like '%Sum%' then 'SUMMER' end as semester\
    , sro.Department as subject, sro.CourseID as number, upper(RTRIM(sro.CourseName)) as [name], section\
    , upper(case when g.displaytext='cancelled' then 'canceled' else g.DisplayText end) as [status]\
    , MaximumEnroll as capacity\
    , case when fac.FirstName is null or fac.LastName is null then null\
            else concat(upper(RTRIM(fac.LastName)), ', ', upper(RTRIM(fac.FirstName)))  end as instructor\
    , case when c.campus in ('', ' ') then null else c.campus end as campus\
    , case when concat(b.Abbreviation, r.Number) in (' ', '') then null\
            when concat(b.Abbreviation, r.Number) = 'NT000' then 'Internet'\
            else concat(b.Abbreviation, r.Number) end as location\
    , case when sch.OfferDays in (' ', '', 'N\\A') then null else sch.OfferDays end as days\
    , cast(sch.OfferTimeFrom as time) as start_time, cast(sch.OfferTimeTo as time) as stop_time\
    from SROffer sro\
        join TermCalendar tc on tc.TermCalendarID = sro.TermCalendarID\
        left join campuses c on c.CampusID = sro.CampusID\
        left join SROfferSchedule sch on sro.SROfferID = sch.SROfferID\
        left join Rooms r on sch.OfferRoomID = r.RoomID\
        left join buildings b on b.BuildingID = r.BuildingID\
        left join SROfferSchedule_Faculty schf on sch.SROfferScheduleID = schf.SROfferScheduleID\
        left join Faculty fac on schf.FacultyID = fac.FacultyID\
        left join glossary g on g.uniqueid = sro.statusid\
        where sro.TermCalendarID in ({','.join([str(x) for x in term_ids])})"


SCHEDULE_QUERY = generate_schedule_query_by_term_ids(TERM_IDS)


TERM_QUERY = f"select year(TermStartDate) as [year]\
    , case when TextTerm like '%Spr%' then 'SPRING'\
           when TextTerm like '%Fal%' then 'FALL'\
           when TextTerm like '%Sum%' then 'SUMMER' end as semester\
    , case when TermStartDate > GETDATE() then 'T' else 'F' end as active\
    from TermCalendar\
    where TermCalendarID in ({','.join([str(x) for x in TERM_IDS])})"


COURSE_QUERY = f"select distinct sro.Department as subject\
    , sro.CourseID as number\
    , sro.Credits as credit\
    , upper(RTRIM(sro.CourseName)) as [name]\
    from SROffer sro\
    where sro.TermCalendarID in ({','.join([str(x) for x in TERM_IDS])}) and len(Department)=4 and len(courseID)=4\
    UNION ALL\
    select distinct srm.Department as subject\
    , srm.CourseID as number\
    , srm.Credits as credit\
    , upper(RTRIM(srm.CourseName)) as [name]\
    from SRMaster srm\
    where activeFlag=1 and len(Department)=4 and len(courseID)=4"


INSTRUCTOR_QUERY = "select distinct upper(RTRIM(fac.LastName)) as last_name\
    , upper(RTRIM(fac.FirstName)) as first_name\
    from Faculty fac\
    where Active = 1 and HireStatusID <> 0 and employeeId <> ''"


CAMPUS_QUERY = "select Campus as [name]\
    from Campuses\
    where Campus <> ''\
    order by Campus"


LOCATION_QUERY = "select case when b.Abbreviation = 'NT' then 'Inter' else b.Abbreviation end as building\
    , case when b.Abbreviation = 'NT' and r.Number = '000' then 'net' else r.Number end as room\
    from Buildings b\
    join Rooms r on b.BuildingID = r.BuildingID\
    where b.Abbreviation <> '' "
