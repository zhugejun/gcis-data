select * 
from SROffer sro
where  CONCAT(sro.Department, sro.CourseID, sro.Section) = 'SPCH2335A01'
    and TermCalendarID = 584


--1022 open

SELECT *
from Glossary
where UniqueId = 1022


select tc.TextTerm, sro.Department, sro.CourseID, sro.Section
    , CourseType, CourseName 
from SROffer sro
join TermCalendar tc on sro.TermCalendarID = tc.TermCalendarID
where  CONCAT(sro.Department, sro.CourseID) = 'WLDG1457'
    and tc.TextTerm in ('Spring 2024', 'Summer 2024', 'Fall 2024', 'Spring 2025')


select distinct tc.TextTerm, tc.Term
    , sro.Department, sro.CourseID, sro.CourseName 
from SROffer sro
join TermCalendar tc on sro.TermCalendarID = tc.TermCalendarID
where  CONCAT(sro.Department, sro.CourseID) = 'WLDG1457'
order by tc.Term DESC



;

select Department, CourseID, CourseName 
from SRMaster srm
where  CONCAT(srm.Department, srm.CourseID) = 'WLDG1428'



select distinct tc.TextTerm, CourseName 
from SROffer sro
join TermCalendar tc on sro.TermCalendarID = tc.TermCalendarID
where  CONCAT(sro.Department, sro.CourseID) = 'WLDG1457'
    and tc.TextTerm in ('Spring 2024', 'Summer 2024', 'Fall 2024', 'Spring 2025')





-- CHEF1305 - SANITATION & SAFETY
-- ACNT1313 - COMP ACCOUNTING APPLICATION
-- HAMG1321 - INTRO HOSPITALITY MANAGEMENT




;
select Department, CourseID, Section, g.DisplayText as [Status]
    , c.Campus
    , fac.FirstName, fac.LastName
    , fac.EmployeeID
from SROffer sro
join TermCalendar tc on tc.TermCalendarID = sro.TermCalendarID
    left join campuses c on c.CampusID = sro.CampusID
    left join SROfferSchedule sch on sro.SROfferID = sch.SROfferID
    left join Rooms r on sch.OfferRoomID = r.RoomID
    left join buildings b on b.BuildingID = r.BuildingID
    left join SROfferSchedule_Faculty schf on sch.SROfferScheduleID = schf.SROfferScheduleID
    left join Faculty fac on schf.FacultyID = fac.FacultyID
    left join glossary g on g.uniqueid = sro.statusid
where tc.TextTerm = 'Spring 2024'
    and CONCAT(sro.Department, sro.CourseID, sro.Section) = 'BIOL2302C01HY'
    -- and fac.FirstName = 'wendy' and sro.Department = 'DNTA'



;


select * --distinct Department, CourseID, CourseName
from SRMaster
-- where Department = 'BARB'
where CONCAT(department, courseid) = 'MATH2318'


select distinct TextTerm, Department, CourseID, CourseName
from SROffer sro
join TermCalendar tc on sro.TermCalendarID = tc.TermCalendarID
where CONCAT(department, courseid) = 'COLL0271'
    and tc.TextTerm = 'Spring 2024'

;


;
select * from TermCalendar
where TextTerm not like '%Flx%'
        and TextTerm not like '%Qtr%'
        and TextTerm not like '%wk%'
order by Term desc


;
select distinct right(section, 2) 
from (
select distinct Section from SROffer sro
where len(Section) = 5
) as d

-- SELECT srm.Department, srm.CourseID, srm.CourseName
--     , cast(srm.LectureHours / 16 as int) as Lecture
--     , cast(srm.LabHours / 16 as int) as Lab
--     , srm.Credits
-- from SRMaster srm
-- where ActiveFlag = 1


SELECT srm.Department, srm.CourseID, srm.CourseName
    --, srm.LectureHours, srm.LabHours, srm.Credits
    --, srm.UpdateTime, srm.InsertTime
from SRMaster srm
where ActiveFlag = 1 
    and CONCAT(srm.Department, srm.CourseID) in ('SMFT2335', 'RADR1309', 'RADR1391', 'AGRI1107', 'AGRI1307', 'AGRI1131', 'AGRI1325', 'DFTG1413', 'DFTG2412', 'CNBT1411', 'DFTG2428', 'INDS2405', 'EECT1104', 'MATH0220', 'SRGT1405', 'SRGT1409', 'SRGT1441', 'SRGT1442', 'SRGT2130', 'SRGT1161', 'SRGT1660', 'SRGT2660', 'SMFT2450', 'RBTC1343', 'HYDR1445', 'INMT1417')
order by InsertTime DESC




SELECT concat(srm.Department,' ', srm.CourseID) as Course, srm.CourseName
    --, srm.LectureHours, srm.LabHours, srm.Credits
    --, srm.UpdateTime, srm.InsertTime
from SRMaster srm
where ActiveFlag = 1 
    and CONCAT(srm.Department, srm.CourseID) in ('SMFT2335', 'RADR1309', 'RADR1391', 'AGRI1107', 'AGRI1307', 'AGRI1131', 'AGRI1325', 'DFTG1413', 'DFTG2412', 'CNBT1411', 'DFTG2428', 'INDS2405', 'EECT1104', 'MATH0220', 'SRGT1405', 'SRGT1409', 'SRGT1441', 'SRGT1442', 'SRGT2130', 'SRGT1161', 'SRGT1660', 'SRGT2660', 'SMFT2450', 'RBTC1343', 'HYDR1445', 'INMT1417')
order by InsertTime DESC




select * from Glossary where Category=1031


select * from TermCalendar
-- where termCalendarID = 553
ORDER BY Term DESC
;


select sro.TermCalendarID, Department, CourseID, CourseType, Section 
    , upper(RTRIM(fac.LastName)) + ', ' + upper(RTRIM(fac.FirstName)) as Instructor
    , CourseName, Credits, g.displaytext as Status, MaximumEnroll as Capacity
    , c.Campus
    , concat(b.Abbreviation, ' ', r.Number) as Location
    , case when sch.OfferDays in (' ', '', 'N\A') then null else sch.OfferDays end as Days
    , sch.OfferTimeFrom as Start, sch.OfferTimeTo as Stop
    , sro.UpdateTime
from SROffer sro
left join campuses c on c.CampusID = sro.CampusID
left join SROfferSchedule sch on sro.SROfferID = sch.SROfferID
left join Rooms r on sch.OfferRoomID = r.RoomID
left join buildings b on b.BuildingID = r.BuildingID
left join SROfferSchedule_Faculty schf on sch.SROfferScheduleID = schf.SROfferScheduleID
left join Faculty fac on schf.FacultyID = fac.FacultyID
left join glossary g on g.uniqueid = sro.statusid
where sro.TermCalendarID = 559 and CONCAT(department, courseid) = 'COLL0271'


select UPPER(FirstName) as FirstName
    ,  UPPER(LastName) as LastName
    , MiddleName
    , InsertTime
    , UpdateTime
    , EmployeeID
from Faculty
-- where FirstName like '%Samantha%'
where lastName like '%Skinner%'



select sro.*
from SROffer sro
where Department = 'SPCH' 
    and TermCalendarID = 585



select * 
from faculty
