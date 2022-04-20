select * 
from SROffer sro
where  CONCAT(sro.Department, sro.CourseID, sro.Section) = 'INMT1419'
    and TermCalendarID = 553

;

select distinct Department, CourseID, CourseName
from SRMaster
where Department = 'BARB'
-- where CONCAT(department, courseid) = 'INMT1419'
;
select Department, CourseID, Section, g.DisplayText as [Status]
    , c.Campus
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
where sro.TermCalendarID = 553
    and CONCAT(sro.Department, sro.CourseID, sro.Section) = 'ABDR1307A02'
    -- and fac.FirstName = 'wendy' and sro.Department = 'DNTA'

;
select distinct TextTerm, Department, CourseID, CourseName
from SROffer sro
join TermCalendar tc on sro.TermCalendarID = tc.TermCalendarID
where CONCAT(department, courseid) = 'DFTG2450'
    and sro.TermCalendarID in (553, 554, 559, 561, 560)

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


SELECT srm.Department, srm.CourseID, srm.CourseName, srm.LectureHours, srm.LabHours, srm.Credits
from SRMaster srm
where ActiveFlag = 1


select * from Glossary where Category=1031


select * from TermCalendar
-- where termCalendarID = 553
ORDER BY Term DESC