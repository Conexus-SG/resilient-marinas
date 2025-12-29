SELECT 
    ccd.calendar_date,
    ccd.month_year_name,
    ccd.month_number,
    ccd.year_number,
    dml.name as marina_name,
    dml.id as marina_id,
    case when  dml.name like '%Mile High%' then 8 
    when dml.name like '%Ray Roberts%' then 16 else 0 end as SubId,
    COUNT(dms.id) as daily_slip_count
FROM custom_calendar_dimension ccd
LEFT JOIN dw_molo_slips dms 
    ON TRUNC(dms.start_date) <= ccd.calendar_date
    AND (dms.end_date IS NULL OR TRUNC(dms.end_date) >= ccd.calendar_date)
LEFT JOIN dw_molo_marina_locations dml
    ON dms.marina_location_id = dml.id
WHERE dms.active = 1
GROUP BY 
    ccd.calendar_date,
    ccd.month_year_name,
    ccd.month_number,
    ccd.year_number,
    dml.name,
    dml.id
ORDER BY ccd.calendar_date DESC, marina_name