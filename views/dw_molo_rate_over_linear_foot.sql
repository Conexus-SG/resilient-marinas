select cd.CALENDAR_DATE,
       cd.MONTH_NUMBER,
       cd.YEAR_NUMBER,
       cd.MONTH_YEAR_NAME,
       r.id as reservation_id, 
       r.NAME as RESERVATION_NAME,
       rs.NAME as RESERVATION_STATUS,
       rt.NAME as RESERVATION_TYPE,
       s.SLIP_ID,
       sl.NAME as SLIP_NAME,
       s.MARINA_LOCATION_ID,
       ml.NAME as MARINA_LOCATION_NAME,
       TO_NUMBER(SUBSTR(sl.RECOMMENDED_LOA, 1, INSTR(sl.RECOMMENDED_LOA, ':') - 1)) as LENGTH_IN_FT,
       r.SCHEDULED_ARRIVAL_TIME,
       r.SCHEDULED_DEPARTURE_TIME,
       TRUNC(r.SCHEDULED_DEPARTURE_TIME) - TRUNC(r.SCHEDULED_ARRIVAL_TIME) as RESERVATION_DAYS,
       ROUND(MONTHS_BETWEEN(r.SCHEDULED_DEPARTURE_TIME, r.SCHEDULED_ARRIVAL_TIME), 2) as RESERVATION_MONTHS,
       r.RESERVATION_STATUS_ID,
       r.RATE as reservation_rate,
       COALESCE(SUM(ii.total), 0) as invoice_total,
       CASE 
           WHEN (TRUNC(r.SCHEDULED_DEPARTURE_TIME) - TRUNC(r.SCHEDULED_ARRIVAL_TIME)) > 0
           THEN ROUND(r.RATE / (TRUNC(r.SCHEDULED_DEPARTURE_TIME) - TRUNC(r.SCHEDULED_ARRIVAL_TIME)), 2)
           ELSE 0
       END as reservation_daily_rate,
       CASE 
           WHEN (TRUNC(r.SCHEDULED_DEPARTURE_TIME) - TRUNC(r.SCHEDULED_ARRIVAL_TIME)) > 0
           THEN ROUND(COALESCE(SUM(ii.total), 0) / (TRUNC(r.SCHEDULED_DEPARTURE_TIME) - TRUNC(r.SCHEDULED_ARRIVAL_TIME)), 2)
           ELSE 0
       END as invoice_daily_rate,
       CASE 
           WHEN ROUND(MONTHS_BETWEEN(r.SCHEDULED_DEPARTURE_TIME, r.SCHEDULED_ARRIVAL_TIME), 0) > 0
           THEN ROUND(r.RATE / ROUND(MONTHS_BETWEEN(r.SCHEDULED_DEPARTURE_TIME, r.SCHEDULED_ARRIVAL_TIME), 0), 2)
           ELSE 0
       END as reservation_monthly_rate,
       CASE 
           WHEN ROUND(MONTHS_BETWEEN(r.SCHEDULED_DEPARTURE_TIME, r.SCHEDULED_ARRIVAL_TIME), 0) > 0
           THEN ROUND(COALESCE(SUM(ii.total), 0) / ROUND(MONTHS_BETWEEN(r.SCHEDULED_DEPARTURE_TIME, r.SCHEDULED_ARRIVAL_TIME), 0), 2)
           ELSE 0
       END as invoice_monthly_rate
from CUSTOM_CALENDAR_DIMENSION cd
left join DW_MOLO_RESERVATIONS r
    on cd.CALENDAR_DATE >= TRUNC(r.SCHEDULED_ARRIVAL_TIME)
    and cd.CALENDAR_DATE < TRUNC(r.SCHEDULED_DEPARTURE_TIME) + 1
left join DW_MOLO_RESERVATION_STATUS rs on r.RESERVATION_STATUS_ID = rs.ID
left join DW_MOLO_RESERVATION_TYPES rt on r.RESERVATION_TYPE_ID = rt.ID
left join (select distinct SLIP_ID, MARINA_LOCATION_ID from DW_MOLO_RESERVATIONS where SLIP_ID is not null) s
    on r.SLIP_ID = s.SLIP_ID
left join DW_MOLO_SLIPS sl on s.SLIP_ID = sl.ID
left join DW_MOLO_MARINA_LOCATIONS ml on s.MARINA_LOCATION_ID = ml.ID
left join dw_molo_invoices i on r.id = i.reservation_id
left join dw_molo_invoice_items ii on i.id = ii.invoice_id
    and (ii.type_field = 'charge-reservation-seasonal' OR ii.type_field = 'charge-reservation-transitive')
    and ii.is_void = 0 
    and ii.status_field != 'deleted'
where sl.ACTIVE = 1 and sl.DO_NOT_COUNT_IN_OCCUPANCY is null AND r.RESERVATION_STATUS_ID IN (4)
group by r.id, r.NAME, rs.NAME, rt.NAME, cd.CALENDAR_DATE, cd.MONTH_NUMBER, cd.YEAR_NUMBER, cd.MONTH_YEAR_NAME, s.SLIP_ID, sl.NAME, s.MARINA_LOCATION_ID, ml.NAME, sl.RECOMMENDED_LOA, r.SCHEDULED_ARRIVAL_TIME, r.SCHEDULED_DEPARTURE_TIME, r.RESERVATION_STATUS_ID, r.RATE
order by cd.CALENDAR_DATE, s.SLIP_ID