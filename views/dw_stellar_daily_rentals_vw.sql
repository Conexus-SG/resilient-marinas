select cd.CALENDAR_DATE,
       cd.MONTH_NUMBER,
       cd.YEAR_NUMBER,
       cd.MONTH_YEAR_NAME,
       dsb.id as booking_boat_id,
       dsb.boat_id,
       dssb.boat_number,
       dsb.boat_departure,
       dsb.boat_return,
       dsb.booking_id,
       ROUND(EXTRACT(DAY FROM (dsb.boat_return - dsb.boat_departure)) * 24 + EXTRACT(HOUR FROM (dsb.boat_return - dsb.boat_departure)), 2) as hours,
       dsb_bookings.boats_total,
       ROUND(dsb_bookings.boats_total / boat_count, 2) as rate_per_boat,
       ROUND((dsb_bookings.boats_total / boat_count) / (EXTRACT(DAY FROM (dsb.boat_return - dsb.boat_departure)) * 24 + EXTRACT(HOUR FROM (dsb.boat_return - dsb.boat_departure))), 2) as rate_per_boat_per_hour
from CUSTOM_CALENDAR_DIMENSION cd
inner join stg_stellar_booking_boats dsb
    on cd.CALENDAR_DATE >= TRUNC(dsb.boat_departure)
    and cd.CALENDAR_DATE <= TRUNC(dsb.boat_return)
    and dsb.status_booking = 'Complete'
inner join dw_stellar_style_boats dssb
    on dsb.boat_id = dssb.id
inner join dw_stellar_bookings dsb_bookings
    on dsb.booking_id = dsb_bookings.id and dsb_bookings.is_canceled = 'No'
inner join (
    select booking_id, count(*) as boat_count
    from dw_stellar_booking_boats
    group by booking_id
) boat_counts
    on dsb.booking_id = boat_counts.booking_id
where dsb.boat_departure IS NOT NULL 
  and dsb.boat_return IS NOT NULL
  and dsb.boat_id IS NOT NULL
order by cd.CALENDAR_DATE, dsb.booking_id