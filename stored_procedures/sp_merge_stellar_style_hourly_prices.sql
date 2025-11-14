
-- ============================================================================
-- Merge STG_STELLAR_STYLE_HOURLY_PRICES to DW_STELLAR_STYLE_HOURLY_PRICES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_STYLE_HOURLY_PRICES
IS
    v_inserted NUMBER := 0;
    v_updated NUMBER := 0;
    v_timestamp TIMESTAMP := SYSTIMESTAMP;
BEGIN
    MERGE INTO DW_STELLAR_STYLE_HOURLY_PRICES tgt
    USING STG_STELLAR_STYLE_HOURLY_PRICES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.STYLE_ID = src.STYLE_ID,
            tgt.SEASON_ID = src.SEASON_ID,
            tgt.HOURLY_TYPE = src.HOURLY_TYPE,
            tgt.DEFAULT_PRICE = src.DEFAULT_PRICE,
            tgt.HOLIDAY = src.HOLIDAY,
            tgt.SATURDAY = src.SATURDAY,
            tgt.SUNDAY = src.SUNDAY,
            tgt.MONDAY = src.MONDAY,
            tgt.TUESDAY = src.TUESDAY,
            tgt.WEDNESDAY = src.WEDNESDAY,
            tgt.THURSDAY = src.THURSDAY,
            tgt.FRIDAY = src.FRIDAY,
            tgt.DAY_DISCOUNT = src.DAY_DISCOUNT,
            tgt.UNDER_ONE_HOUR = src.UNDER_ONE_HOUR,
            tgt.FIRST_HOUR_AM = src.FIRST_HOUR_AM,
            tgt.FIRST_HOUR_PM = src.FIRST_HOUR_PM,
            tgt.MAX_PRICE = src.MAX_PRICE,
            tgt.MIN_HOURS = src.MIN_HOURS,
            tgt.MAX_HOURS = src.MAX_HOURS,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = v_timestamp
        WHERE (
            NVL(tgt.ID, -999) <> NVL(src.ID, -999) OR
            NVL(tgt.STYLE_ID, -999) <> NVL(src.STYLE_ID, -999) OR
            NVL(tgt.SEASON_ID, -999) <> NVL(src.SEASON_ID, -999) OR
            NVL(tgt.HOURLY_TYPE, '~NULL~') <> NVL(src.HOURLY_TYPE, '~NULL~') OR
            NVL(tgt.DEFAULT_PRICE, -999.999) <> NVL(src.DEFAULT_PRICE, -999.999) OR
            NVL(tgt.HOLIDAY, -999.999) <> NVL(src.HOLIDAY, -999.999) OR
            NVL(tgt.SATURDAY, -999.999) <> NVL(src.SATURDAY, -999.999) OR
            NVL(tgt.SUNDAY, -999.999) <> NVL(src.SUNDAY, -999.999) OR
            NVL(tgt.MONDAY, -999.999) <> NVL(src.MONDAY, -999.999) OR
            NVL(tgt.TUESDAY, -999.999) <> NVL(src.TUESDAY, -999.999) OR
            NVL(tgt.WEDNESDAY, -999.999) <> NVL(src.WEDNESDAY, -999.999) OR
            NVL(tgt.THURSDAY, -999.999) <> NVL(src.THURSDAY, -999.999) OR
            NVL(tgt.FRIDAY, -999.999) <> NVL(src.FRIDAY, -999.999) OR
            NVL(tgt.DAY_DISCOUNT, -999.999) <> NVL(src.DAY_DISCOUNT, -999.999) OR
            NVL(tgt.UNDER_ONE_HOUR, -999.999) <> NVL(src.UNDER_ONE_HOUR, -999.999) OR
            NVL(tgt.FIRST_HOUR_AM, -999.999) <> NVL(src.FIRST_HOUR_AM, -999.999) OR
            NVL(tgt.FIRST_HOUR_PM, -999.999) <> NVL(src.FIRST_HOUR_PM, -999.999) OR
            NVL(tgt.MAX_PRICE, -999.999) <> NVL(src.MAX_PRICE, -999.999) OR
            NVL(tgt.MIN_HOURS, -999.999) <> NVL(src.MIN_HOURS, -999.999) OR
            NVL(tgt.MAX_HOURS, -999.999) <> NVL(src.MAX_HOURS, -999.999) OR
            NVL(tgt.CREATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) <> NVL(src.CREATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) OR
            NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) <> NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))
        )
    WHEN NOT MATCHED THEN
        INSERT (
            ID, STYLE_ID, SEASON_ID, HOURLY_TYPE, DEFAULT_PRICE, HOLIDAY, SATURDAY, SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, DAY_DISCOUNT, UNDER_ONE_HOUR, FIRST_HOUR_AM, FIRST_HOUR_PM, MAX_PRICE, MIN_HOURS, MAX_HOURS, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.STYLE_ID, src.SEASON_ID, src.HOURLY_TYPE, src.DEFAULT_PRICE, src.HOLIDAY, src.SATURDAY, src.SUNDAY, src.MONDAY, src.TUESDAY, src.WEDNESDAY, src.THURSDAY, src.FRIDAY, src.DAY_DISCOUNT, src.UNDER_ONE_HOUR, src.FIRST_HOUR_AM, src.FIRST_HOUR_PM, src.MAX_PRICE, src.MIN_HOURS, src.MAX_HOURS, src.CREATED_AT, src.UPDATED_AT,
            v_timestamp,
            v_timestamp
        );
    
    SELECT COUNT(*) INTO v_inserted FROM DW_STELLAR_STYLE_HOURLY_PRICES WHERE DW_LAST_INSERTED = v_timestamp;
    SELECT COUNT(*) INTO v_updated FROM DW_STELLAR_STYLE_HOURLY_PRICES WHERE DW_LAST_UPDATED = v_timestamp AND DW_LAST_INSERTED < v_timestamp;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_STYLE_HOURLY_PRICES: ' || v_inserted || ' inserted, ' || v_updated || ' updated');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_STYLE_HOURLY_PRICES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_STYLE_HOURLY_PRICES;
/
