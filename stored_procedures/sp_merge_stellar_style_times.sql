
-- ============================================================================
-- Merge STG_STELLAR_STYLE_TIMES to DW_STELLAR_STYLE_TIMES
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_STYLE_TIMES
IS
    v_inserted NUMBER := 0;
    v_updated NUMBER := 0;
    v_timestamp TIMESTAMP := SYSTIMESTAMP;
BEGIN
    MERGE INTO DW_STELLAR_STYLE_TIMES tgt
    USING STG_STELLAR_STYLE_TIMES src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.STYLE_ID = src.STYLE_ID,
            tgt.SEASON_ID = src.SEASON_ID,
            tgt.DESCRIPTION_TEXT = src.DESCRIPTION_TEXT,
            tgt.FRONTEND_DISPLAY = src.FRONTEND_DISPLAY,
            tgt.START_1 = src.START_1,
            tgt.END_1 = src.END_1,
            tgt.END_DAYS_1 = src.END_DAYS_1,
            tgt.STATUS_1 = src.STATUS_1,
            tgt.START_2 = src.START_2,
            tgt.END_2 = src.END_2,
            tgt.END_DAYS_2 = src.END_DAYS_2,
            tgt.STATUS_2 = src.STATUS_2,
            tgt.START_3 = src.START_3,
            tgt.END_3 = src.END_3,
            tgt.END_DAYS_3 = src.END_DAYS_3,
            tgt.STATUS_3 = src.STATUS_3,
            tgt.START_4 = src.START_4,
            tgt.END_4 = src.END_4,
            tgt.END_DAYS_4 = src.END_DAYS_4,
            tgt.STATUS_4 = src.STATUS_4,
            tgt.VALID_DAYS = src.VALID_DAYS,
            tgt.HOLIDAYS_ONLY_IF_VALID_DAY = src.HOLIDAYS_ONLY_IF_VALID_DAY,
            tgt.MAPPED_TIME_ID = src.MAPPED_TIME_ID,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = v_timestamp
        WHERE (
            NVL(tgt.ID, -999) <> NVL(src.ID, -999) OR
            NVL(tgt.STYLE_ID, -999) <> NVL(src.STYLE_ID, -999) OR
            NVL(tgt.SEASON_ID, -999) <> NVL(src.SEASON_ID, -999) OR
            NVL(tgt.DESCRIPTION_TEXT, '~NULL~') <> NVL(src.DESCRIPTION_TEXT, '~NULL~') OR
            NVL(tgt.FRONTEND_DISPLAY, '~NULL~') <> NVL(src.FRONTEND_DISPLAY, '~NULL~') OR
            NVL(tgt.START_1, -999.999) <> NVL(src.START_1, -999.999) OR
            NVL(tgt.END_1, -999.999) <> NVL(src.END_1, -999.999) OR
            NVL(tgt.END_DAYS_1, -999) <> NVL(src.END_DAYS_1, -999) OR
            NVL(tgt.STATUS_1, '~NULL~') <> NVL(src.STATUS_1, '~NULL~') OR
            NVL(tgt.START_2, -999.999) <> NVL(src.START_2, -999.999) OR
            NVL(tgt.END_2, -999.999) <> NVL(src.END_2, -999.999) OR
            NVL(tgt.END_DAYS_2, -999) <> NVL(src.END_DAYS_2, -999) OR
            NVL(tgt.STATUS_2, '~NULL~') <> NVL(src.STATUS_2, '~NULL~') OR
            NVL(tgt.START_3, -999.999) <> NVL(src.START_3, -999.999) OR
            NVL(tgt.END_3, -999.999) <> NVL(src.END_3, -999.999) OR
            NVL(tgt.END_DAYS_3, -999) <> NVL(src.END_DAYS_3, -999) OR
            NVL(tgt.STATUS_3, '~NULL~') <> NVL(src.STATUS_3, '~NULL~') OR
            NVL(tgt.START_4, -999.999) <> NVL(src.START_4, -999.999) OR
            NVL(tgt.END_4, -999.999) <> NVL(src.END_4, -999.999) OR
            NVL(tgt.END_DAYS_4, -999) <> NVL(src.END_DAYS_4, -999) OR
            NVL(tgt.STATUS_4, '~NULL~') <> NVL(src.STATUS_4, '~NULL~') OR
            NVL(tgt.HOLIDAYS_ONLY_IF_VALID_DAY, -999) <> NVL(src.HOLIDAYS_ONLY_IF_VALID_DAY, -999) OR
            NVL(tgt.MAPPED_TIME_ID, -999) <> NVL(src.MAPPED_TIME_ID, -999) OR
            NVL(tgt.CREATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) <> NVL(src.CREATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) OR
            NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) <> NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))
        )
    WHEN NOT MATCHED THEN
        INSERT (
            ID, STYLE_ID, SEASON_ID, DESCRIPTION_TEXT, FRONTEND_DISPLAY, START_1, END_1, END_DAYS_1, STATUS_1, START_2, END_2, END_DAYS_2, STATUS_2, START_3, END_3, END_DAYS_3, STATUS_3, START_4, END_4, END_DAYS_4, STATUS_4, VALID_DAYS, HOLIDAYS_ONLY_IF_VALID_DAY, MAPPED_TIME_ID, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.STYLE_ID, src.SEASON_ID, src.DESCRIPTION_TEXT, src.FRONTEND_DISPLAY, src.START_1, src.END_1, src.END_DAYS_1, src.STATUS_1, src.START_2, src.END_2, src.END_DAYS_2, src.STATUS_2, src.START_3, src.END_3, src.END_DAYS_3, src.STATUS_3, src.START_4, src.END_4, src.END_DAYS_4, src.STATUS_4, src.VALID_DAYS, src.HOLIDAYS_ONLY_IF_VALID_DAY, src.MAPPED_TIME_ID, src.CREATED_AT, src.UPDATED_AT,
            v_timestamp,
            v_timestamp
        );
    
    SELECT COUNT(*) INTO v_inserted FROM DW_STELLAR_STYLE_TIMES WHERE DW_LAST_INSERTED = v_timestamp;
    SELECT COUNT(*) INTO v_updated FROM DW_STELLAR_STYLE_TIMES WHERE DW_LAST_UPDATED = v_timestamp AND DW_LAST_INSERTED < v_timestamp;
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_STYLE_TIMES: ' || v_inserted || ' inserted, ' || v_updated || ' updated');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_STYLE_TIMES: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_STYLE_TIMES;
/
