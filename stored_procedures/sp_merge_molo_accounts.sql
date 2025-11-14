
-- ============================================================================
-- Merge STG_MOLO_ACCOUNTS to DW_MOLO_ACCOUNTS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_MOLO_ACCOUNTS
IS
    v_inserted NUMBER := 0;
    v_updated NUMBER := 0;
    v_timestamp TIMESTAMP := SYSTIMESTAMP;
BEGIN
    MERGE INTO DW_MOLO_ACCOUNTS tgt
    USING STG_MOLO_ACCOUNTS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.ACCOUNT_STATUS_ID = src.ACCOUNT_STATUS_ID,
            tgt.MARINA_LOCATION_ID = src.MARINA_LOCATION_ID,
            tgt.CONTACT_ID = src.CONTACT_ID,
            tgt.DW_LAST_UPDATED = v_timestamp
        WHERE
            NVL(tgt.ACCOUNT_STATUS_ID, -999) <> NVL(src.ACCOUNT_STATUS_ID, -999) OR
            NVL(tgt.MARINA_LOCATION_ID, -999) <> NVL(src.MARINA_LOCATION_ID, -999) OR
            NVL(tgt.CONTACT_ID, -999) <> NVL(src.CONTACT_ID, -999)
    WHEN NOT MATCHED THEN
        INSERT (
            ID, ACCOUNT_STATUS_ID, MARINA_LOCATION_ID, CONTACT_ID,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.ACCOUNT_STATUS_ID, src.MARINA_LOCATION_ID, src.CONTACT_ID,
            v_timestamp,
            v_timestamp
        );
    
    SELECT COUNT(*) INTO v_inserted
    FROM DW_MOLO_ACCOUNTS
    WHERE DW_LAST_INSERTED = v_timestamp;
    
    SELECT COUNT(*) INTO v_updated
    FROM DW_MOLO_ACCOUNTS
    WHERE DW_LAST_UPDATED = v_timestamp
    AND DW_LAST_INSERTED < v_timestamp;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_MOLO_ACCOUNTS: ' || v_inserted || ' inserted, ' || v_updated || ' updated');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_MOLO_ACCOUNTS: ' || SQLERRM);
        RAISE;
END SP_MERGE_MOLO_ACCOUNTS;
/
