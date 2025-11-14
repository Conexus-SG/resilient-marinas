
-- ============================================================================
-- Merge STG_STELLAR_POS_ITEMS to DW_STELLAR_POS_ITEMS
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_MERGE_STELLAR_POS_ITEMS
IS
    v_inserted NUMBER := 0;
    v_updated NUMBER := 0;
    v_timestamp TIMESTAMP := SYSTIMESTAMP;
BEGIN
    MERGE INTO DW_STELLAR_POS_ITEMS tgt
    USING STG_STELLAR_POS_ITEMS src
    ON (tgt.ID = src.ID)
    WHEN MATCHED THEN
        UPDATE SET
            tgt.LOCATION_ID = src.LOCATION_ID,
            tgt.SKU = src.SKU,
            tgt.ITEM_NAME = src.ITEM_NAME,
            tgt.COST = src.COST,
            tgt.PRICE = src.PRICE,
            tgt.TAX_EXEMPT = src.TAX_EXEMPT,
            tgt.CREATED_AT = src.CREATED_AT,
            tgt.UPDATED_AT = src.UPDATED_AT,
            tgt.DW_LAST_UPDATED = v_timestamp
        WHERE (
            NVL(tgt.ID, -999) <> NVL(src.ID, -999) OR
            NVL(tgt.LOCATION_ID, -999) <> NVL(src.LOCATION_ID, -999) OR
            NVL(tgt.SKU, '~NULL~') <> NVL(src.SKU, '~NULL~') OR
            NVL(tgt.ITEM_NAME, '~NULL~') <> NVL(src.ITEM_NAME, '~NULL~') OR
            NVL(tgt.COST, -999.999) <> NVL(src.COST, -999.999) OR
            NVL(tgt.PRICE, -999.999) <> NVL(src.PRICE, -999.999) OR
            NVL(tgt.TAX_EXEMPT, '~NULL~') <> NVL(src.TAX_EXEMPT, '~NULL~') OR
            NVL(tgt.CREATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) <> NVL(src.CREATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) OR
            NVL(tgt.UPDATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')) <> NVL(src.UPDATED_AT, TO_TIMESTAMP('1900-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'))
        )
    WHEN NOT MATCHED THEN
        INSERT (
            ID, LOCATION_ID, SKU, ITEM_NAME, COST, PRICE, TAX_EXEMPT, CREATED_AT, UPDATED_AT,
            DW_LAST_INSERTED,
            DW_LAST_UPDATED
        )
        VALUES (
            src.ID, src.LOCATION_ID, src.SKU, src.ITEM_NAME, src.COST, src.PRICE, src.TAX_EXEMPT, src.CREATED_AT, src.UPDATED_AT,
            v_timestamp,
            v_timestamp
        );
    
    -- Count inserts and updates
    SELECT COUNT(*) INTO v_inserted 
    FROM DW_STELLAR_POS_ITEMS 
    WHERE DW_LAST_INSERTED = v_timestamp;
    
    SELECT COUNT(*) INTO v_updated 
    FROM DW_STELLAR_POS_ITEMS 
    WHERE DW_LAST_UPDATED = v_timestamp 
    AND DW_LAST_INSERTED < v_timestamp;
    
    COMMIT;
    
    DBMS_OUTPUT.PUT_LINE('DW_STELLAR_POS_ITEMS: ' || v_inserted || ' inserted, ' || v_updated || ' updated');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Error in SP_MERGE_STELLAR_POS_ITEMS: ' || SQLERRM);
        RAISE;
END SP_MERGE_STELLAR_POS_ITEMS;
/
