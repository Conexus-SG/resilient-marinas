SELECT
    'deposit' T_TYPE,
    t.tranid TRAN_ID,
      t.ID InternalID,
    tl.id LINE_ID,
    t.trandate TRAN_DATE,
    tl.trandisplayname TRAN_NAME,    
    tl.subsidiary SUBSIDIARY_ID,
    tl.location LOCATION_ID,
    tl.class DIVISION_ID,
    tl.department DEPARTMENT_ID,
    -99999 ENTITY_ID,
    -99999 ITEM_ID,
    t.postingperiod POSTING_PERIOD_ID,
    tl.account ACCOUNT_ID,
    a.acctnumber ACCOUNT_NUMBER,
    a.accttype ACCOUNT_TYPE,
    a.description ACCOUNT_DESC,
    null QTY,
    null EXT_COST,
    NVL(tl.accountingbook_credit,0) - NVL(tl.accountingbook_debit,0) ACCTG_AMOUNT,
    tl.memo MEMO
FROM  dw_ns_deposit_f t
JOIN dw_ns_deposit_lines_f tl
    ON t.id = tl.transaction
JOIN dw_ns_account_d a
    ON tl.account = a.id
WHERE 
--SUBSTR(a.acctnumber,1,1) IN ('4','5','6','7','8','9') AND 
t.posting = 'T'