SELECT
    'DEPENTRY' T_TYPE,
    t.tranid TRAN_ID,
      t.ID InternalID,
    tl.id LINE_ID,
    t.trandate TRAN_DATE,
    t.tranid TRAN_NAME,
    tl.subsidiary SUBSIDIARY_ID,
    tl.location LOCATION_ID,
    tl.class DIVISION_ID,
    tl.department DEPARTMENT_ID,
    tl.entity ENTITY_ID,
    -99999 ITEM_ID,
    t.postingperiod POSTING_PERIOD_ID,
    a.id ACCOUNT_ID,
    a.acctnumber ACCOUNT_NUMBER,
    a.accttype ACCOUNT_TYPE,
    a.description ACCOUNT_DESC,
    null QTY,
    null EXT_COST,
    NVL(tl.creditforeignamount,0) - NVL(tl.debitforeignamount,0) ACCTG_AMOUNT,
    tl.memo MEMO
FROM  dw_ns_x_fam_dep_tran t
JOIN dw_ns_x_fam_dep_transaction_line tl
    ON t.id = tl.transaction
JOIN dw_ns_account_d a
    ON tl.expenseaccount = a.id
WHERE
--SUBSTR(a.acctnumber,1,1) IN ('4','5','6','7','8','9') AND
t.posting = 'T'