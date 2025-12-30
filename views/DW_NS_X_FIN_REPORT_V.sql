SELECT 

    u.t_type "T_TYPE",
    u.tran_id "TRAN_ID",
    u.internalid "INTERNALID",
    u.line_id "LINE ID",
    u.tran_date "TRAN_DATE",
    u.tran_name "TRAN_NAME",
    s.name SUBSIDIARY,
    l.name LOCATION,
    c.name CLASS,
    d.name DEPARTMENT,
    cs.entityid CUSTOMER,
    v.entityid "TRAN_VENDOR",
    i.itemid "ITEM_NAME",
    a.acctnumber "ACCOUNT_NUMBER",
    a.accttype "ACCOUNT_TYPE",
    a.accountsearchdisplayname "ACCOUNT_DESC",
    SUBSTR(p.periodname,5,8) "POSTING_YEAR",
    SUBSTR(p.periodname,1,3) "POSTING_MONTH",
    CAST(TO_CHAR(TO_DATE('01-' || SUBSTR(p.periodname,1,3) || '-' ||SUBSTR(p.periodname,5,8)), 'YYYYMM') AS INT) "POSTING_MONTH_NUM",
    ACCTG_AMOUNT,
    CASE WHEN a.accttype = 'Income' THEN ACCTG_AMOUNT else 0 end as INCOME_AMOUNT,
    CASE WHEN a.accttype = 'COGS' THEN ACCTG_AMOUNT else 0 end as COGS_AMOUNT,
    CASE WHEN a.accttype = 'Expense' THEN ACCTG_AMOUNT else 0 end as EXPENSE_AMOUNT, 
    CASE WHEN a.accttype = 'OthIncome' THEN ACCTG_AMOUNT else 0 end as OTHERINCOME_AMOUNT,
    CASE WHEN a.accttype = 'OthExpense' THEN ACCTG_AMOUNT else 0 end as OTHEREXPENSE_AMOUNT,
    CASE WHEN a.accttype = 'Income' THEN ACCTG_AMOUNT else 0 end + CASE WHEN a.accttype = 'COGS' THEN ACCTG_AMOUNT else 0 end as GROSS_PROFIT,
    CASE WHEN a.accttype = 'Income' THEN ACCTG_AMOUNT else 0 end + CASE WHEN a.accttype = 'COGS' THEN ACCTG_AMOUNT else 0 end +  CASE WHEN a.accttype = 'Expense' THEN ACCTG_AMOUNT else 0 end +
    CASE WHEN a.accttype = 'OthIncome' THEN ACCTG_AMOUNT else 0 end +   CASE WHEN a.accttype = 'OthExpense' THEN ACCTG_AMOUNT else 0 end  as NET_PROFIT,
    u.memo MEMO,
    ak.modelmappingdetail as mapvalue,
    ak.SORTORDER,
    ak.REPORTTYPE,
    ak.REPORTSECTION


FROM
(
SELECT * FROM DW_NS_X_FIN_JOURNAL_V --Journal Entries
UNION ALL
SELECT * FROM DW_NS_X_FIN_VENDBILL_V --Vendor Bills
UNION ALL
SELECT * FROM DW_NS_X_FIN_VENDCRED_V --Vendor Bill Credits
UNION ALL
SELECT * FROM DW_NS_X_FIN_CHECK_V --Checks
UNION ALL
SELECT * FROM DW_NS_X_FIN_DEPOSIT_V --Deposits
UNION ALL
SELECT * FROM DW_NS_X_FIN_CASHSALE_V --Cashsales
UNION ALL
SELECT * FROM DW_NS_X_FIN_INVOICE_V --Invoices
UNION ALL
SELECT * FROM DW_NS_X_FIN_VENDBILL_PAYMENT_V -- Vendor Bill Payments
UNION ALL
SELECT * FROM DW_NS_X_FIN_CUST_CREDIT_V -- Customer Credits
UNION ALL
SELECT * FROM DW_NS_X_FIN_CUST_PAYMENT_V -- Customer Payments
UNION ALL
SELECT * FROM DW_NS_X_FIN_CUST_REFUND_V -- Customer Refund
UNION ALL
SELECT * FROM DW_NS_X_FIN_CREDIT_CARD_V -- Credit Card
UNION ALL
SELECT * FROM DW_NS_X_FIN_DEPENTRY_V -- FAM DEP ENTRY
UNION ALL
SELECT * FROM DW_NS_X_FIN_CC_REFUND_V -- CC Refund
 ) u
JOIN DW_NS_ACCOUNT_D a ON u.account_id = a.id
LEFT JOIN DW_NS_SUBSIDIARY_D s ON u.subsidiary_id = s.id
LEFT JOIN DW_NS_LOCATION_D l ON u.location_id = l.id
LEFT JOIN DW_NS_CLASSIFICATION_D c ON u.division_id = c.id
LEFT JOIN DW_NS_LOCATION_D l ON u.location_id = l.id
LEFT JOIN DW_NS_DEPARTMENT_D d ON u.department_id = d.id
LEFT JOIN DW_NS_CUSTOMER_D cs ON u.entity_id = cs.id
LEFT JOIN DW_NS_VENDOR_D v ON u.entity_id = v.id
LEFT JOIN DW_NS_ITEM_D i ON u.item_id = i.id
LEFT JOIN DW_NS_ACCOUNTINGPERIOD_D p ON u.posting_period_id = p.id
LEFT JOIN NSACCOUNTKEY ak on CAST(ak.NSACCOUNT as varchar2(255))=u.account_number
  --WHERE  SUBSTR(p.periodname,5,8)='2025' and   SUBSTR(p.periodname,1,3)='Aug' and a.acctnumber like '8%'