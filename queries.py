standard_cost_query = """
        SELECT 
            SKU_ID, 
            STANDARD_COST	
        FROM DCSDBA.V_SKU_PROPERTIES	
        WHERE SITE_ID='MEM'	
            AND STANDARD_COST IS NOT NULL	
    """


standard_cost_insert_query = """
    INSERT INTO Standard_Cost(
        SKU_ID
        ,STANDARD_COST
        ,RECORD_DATE
        ,RECORD_KEY
        )
    VALUES(?, ?, ?, ?)
    """


crown_hlg_kits_dp_query = """
    SELECT TRUNC(SYSDATE-0.19) AS REPORT_DATE,CASE WHEN ORDER_TYPE='KTS' THEN 'CROWN' ELSE CASE WHEN CONSIGNMENT LIKE '%HLG%' THEN 'HLG' ELSE '' END END AS "KIT_BUILDER",
     COMPONENT_SKU,KIT_SKU,  ORDER_ID, LINE_ID,ORDER_TYPE, V_BRAND, TRUNC(SHIP_BY_DATE) AS SHIP_BY_DATE_, TRUNC(DSTAM) AS SHIPPED_DATE,QTY_ORDERED,SHIPPED_QTY, STATUS AS ORDER_STATUS,
    
    CASE WHEN STATUS='Shipped' and SHIPPED_QTY>=QTY_ORDERED AND TRUNC(DSTAM-0.2)<TRUNC(SHIP_BY_DATE-0.2) THEN 'Early' ELSE
    CASE WHEN STATUS='Shipped' and SHIPPED_QTY>=QTY_ORDERED AND TRUNC(DSTAM-0.2)=TRUNC(SHIP_BY_DATE-0.2) THEN 'On Time' ELSE
    CASE WHEN STATUS='Shipped' and SHIPPED_QTY>=QTY_ORDERED AND TRUNC(DSTAM-0.2)=TRUNC(SHIP_BY_DATE+1-0.2) AND EXTRACT(HOUR FROM DSTAM)<2 THEN 'On Time' ELSE
    CASE WHEN STATUS='Shipped' and SHIPPED_QTY>=QTY_ORDERED AND TRUNC(DSTAM)-0.2>TRUNC(SHIP_BY_DATE-0.2) THEN 'Late' ELSE
    'Open'  END END END END  AS LINE_STATUS
    
    FROM(
            SELECT  UPL.SKU_ID AS KIT_SKU,OL.ORDER_ID, OL.LINE_ID, OL.SKU_ID AS COMPONENT_SKU, OH.SHIP_BY_DATE, DSTAM, ORDER_TYPE, V_BRAND, CONSIGNMENT, OH.STATUS, QTY_ORDERED,SHIPPED_QTY
    
    
    
    
    
    
    
            FROM DCSDBA.ORDER_LINE OL LEFT JOIN DCSDBA.ORDER_HEADER OH ON OL.ORDER_ID=OH.ORDER_ID AND OL.CLIENT_ID=OH.CLIENT_ID
                                        
    
        
                LEFT JOIN    (SELECT CLIENT_ID,REFERENCE_ID, LINE_ID, SITE_ID, MAX(DSTAMP) AS DSTAM, SUM(UPDATE_QTY) AS SHIPPED_QTY FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM' AND CODE='Shipment'
                                GROUP BY CLIENT_ID, REFERENCE_ID, LINE_ID, SITE_ID) IT
                                ON OL.ORDER_ID=IT.REFERENCE_ID AND OL.LINE_ID=IT.LINE_ID
                 LEFT JOIN DCSDBA.PRE_ADVICE_LINE UPL ON OL.ORDER_ID=UPL.PRE_ADVICE_ID  AND OL.CLIENT_ID=UPL.CLIENT_ID              
                                
     
            WHERE OH.CLIENT_ID='VPNA' AND FROM_SITE_ID='MEM'
                    AND TRUNC(SHIP_BY_DATE)=TRUNC(SYSDATE-1.15)
                    AND (CONSIGNMENT LIKE '%HLG%' OR ORDER_TYPE='KTS')
    
        )
    
    WHERE STATUS<>'Cancelled'
    """


shipped_lines_query = """
    SELECT TRUNC(SYSDATE-0.19) AS REPORT_DATE, CASE WHEN ORDER_TYPE='KTS' THEN 'CRW' ELSE 'HLG' END AS "3PL", SKU_ID, UPDATE_QTY AS SHIPPED_QTY, ORDER_TYPE, ORDER_ID, LINE_ID,
SHIP_BY_DATE, DSTAMP AS SHIPPED_DATE
FROM DCSDBA.INVENTORY_TRANSACTION ITL LEFT JOIN DCSDBA.ORDER_HEADER OH ON ITL.REFERENCE_ID=OH.ORDER_ID AND ITL.SITE_ID=OH.FROM_SITE_ID
WHERE ITL.SITE_ID='MEM'
 AND TRUNC(DSTAMP)=TRUNC(SYSDATE-1.19)
 AND CODE='Shipment'
AND (ITL.CONSIGNMENT LIKE '%HLG%' OR ORDER_TYPE='KTS')
    """


open_kits_query = """
    SELECT 
        TRUNC(SYSDATE-0.19) AS REPORT_DATE, 
        SHIP_BY_DATE, 
        CASE WHEN ORDER_TYPE='KTS' THEN 'CRW' ELSE 'HLG' END AS "3PL", 
        QTY_ORDERED,
        OL.SKU_ID,  
        ORDER_TYPE, 
        OL.ORDER_ID, 
        OL.LINE_ID
    FROM DCSDBA.ORDER_LINE OL LEFT JOIN DCSDBA.ORDER_HEADER OH ON OL.ORDER_ID=OH.ORDER_ID AND OL.CLIENT_ID=OH.CLIENT_ID
    LEFT JOIN (SELECT DISTINCT SKU_ID, SITE_ID, REFERENCE_ID, LINE_ID FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM' AND CODE='Shipment')ITL
    ON OL.ORDER_ID=ITL.REFERENCE_ID AND ITL.SKU_ID=OL.SKU_ID AND OH.FROM_SITE_ID=ITL.SITE_ID
    where  OH.FROM_SITE_ID='MEM' AND OL.CLIENT_ID='VPNA'
    AND STATUS NOT IN ('Cancelled','Shipped')
    AND (OH.CONSIGNMENT LIKE '%HLG%' OR ORDER_TYPE='KTS')
    AND TRUNC(SHIP_BY_DATE)<TRUNC(SYSDATE-0.19)
    AND ITL.REFERENCE_ID IS NULL
    """


dp_kits_insert_query = """
                    INSERT INTO DP_Kits (REPORT_DATE
                                        ,KIT_BUILDER
                                        ,COMPONENT_SKU
                                        ,ORDER_ID
                                        ,LINE_ID
                                        ,ORDER_TYPE
                                        ,V_BRAND
                                        ,SHIP_BY_DATE
                                        ,SHIPPED_DATE
                                        ,QTY_ORDERED
                                        ,SHIPPED_QTY
                                        ,ORDER_STATUS
                                        ,LINE_STATUS
                                        ,RECORD_KEY) 
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """


shipped_lines_insert_query = """
                    INSERT INTO Shipped_Kits (
                                            REPORT_DATE
                                            ,TYPE
                                            ,SKU_ID
                                            ,SHIPPED_QTY
                                            ,ORDER_TYPE
                                            ,ORDER_ID
                                            ,LINE_ID
                                            ,SHIP_BY_DATE
                                            , SHIPPED_DATE
                                            ,RECORD_KEY)
                    VALUES(?,?,?,?,?,?,?,?,?, ?)
                    """


open_kits_insert_query = """
                    INSERT INTO  Open_Kits (
                                            REPORT_DATE
                                            ,SHIP_BY_DATE
                                            ,TYPE
                                            ,QTY_ORDERED
                                            ,SKU_ID
                                            ,ORDER_TYPE
                                            ,ORDER_ID
                                            ,LINE_ID
                                            ,RECORD_KEY)
                    VALUES(?,?,?,?,?,?,?,?,?)
                """


des_ops_query = """
    SELECT 
        SKU_ID
        ,USER_DEF_TYPE_3 AS OPC
        ,DESCRIPTION	
    FROM DCSDBA.SKU 	
    WHERE CLIENT_ID='VPNA'	
    """


des_ops_insert_query = """
    INSERT INTO Description_OPC(
        SKU_ID
        ,OPC
        ,DESCRIPTION
        ,REPORT_DATE
        ,RECORD_KEY
        ,BRAND_OPC
        )
    VALUES(?, ?, ?, ?, ?, ?)
    """


crown_prepack_rates_query = """
    SELECT 
        MAT.SKU_ID
        ,MAT.PREPACK_CODE
        ,SALES_MULTIPLE
        ,PREPACK_RATE	
    FROM DCSDBA.V_PREPACK_MAT_CODES MAT LEFT JOIN DCSDBA.V_SKU_PROPERTIES VSK ON MAT.SKU_ID=VSK.SKU_ID AND MAT.SITE_ID=VSK.SITE_ID	
    WHERE MAT.SITE_ID='MEM' AND PREPACK_RATE IS NOT NULL
    """


crown_prepack_rates_insert_query = """
    INSERT INTO Crown_Prepack_Rates(
        SKU_ID
        ,PREPACK_CODE
        ,SALES_MULTIPLE
        ,PREPACK_RATE
        ,REPORT_DATE
        ,RECORD_KEY
        )
    VALUES(?, ?, ?, ?, ?, ?)
    """


inbound_putaway_query = """
    SELECT 
    TRUNC(SYSDATE-0.19) AS REPORT_DATE
    ,PA.TAG_ID
    ,PA.SKU_ID
    ,PA.DSTAMP AS PUTAWAY_DATE
    ,PUTAWAY_QTY
    ,CASE WHEN REC.RECEIPT_DATE IS NULL THEN RECA.RECEIPT_DATE ELSE REC.RECEIPT_DATE END AS BYH_RECEIPT_FROM_CROWN_DATE_
    ,CASE WHEN REC.RECEIPT_DATE IS NULL THEN RECA.RECEIPT_QTY ELSE REC.RECEIPT_QTY END AS BYH_RECEIPT_QTY_FROM_CROWN
    ,CASE WHEN CROT.CROWN_OT_TRAILER_DATE IS NULL THEN CROTA.CROWN_OT_TRAILER_DATE ELSE CROT.CROWN_OT_TRAILER_DATE END AS CROWN_TRAILER_LOAD_DATE
    ,CASE WHEN CROT.CROWN_OT_TRAILER_DATE IS NULL THEN CROTA.QTY ELSE CROT.QTY END AS CROWN_TRAILER_LOAD_QTY
    FROM 
        (SELECT TAG_ID, SKU_ID, UPDATE_QTY AS PUTAWAY_QTY, DSTAMP
        FROM DCSDBA.INVENTORY_TRANSACTION
        WHERE SITE_ID='MEM'
        AND CODE='Putaway'
        AND TRUNC(DSTAMP)=TRUNC(SYSDATE-1.19)
        )PA
    LEFT JOIN 
    (SELECT TAG_ID, SKU_ID, UPDATE_QTY AS RECEIPT_QTY, MAX(DSTAMP) AS RECEIPT_DATE
    FROM DCSDBA.INVENTORY_TRANSACTION
    WHERE SITE_ID='MEM'
    AND FROM_LOC_ID LIKE 'CRTRL%OT%' AND TO_LOC_ID NOT LIKE 'CR%'
    GROUP BY TAG_ID, SKU_ID, UPDATE_QTY)REC ON PA.SKU_ID=REC.SKU_ID AND PA.TAG_ID=REC.tag_ID
    LEFT JOIN (
    SELECT TAG_ID, SKU_ID, UPDATE_QTY AS RECEIPT_QTY, MAX(DSTAMP) AS RECEIPT_DATE
    FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE
    WHERE SITE_ID='MEM'
    AND FROM_LOC_ID LIKE 'CRTRL%OT%' AND TO_LOC_ID NOT LIKE 'CR%'
    GROUP BY TAG_ID, SKU_ID, UPDATE_QTY
    ) RECA ON PA.TAG_ID=RECA.TAG_ID
    LEFT JOIN 
    (SELECT TAG_ID, SKU_ID, SUM(UPDATE_QTY) AS QTY, MAX(DSTAMP) AS CROWN_OT_TRAILER_DATE
    FROM DCSDBA.INVENTORY_TRANSACTION
    WHERE SITE_ID='MEM'
    AND  TO_LOC_ID LIKE 'CRTRL%OT%'
    AND UPDATE_QTY>0
    GROUP BY TAG_ID, SKU_ID
    ) CROT ON PA.TAG_ID=CROT.TAG_ID 
    
    LEFT JOIN (
    
    SELECT TAG_ID, SKU_ID, SUM(UPDATE_QTY) AS QTY, MAX(DSTAMP) AS CROWN_OT_TRAILER_DATE
    FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE
    WHERE SITE_ID='MEM'
    AND  TO_LOC_ID LIKE 'CRTRL%OT%'
    AND UPDATE_QTY>0
    GROUP BY TAG_ID, SKU_ID
    
    
    ) CROTA ON PA.TAG_ID=CROTA.TAG_ID 
    
    WHERE CROT.TAG_ID IS NOT NULL
    """


inbound_putaway_insert_query = """
    INSERT INTO Crown_Putaways(
                REPORT_DATE
                ,TAG_ID
                ,SKU_ID
                ,PUTAWAY_TIMESTAMP
                ,PUTAWAY_QTY
                ,BYH_RECEIPT_FROM_CROWN_DATE
                ,BYH_RECEIPT_QTY_FROM_CROWN
                ,CROWN_TRAILER_LOAD_DATE
                ,CROWN_TRAILER_LOAD_QTY
                ,RECORD_KEY
                )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


hlg_lip_putaways = """
    SELECT 
        TRUNC(SYSDATE-0.19) AS REPORT_DATE
        ,CASE WHEN ITL.SUPPLIER_ID='54197' THEN 'HLG' ELSE 'Lippert' END AS "3PL"
        ,ASN_ID
        ,ITL.TAG_ID
        ,ITL.SKU_ID
        ,ITL.SUPPLIER_ID
        ,CASE WHEN REC.QTY_RECEIVED IS NULL THEN RECA.QTY_RECEIVED ELSE REC.QTY_RECEIVED END AS QTY_RECEIVED
        ,UPDATE_QTY AS QTY_PUTAWAY
        ,CASE WHEN REC.RECEIPT_DATE IS NULL THEN RECA.RECEIPT_DATE else REC.RECEIPT_DATE END AS RECEIPT_DATE
        ,DSTAMP as PUTAWAY_DATE
        ,PAH.CREATION_DATE AS ASN_CREATION_DATE
        ,PAH.FINISH_DSTAMP AS ASN_COMPLETION_DATE
    FROM DCSDBA.INVENTORY_TRANSACTION ITL
    LEFT JOIN DCSDBA.PRE_ADVICE_HEADER PAH ON ITL.ASN_ID=PAH.PRE_ADVICE_ID AND ITL.SITE_ID=PAH.SITE_ID 
    LEFT JOIN DCSDBA.PRE_ADVICE_LINE PAL ON ITL.ASN_ID=PAL.PRE_ADVICE_ID AND ITL.CLIENT_ID=PAL.CLIENT_ID AND ITL.SKU_ID=PAL.SKU_ID
    LEFT JOIN (SELECT TAG_ID, SKU_ID, SITE_ID, CLIENT_ID, DSTAMP AS RECEIPT_DATE, UPDATE_QTY AS QTY_RECEIVED FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM' AND CODE='Receipt')
                REC ON ITL.SITE_ID=REC.SITE_ID AND ITL.TAG_ID=REC.TAG_ID
    LEFT JOIN (SELECT TAG_ID, SKU_ID, SITE_ID, CLIENT_ID, DSTAMP AS RECEIPT_DATE, UPDATE_QTY AS QTY_RECEIVED FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE WHERE SITE_ID='MEM' AND CODE='Receipt')
                RECA ON ITL.SITE_ID=RECA.SITE_ID AND ITL.TAG_ID=RECA.TAG_ID            
                
    
    WHERE ITL.SITE_ID='MEM'
    and code='Putaway'
    AND ITL.SUPPLIER_ID IN ('0008123','0753080039','0008121','8419','80039','32042','54197')
    AND TRUNC(DSTAMP)=TRUNC(SYSDATE-1.19)

    """


hlg_lip_putaways_insert = """
    INSERT INTO HLG_LIP_Putaways(
        REPORT_DATE
        ,TYPE
        ,ASN_ID
        ,TAG_ID
        ,SKU_ID
        ,SUPPLIER_ID
        ,QTY_RECEIVED
        ,QTY_PUTAWAY
        ,RECEIPT_DATE
        ,PUTAWAY_DATE
        ,ASN_CREATION_DATE
        ,ASN_COMPLETION_DATE
        ,RECORD_KEY
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


inbound_receipts = """
    SELECT 
        TRUNC(SYSDATE-0.19) AS REPORT_DATE
        ,CASE WHEN ITL.SUPPLIER_ID='54197' THEN 'HLG' ELSE 'Lippert' END AS "3PL"
        ,TRUNC(DSTAMP) AS DATE_
        ,TAG_ID
        ,ITL.SKU_ID
        ,SUPPLIER_ID
        ,UPDATE_QTY AS QTY
    FROM DCSDBA.INVENTORY_TRANSACTION ITL 
    WHERE ITL.SITE_ID='MEM'
    AND SUPPLIER_ID IN ('0008123','0753080039','0008121','8419','80039','32042','54197')
    AND CODE='Receipt'
    AND TRUNC (DSTAMP)=TRUNC(SYSDATE-1.19)
    
    union all 
    
    SELECT TRUNC(SYSDATE-0.19) AS REPORT_DATE, 'CRW' AS "3PL",TRUNC(DSTAMP) AS DATE_, TAG_ID, ITL.SKU_ID, SUPPLIER_ID,  UPDATE_QTY AS QTY
    FROM DCSDBA.INVENTORY_TRANSACTION ITL LEFT JOIN DCSDBA.V_SKU_PROPERTIES VSK ON ITL.SKU_ID=VSK.SKU_ID AND ITL.SITE_ID=VSK.SITE_ID
    WHERE ITL.SITE_ID='MEM'
    AND FROM_LOC_ID LIKE 'CR%OT%'
    AND TO_LOC_ID NOT LIKE 'CR%'
    AND TRUNC (DSTAMP)=TRUNC(SYSDATE-1.19)

    """


inbound_receipts_insert = """
    INSERT INTO Receipts(
        REPORT_DATE
        ,TYPE
        ,RECEIPT_DATE
        ,TAG_ID
        ,SKU_ID
        ,SUPPLIER_ID
        ,QTY
        ,RECORD_KEY
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
    """


open_asns = """
    SELECT TRUNC(SYSDATE-0.19) AS REPORT_DATE, CASE WHEN SUPPLIER_ID='54197' THEN 'HLG' ELSE 'Lippert' END AS "3PL",
    PAH.PRE_ADVICE_ID AS ASN_ID, PAL.SKU_ID, SUPPLIER_ID,QTY_DUE,
    PAH.CREATION_DATE AS ASN_CREATION_DATE,
     PAH.STATUS AS ASN_STATUS
    
    FROM  DCSDBA.PRE_ADVICE_HEADER PAH LEFT JOIN DCSDBA.PRE_ADVICE_LINE PAL ON PAH.PRE_ADVICE_ID=PAL.PRE_ADVICE_ID AND PAH.CLIENT_ID=PAL.CLIENT_ID 
         LEFT JOIN (SELECT ASN_ID, SKU_ID,DSTAMP, UPDATE_QTY FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM' AND CODE='Receipt')REC ON PAL.PRE_ADVICE_ID=REC.ASN_ID AND PAL.SKU_ID=REC.SKU_ID
         LEFT JOIN (SELECT ASN_ID, SKU_ID,DSTAMP, UPDATE_QTY FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE WHERE SITE_ID='MEM' AND CODE='Receipt')RECA ON PAL.PRE_ADVICE_ID=RECA.ASN_ID AND PAL.SKU_ID=RECA.SKU_ID
         
    WHERE PAH.SITE_ID='MEM'
           AND SUPPLIER_ID IN ('0008123','0753080039','0008121','8419','80039','32042','54197')
            AND TRUNC(DUE_DSTAMP)<TRUNC(SYSDATE-0.2)
             AND REC.DSTAMP IS NULL 
               AND RECA.DSTAMP IS NULL
               AND PAH.STATUS <>'Complete'
    """


open_asns_insert = """
    INSERT INTO Open_ASN (
        REPORT_DATE
        ,TYPE
        ,ASN_ID
        ,SKU_ID
        ,SUPPLIER_ID
        ,QTY_DUE
        ,ASN_CREATION_DATE
        ,ASN_STATUS
        ,RECORD_KEY
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


open_tags_hlg_lip = """
        SELECT 
            TRUNC(SYSDATE-0.19) AS REPORT_DATE,
            CASE WHEN SUPPLIER_ID='54197' THEN 'HLG' ELSE 'Lippert' END AS "3PL",  
            PALLET_ID, 
            INV.SKU_ID, 
            LOCATION_ID, 
        CASE WHEN REC.RECEIPT_DSTAMP IS NULL THEN INV.RECEIPT_DSTAMP ELSE REC.RECEIPT_DSTAMP END AS RECEIPT_DSTAMP, 
        MOVE_DSTAMP, 
        QTY_ON_HAND AS QTY
        ,INV.tag_id
        FROM DCSDBA.INVENTORY INV LEFT JOIN 
        (SELECT TAG_ID, SKU_ID, MIN(DSTAMP) AS RECEIPT_DSTAMP FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM'
        GROUP BY TAG_ID, SKU_ID)
        REC ON INV.TAG_ID=REC.TAG_ID 
        
        WHERE INV.SITE_ID='MEM'
        AND V_RIP='Y'
        AND SUPPLIER_ID IN ('0008123','0753080039','0008121','8419','80039','32042','54197')
        AND LOCATION_ID NOT LIKE 'CR%'
        oRDER BY RECEIPT_DSTAMP DESC
    """


open_tags_hlg_lip_insert = """
    INSERT INTO OPEN_TAGS(
        REPORT_DATE
        ,TYPE
        ,PALLET_ID
        ,SKU_ID
        ,LOCATION_ID
        ,RECEIPT_DSTAMP
        ,MOVE_DSTAMP
        ,QTY
        ,TAG_ID
        ,RECORD_KEY
        )
    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


crown_open_tags = """
              SELECT TRUNC(SYSDATE-0.19) AS REPORT_DATE,'CRW' AS "3PL",   PALLET_ID, INV.SKU_ID, LOCATION_ID, 
    CASE WHEN REC.RECEIPT_DSTAMP IS NULL THEN INV.RECEIPT_DSTAMP ELSE REC.RECEIPT_DSTAMP END AS RECEIPT_DSTAMP, MOVE_DSTAMP, QTY_ON_HAND AS QTY,
    INV.tag_id
    FROM DCSDBA.INVENTORY INV LEFT JOIN 
        (SELECT DISTINCT TAG_ID, SKU_ID FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM' AND TO_LOC_ID LIKE 'CR%') ITL ON INV.TAG_ID=ITL.TAG_ID AND INV.SKU_ID=ITL.SKU_ID
        LEFT JOIN
        (SELECT DISTINCT TAG_ID, SKU_ID FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE WHERE SITE_ID='MEM' AND TO_LOC_ID LIKE 'CR%' ) ITLA ON INV.TAG_ID=ITLA.TAG_ID AND INV.SKU_ID=ITLA.SKU_ID
    
        LEFT JOIN (SELECT TAG_ID, SKU_ID, MIN(DSTAMP) AS RECEIPT_DSTAMP FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM'
    GROUP BY TAG_ID, SKU_ID)
    REC ON INV.TAG_ID=REC.TAG_ID 
    
    WHERE SITE_ID='MEM'
    AND V_RIP='Y'
    AND ITL.TAG_ID||ITLA.TAG_ID IS NOT NULL
    AND INV.LOCATION_ID NOT LIKE 'CR%'
    """


receipt_dates = """
    SELECT DISTINCT(REPORT_DATE) FROM Receipts ORDER BY REPORT_DATE DESC
    """


crown_putaways_dates = """
        SELECT DISTINCT(REPORT_DATE) FROM Crown_Putaways ORDER BY REPORT_DATE DESC
    """


hlg_lip_putaways_dates = """
    SELECT DISTINCT(REPORT_DATE) FROM HLG_LIP_Putaways ORDER BY REPORT_DATE DESC
    """


THREE_PL_COST = """
                        SELECT TRUNC(SYSDATE-0.15) AS REPORT_DATE,'LIP' AS "3PL", CODE,TRUNC(DSTAMP) AS TRANSACTION_DATE, TAG_ID, ITL.SKU_ID, SUPPLIER_ID, UPDATE_QTY AS QTY

                        FROM DCSDBA.INVENTORY_TRANSACTION ITL
                        WHERE ITL.SITE_ID='MEM'
                        AND SUPPLIER_ID IN ('0008123','0753080039','0008121','8419','80039','32042')
                        AND CODE='Receipt'
                        AND TRUNC (DSTAMP) =TRUNC(SYSDATE-1.19)

                        union ALL

                        SELECT TRUNC(SYSDATE-0.15) AS REPORT_DATE,'HLG' AS "3PL", CODE,TRUNC(DSTAMP) AS TRANSACTION_DATE, TAG_ID, ITL.SKU_ID, SUPPLIER_ID, UPDATE_QTY AS QTY
                        FROM DCSDBA.INVENTORY_TRANSACTION ITL
                        WHERE ITL.SITE_ID='MEM'
                        AND CODE='Receipt'
                        AND SUPPLIER_ID='54197'
                        AND TRUNC (DSTAMP) =TRUNC(SYSDATE-1.19)

                        UNION ALL

                        SELECT TRUNC(SYSDATE-0.15) AS REPORT_DATE,'CRW' AS "3PL", CODE,TRUNC(DSTAMP) AS TRANSACTION_DATE, TAG_ID, ITL.SKU_ID, SUPPLIER_ID, UPDATE_QTY AS QTY
                        FROM DCSDBA.INVENTORY_TRANSACTION ITL
                        WHERE ITL.SITE_ID='MEM'
                        AND FROM_LOC_ID LIKE 'CR%'
                        AND TO_LOC_ID NOT LIKE 'CR%'
                        AND TRUNC (DSTAMP) =TRUNC(SYSDATE-1.19)
                    """


INSERT_DBO_COST_THREEPL = """
                        INSERT INTO COST_THREEPL(
                            REPORT_DATE
                            ,TYPE
                            ,CODE
                            ,TRANSACTION_DATE
                            ,TAG_ID
                            ,SKU_ID
                            ,SUPPLIER_ID
                            ,QTY
                            ,RECORD_KEY
                        )VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """


WORK_IN_PROGRESS = """
                    Select TRUNC(SYSDATE) AS REPORT_DATE,
                    "RECEIVED DATE",
                    LAST_MOVE,
                    MIN(CED) AS CROWN_ENTRY_DATE,
                    "Returns?",
                    PALLET,
                    TAG,
                    BINLOC,
                    SKU,
                    QOH,
                    DESCRIPTION,
                    ORIGIN,
                    SUPPLIER,
                    SUPP_PALLET,
                    CONDITION_CODE
                    FROM(
                    
                    
                    
                    Select distinct
                    
                    "RECEIVED DATE",
                    LAST_MOVE,
                    CASE WHEN CR_INN IS NOT NULL THEN CR_INN ELSE CR_IN END AS CED,
                    case WHEN trim(SUBSTR(CRINA.REFERENCE_ID,1,3)|| SUBSTR(CRIN.REFERENCE_ID,1,3))<>'000' THEN 'Not Returns' ELSE 'Returns' end AS "Returns?",
                    PALLET,
                    TAG,
                    BINLOC,
                    SKU,
                    QOH,
                    LPA.DESCRIPTION,
                    ORIGIN,
                    SUPPLIER,
                    SUPP_PALLET,
                    CONDITION_CODE
                    From
                    (
                    SELECT
                    CASE
                    WHEN inv.V_RIP = 'Y'
                    THEN (CAST (upl.receipt_dstamp AS DATE))
                    ELSE NULL
                    END "RECEIVED DATE",
                    CASE
                    WHEN inv.V_RIP = 'Y'
                    THEN TRUNC (SYSDATE - CAST (upl.receipt_dstamp AS DATE))
                    ELSE NULL
                    END DAYS,
                    CASE
                    WHEN inv.V_RIP = 'Y'
                    THEN CAST (inv.move_dstamp AS DATE)
                    ELSE NULL
                    END LAST_MOVE,
                    CASE WHEN inv.V_RIP = 'Y' THEN inv.pallet_id ELSE NULL END PALLET,
                    CASE WHEN inv.V_RIP = 'Y' THEN inv.tag_id ELSE inv.tag_id END TAG,
                    CASE
                    WHEN inv.V_RIP = 'Y' THEN inv.location_id
                    ELSE inv.location_id
                    END BINLOC,
                    CASE WHEN inv.V_RIP = 'Y' THEN inv.sku_id ELSE inv.sku_id END SKU,
                    CASE
                    WHEN inv.V_RIP = 'Y' THEN inv.qty_on_hand
                    ELSE inv.qty_on_hand
                    END QOH,
                    CASE
                    WHEN inv.V_RIP = 'Y' THEN inv.config_id
                    ELSE inv.config_id
                    END CONFIG,
                    CASE
                    WHEN inv.V_RIP = 'Y' THEN TRANSLATE (inv.description, ',.;', ' ')
                    ELSE NULL
                    END DESCRIPTION,
                    inv.origin_id ORIGIN,
                    CASE WHEN inv.V_RIP = 'Y' THEN inv.supplier_id ELSE NULL END SUPPLIER,
                    CASE
                    WHEN inv.V_RIP = 'Y'
                    THEN
                    NVL ((SELECT v_supplier_pallet_id
                    FROM dcsdba.upi_receipt_header uph
                    WHERE upl.pallet_id = uph.pallet_id),
                    '')
                    ELSE NULL
                    END SUPP_PALLET,
                    CASE WHEN inv.V_RIP = 'Y' THEN inv.condition_id ELSE NULL END CONDITION_CODE,
                    inr.receipt_id RECEIPT_ID,
                    inr.sku_id "SKU?",
                    TRUNC (
                    CAST (
                    NVL (
                    (SELECT ul.receipt_dstamp
                    FROM dcsdba.upi_receipt_line ul
                    WHERE inr.receipt_id = ul.pallet_id
                    AND inr.sku_id = ul.sku_id
                    AND ROWNUM = 1),
                    '') AS DATE)) "RECEIPT DAY"
                    
                    FROM DCSDBA.INVENTORY INV
                    INNER JOIN DCSDBA.LOCATION LOC
                    ON (INV.SITE_ID = LOC.SITE_ID AND INV.location_id = loc.location_id)
                    INNER JOIN DCSDBA.INVENTORY INR
                    ON (INV.SITE_ID = INR.SITE_ID AND INV.SKU_ID = INR.SKU_ID AND INR.V_RIP = 'Y')
                    LEFT OUTER JOIN DCSDBA.UPI_RECEIPT_LINE UPL
                    ON (INV.RECEIPT_ID = UPL.PALLET_ID AND INV.CLIENT_ID = UPL.CLIENT_ID AND INV.V_RIP = 'Y' AND INV.SKU_ID = UPL.SKU_ID)
                    LEFT OUTER JOIN DCSDBA.UPI_RECEIPT_LINE UL
                    ON (INR.RECEIPT_ID = UL.PALLET_ID AND INR.CLIENT_ID = UL.CLIENT_ID AND (CAST (ul.receipt_dstamp AS DATE) <= TO_DATE (CURRENT_DATE - 1)))
                    
                    
                    
                    WHERE
                    INV.CLIENT_ID = 'VPNA'
                    AND INV.SITE_ID = 'MEM'
                    AND (LOC.LOC_TYPE = 'Tag-FIFO' OR INV.v_rip = 'Y')
                    ) LPA
                    LEFT JOIN
                    (SELECT TAG_ID, SKU_ID,REFERENCE_ID, MIN(DSTAMP) AS CR_IN
                    FROM DCSDBA.INVENTORY_TRANSACTION
                    WHERE SITE_ID='MEM'
                    AND TO_LOC_ID LIKE 'CR%'
                    GROUP BY TAG_ID, SKU_ID, REFERENCE_ID
                    ) CRIN ON LPA.TAG=CRIN.TAG_ID
                    LEFT JOIN
                    
                    
                    
                    (SELECT TAG_ID, SKU_ID,REFERENCE_ID, MIN(DSTAMP) AS CR_INN
                    FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE
                    WHERE SITE_ID='MEM'
                    AND TO_LOC_ID LIKE 'CR%'
                    GROUP BY TAG_ID, SKU_ID, REFERENCE_ID
                    ) CRINA ON LPA.TAG=CRINA.TAG_ID
                    where
                    "RECEIVED DATE" is not null
                    and SUBSTR(BINLOC,1,2)='CR')
                    GROUP BY
                    
                    
                    
                    "RECEIVED DATE",
                    LAST_MOVE,
                    "Returns?",
                    PALLET,
                    TAG,
                    BINLOC,
                    SKU,
                    QOH,
                    DESCRIPTION,
                    ORIGIN,
                    SUPPLIER,
                    SUPP_PALLET,
                    CONDITION_CODE
                    """


INSERT_DBO_CROWN_WORK_IN_PROCESS = """  
                                    INSERT INTO CROWN_WORK_IN_PROCESS(
                                        REPORT_DATE
                                        ,RECEIVED_DATE
                                        ,LAST_MOVE
                                        ,CROWN_ENTRY_DATE
                                        ,IS_RETURNED
                                        ,PALLET
                                        ,TAG_ID
                                        ,BINLOC
                                        ,SKU_ID
                                        ,QOH
                                        ,DESCRIPTION
                                        ,ORIGIN
                                        ,SUPPLIER
                                        ,SUPP_PALLET
                                        ,CONDITION_CODE
                                        ,RECORD_KEY
                                    )VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)                           
                                    """


DATES_CROWN_WORK_IN_PROCESS = """
                                SELECT DISTINCT REPORT_DATE FROM CROWN_WORK_IN_PROCESS
                                """

BACKTRACK_CRWN_COST = """
      SELECT TRUNC(DSTAMP+1.15) AS REPORT_DATE,'CRW' AS "3PL", CODE,TRUNC(DSTAMP) AS TRANSACTION_DATE, TAG_ID, ITL.SKU_ID, SUPPLIER_ID, UPDATE_QTY AS QTY
                        FROM DCSDBA.INVENTORY_TRANSACTION ITL
                        WHERE ITL.SITE_ID='MEM'
                        AND FROM_LOC_ID LIKE 'CR%'
                        AND TO_LOC_ID NOT LIKE 'CR%'
                     AND EXTRACT(YEAR FROM DSTAMP)='2023'
    """