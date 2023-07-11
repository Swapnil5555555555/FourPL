standard_cost_query = """
        SELECT 
            SKU_ID, 
            STANDARD_COST	
        FROM DCSDBA.V_SKU_PROPERTIES	
        WHERE SITE_ID='MEM'	
            AND STANDARD_COST IS NOT NULL	

    """


standard_cost_insert_query = """
    INSERT INTO Standard_Cost_Two(
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
                    SELECT TRUNC(SYSDATE) AS REPORT_DATE,
                    CASE WHEN R2.TAG_ID IS NULL THEN RD1 ELSE RD2 END AS RECEIVED_DATE,
                    MOVE_DSTAMP AS LAST_MOVE,
                    
                    CASE WHEN CR_INN IS NULL THEN CR_IN ELSE CR_INN END AS CROWN_ENTRY_DATE,
                    
                      case WHEN SUBSTR(RECEIPT_ID,1,3) IN ('000')  THEN 'Returns' ELSE 'Not Returns' end AS "Returns?",
                    '' AS SUPPLIER_PALLET,
                    INV.PALLET_ID AS PALLET,
                    INV.TAG_ID AS TAG,
                    INV.LOCATION_ID AS BINLOC,
                    INV.SKU_ID AS SKU,
                    QTY_ON_HAND AS QOH,
                    INV.DESCRIPTION AS DESCRIPTION,
                     INV.ORIGIN_ID AS ORIGIN,
                     INV.SUPPLIER_ID AS SUPPLIER,
                   CONDITION_ID AS CONDITION_CODE
                   
                     
from DCSDBA.INVENTORY INV INNER JOIN DCSDBA.LOCATION LOC ON (INV.SITE_ID = LOC.SITE_ID AND INV.location_id = loc.location_id)
        LEFT JOIN (SELECT TAG_ID, MIN(DSTAMP) AS RD1 FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='MEM' GROUP BY TAG_ID) R1 ON INV.TAG_ID=R1.TAG_ID
       LEFT JOIN (SELECT TAG_ID, MIN(DSTAMP) AS RD2 FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE WHERE SITE_ID='MEM' GROUP BY TAG_ID) R2 ON INV.TAG_ID=R2.TAG_ID
       LEFT JOIN
                    (SELECT TAG_ID, SKU_ID, MIN(DSTAMP) AS CR_IN
                    FROM DCSDBA.INVENTORY_TRANSACTION
                    WHERE SITE_ID='MEM'
                    AND TO_LOC_ID LIKE 'CR%'
                    GROUP BY TAG_ID, SKU_ID
                    ) CRIN ON INV.TAG_ID=CRIN.TAG_ID
                    LEFT JOIN
                    
                    
                    
                    (SELECT TAG_ID, SKU_ID, MIN(DSTAMP) AS CR_INN
                    FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE
                    WHERE SITE_ID='MEM'
                    AND TO_LOC_ID LIKE 'CR%'
                    GROUP BY TAG_ID, SKU_ID
                    ) CRINA ON INV.TAG_ID=CRINA.TAG_ID
                    
WHERE INV.SITE_ID='MEM' AND INV.LOCATION_ID LIKE 'CR%'
AND (LOC.LOC_TYPE = 'Tag-FIFO' OR INV.v_rip = 'Y')
order by INV.TAG_ID ASC
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


CRN_LIP_PRODUCTIVITY = """
        SELECT  
        'CRW_PROD' AS TYPE, 
        SKU_ID,
        TAG_ID, 
        MAX(DSTAMP) AS DSTAMP, 
        UPDATE_QTY AS QTY
        FROM DCSDBA.INVENTORY_TRANSACTION
        WHERE SITE_ID='MEM'
        AND TO_LOC_ID LIKE 'CR%OT%'
        AND TRUNC(DSTAMP)=TRUNC(SYSDATE-1.19)
        GROUP BY  SKU_ID, TAG_ID, UPDATE_QTY

        UNION ALL

        SELECT 'LIP_PROD' AS TYPE, SKU_ID, TAG_ID, MAX(DSTAMP) AS DSTAMP, UPDATE_QTY AS QTY
        FROM DCSDBA.INVENTORY_TRANSACTION
        WHERE SITE_ID='LIP'
        AND CODE='Shipment'
        AND TRUNC(DSTAMP)=TRUNC(SYSDATE-1.19)
        AND CUSTOMER_ID='8359'
        GROUP BY  SKU_ID, TAG_ID, UPDATE_QTY
"""
INSERT_CRN_LIP_PRODUCTIVITY = """
    INSERT INTO [dbo].[Crw_Lip_Productivity] 
    (
         TYPE
    ,SKU_ID 
    ,TAG_ID
    ,DSTAMP
    ,QTY
    )
    VALUES 
    (
        ?
        ,?
        ,?
        ,?
        ,?
    )
    """
    
LIP_AGING_GR="""
    SELECT TRUNC(SYSDATE) AS REPORT_DATE, 
            INV.SITE_ID, 
            case WHEN SUBSTR(RECEIPT_ID,1,3) IN ('000')  THEN 1 ELSE 0 end AS "IS_RETURNS",
            INV.DESCRIPTION, 
            INV.ORIGIN_ID AS ORIGIN, 
            INV.SUPPLIER_ID AS SUPPLIER, 
            INV.TAG_ID AS TAG, INV.SKU_ID AS SKU,  
            INV.LOCATION_ID AS LOCATION, 
            INV.PALLET_ID AS PALLET, 
            QTY_ON_HAND AS QOH,
            MOVE_DSTAMP AS LAST_MOVE, 
            CASE WHEN R2.TAG_ID IS NULL THEN RD1 ELSE RD2 END AS RECEIVED_DATE, 
            CONDITION_ID AS CONDITION_CODE
            
from DCSDBA.INVENTORY INV INNER JOIN DCSDBA.LOCATION LOC ON (INV.SITE_ID = LOC.SITE_ID AND INV.location_id = loc.location_id)
        LEFT JOIN (SELECT TAG_ID, MIN(DSTAMP) AS RD1 FROM DCSDBA.INVENTORY_TRANSACTION WHERE SITE_ID='LIP' GROUP BY TAG_ID) R1 ON INV.TAG_ID=R1.TAG_ID
       LEFT JOIN (SELECT TAG_ID, MIN(DSTAMP) AS RD2 FROM DCSDBA.INVENTORY_TRANSACTION_ARCHIVE WHERE SITE_ID='LIP' GROUP BY TAG_ID) R2 ON INV.TAG_ID=R2.TAG_ID
WHERE INV.SITE_ID='LIP'
AND (INV.v_rip = 'Y')
"""

INSERT_LIP_AGING = """
    INSERT INTO [dbo].[LIP_AGING_GR] 
    (
    REPORT_DATE
    ,SITE_ID
    ,IS_RETURNS
    ,DESCRIPTION
    ,ORIGIN
    ,SUPPLIER
    ,TAG
    ,SKU
    ,LOCATION
    ,PALLET
    ,QOH
    ,LAST_MOVE
    ,RECEIVED_DATE
    ,CONDITION_CODE
    )
    VALUES 
    (
        ?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
        ,?
    )
    """
    
    
SELECT_DISTINCT_SKU_AT_DATE = """
        SELECT
    DISTINCT
        SKU_ID
    FROM Standard_Cost_Two
    WHERE RECORD_DATE = '07/10/2023'
        """
        
        
SELECT_SKU_DISTINCT_BATCH_UPLOAD = """
    
    SELECT SKU_ID, STANDARD_COST
        FROM DCSDBA.V_SKU_PROPERTIES VSK
        WHERE SITE_ID='MEM'
        AND SKU_ID IN ('VOE17533016',
        'VOE16952314',
        'VOE53036813',
        'VOE54221249',
        'VOE15723224',
        'VOE24189359',
        'VOE55086004',
        'VOE23666979',
        'VOE53532562',
        'VOE54678300',
        'VOE54678200',
        'VOE17518881',
        'VOE14800440',
        'VO 23408089',
        'VO 24427882',
        'VO 22824189',
        'VO 24116134',
        'VO 23207447',
        'VO 21876101',
        'VOE12846371',
        'VO 24213158',
        'VO 24467021',
        'VO 24467022',
        'VO 24467023',
        'VO 24467024',
        'VOP23943433',
        'VOP22357748',
        'VOP24172588',
        'VOP21790095',
        'VOP24050842',
        'VOP24422740',
        'VOP23434168',
        'VOP23571049',
        'VOP23794280',
        'VOP21984801',
        'VOP23880370',
        'VOP24050847',
        'VOE14791525',
        'VOE14801261',
        'VOE14801268',
        'VOE14798024',
        'VOE14800492',
        'VOE14801259',
        'VOE14797202',
        'VOE14757237',
        'VOE14778130',
        'VOE14736356',
        'VOE14745633',
        'VOE14731268',
        'VOE14721347',
        'VOE54023828',
        'VOE17532118',
        'VO 24157510',
        'VO 23773351',
        'VO 21149785',
        'VO 21916208',
        'VOE53761972',
        'VOE17445923',
        'VOE54549790',
        'VOE54549792',
        'VO 84755345',
        'VO 78662195',
        'VO 24335562',
        'VO 60114231',
        'VOP23448104',
        'VO 23890153',
        'VO 22328788',
        'VO 24275412',
        'VO 22326557',
        'VO 24275462',
        'VOP24468713',
        'VO 84587147',
        'VO 22327338',
        'VO 78595539',
        'VOE53942931',
        'VO 24252007',
        'VOP24468101',
        'VOE12789432',
        'VOE53952263',
        'VO 82788263',
        'VO 82758311',
        'VO 22327341',
        'VO 24140815',
        'VO 24229627',
        'VO 23060019',
        'VOP24440503',
        'VOP24218952',
        'VO 23220464',
        'VOE12838647',
        'VOE53214405',
        'VO 21768442',
        'VO 24153691',
        'VOE20815301',
        'VO 24143306',
        'VO 24134183',
        'VO 24150179',
        'VOE53214399',
        'VOE53214406',
        'VOE17504673',
        'VOE53951839',
        'VOE54171373',
        'MK26971-MT945467',
        'VOE12804643',
        'VO 78665549',
        'VO 78665548',
        'VO 24254792',
        'VO 24177916',
        'VOP47713868',
        'VOP7749446',
        'VO 24299762',
        'VO 24299766',
        'VO 24473235',
        'VO 24383544',
        'VOE54715104',
        'VO 24365442',
        'VOE54647288',
        'VOE17485127',
        'VOE54094100',
        'VOE17445921',
        'VOP23018116',
        'VO 24345126',
        'VO 22794343',
        'VO 24365445',
        'VO 22582663',
        'VOE53885486',
        'VO 23552347',
        'VO 22431601',
        'VOE12814867',
        'VO 23498293',
        'VOP24452807',
        'VO 85145463',
        'VO 24475956',
        'VO 24459407',
        'VO 78645622',
        'VO 23721100',
        'VO 84742775',
        'VO 23144425',
        'VO 24169558',
        'VO 24169496',
        'VO 23968388',
        'VO 24226284',
        'VO 24226282',
        'VO 78554546',
        'VO 84750548',
        'VO 23556932',
        'VO 23849188',
        'VO 25332843',
        'VO 22419374',
        'VO 84222462',
        'VO 84750497',
        'VO 23640660',
        'VOE23080065',
        'VO 22327431',
        'VO 23720377',
        'VO 82789076',
        'VO 84750581',
        'VO 23823010',
        'VO 24335561',
        'VO 22431600',
        'VO 82782842',
        'VO 22329950',
        'VO 84200841',
        'VO 78602962',
        'VO 82781330',
        'VO 24171491',
        'VO 24413202',
        'VO 23721103',
        'VO 24177915',
        'VO 84752309',
        'VO 22419371',
        'VO 82758307',
        'VO 23481817',
        'VO 84200851',
        'VO 78588756',
        'VO 24110689',
        'VOE16337073',
        'VO 22419376',
        'VO 23850825',
        'VO 84222461',
        'VO 24433478',
        'VO 22312050',
        'VO 78660099',
        'VOE54884613',
        'VOE16683462',
        'VO 84731279',
        'VO 84716338',
        'VO 24296352',
        'VO 23498319',
        'VO 22431613',
        'VOE53109417',
        'VOE14797060',
        'VO 23746844',
        'VO 24074915',
        'VO 24074914',
        'VO 24074916',
        'VO 24074917',
        'VO 23327733',
        'VO 24145225',
        'VO 22937610',
        'VO 23554402',
        'VO 84736225',
        'VO 21536410',
        'VO 24365444',
        'VO 24129397',
        'VO 23394161',
        'VO 23556949',
        'VOE55243002',
        'VO 24345123',
        'VO 24431421',
        'VO 24134126',
        'VO 24013917',
        'VO 24128020',
        'VOE60110036',
        'VOE16358994',
        'VOE55175314',
        'VOE54722722',
        'VOE53043918',
        'VOE55171964',
        'VOE17515738',
        'VOE16928977',
        'VOE16355224',
        'VOE24319373',
        'VOE17511826',
        'VOE53009339',
        'VOE16954925',
        'VOE15723674',
        'VOE16901490',
        'VOE54542986',
        'VOE53256460',
        'VOE53489684',
        'VOE14765130',
        'VOE14800490',
        'VOE14680754',
        'VOE14800481',
        'VOE14800488',
        'VOE14801265',
        'VOE14801263',
        'VOE14798015',
        'VOE14800477',
        'VOE14799333',
        'VOE14736675',
        'VOE14799837',
        'VOE14800473',
        'VOE14719257',
        'VOE14732090',
        'VOE14776749',
        'VO 24459389',
        'VOP24189544',
        'VOE16676991',
        'VOP23681927',
        'VO 70351722',
        'VOP24038948',
        'VO 24475980',
        'VO 21805002',
        'VOP23040864',
        'VOP22697632',
        'VOP23108178',
        'VO 84706782',
        'VO 24157504',
        'VO 22412407',
        'CH 97020',
        'VO 22904455',
        'VO 23817329',
        'VO 23817328',
        'VO 24136280',
        'VO 23584606',
        'VO 24188609',
        'VO 24419255',
        'VO 24157506',
        'VO 24159627',
        'VOE54464438',
        'VO 24427671',
        'VO 24117227',
        'VO 24169586',
        'VOE15405117',
        'VOE54123613',
        'VO 24378426',
        'VO 24013984',
        'VOE53135099',
        'VO 24062544',
        'VO 21562355',
        'VOP24154070',
        'VO 23833396',
        'VO 84706777',
        'VO 23579630',
        'VO 22558248',
        'VO 24228003',
        'VO 23774883',
        'VO 21096421',
        'VOE54679802',
        'VO 23231628',
        'VO 22432303',
        'VO 82783835',
        'VO 24207677',
        'VO 22656125',
        'VO 24244546',
        'VO 84740380',
        'VO 24226285',
        'VO 82052707',
        'VO 78585299',
        'VOP54589479',
        'VO 23987583',
        'VO 23283797',
        'VO 24228566',
        'VO 24170609',
        'VOE14800494',
        'VOE14713519',
        'VOE14713520',
        'VOE23621308',
        'VO 24157502',
        'VOE24137351',
        'VOE14750391',
        'VOE17506330',
        'VOE53666524',
        'VOE54719409',
        'VOE24282206',
        'VOE55163080',
        'VOE17514154',
        'VOE17534642',
        'VOE17515236',
        'VOE17518365',
        'VOE17520289',
        'VOE17513510',
        'VOE54871525',
        'VOE54029944',
        'VOE55185699',
        'VOE55182942',
        'VOE55185457')
        
    """