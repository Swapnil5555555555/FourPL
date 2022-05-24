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
    SELECT 
        TRUNC(SYSDATE-0.19) AS REPORT_DATE, 
        CASE WHEN ORDER_TYPE='KTS' THEN 'CRW' ELSE 'HLG' END AS "3PL", 
        SKU_ID, 
        UPDATE_QTY AS SHIPPED_QTY, 
        ORDER_TYPE, 
        ORDER_ID, 
        LINE_ID,
        SHIP_BY_DATE
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
                                            ,RECORD_KEY)
                    VALUES(?,?,?,?,?,?,?,?,?)
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


