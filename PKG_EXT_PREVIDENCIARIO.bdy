CREATE OR REPLACE PACKAGE BODY OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO IS

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SISTEMA     : AMADEUS CAPITALIZACAO
-- DESCRICAO   : EM VIRTUDE DO SALDAMENTO DO PLANO PSAP/ELETROPAULO (PLANO 19), O EXTRATO PREVIDENCIARIO PRECISARA PASSAR POR ALTERACOES
-- ANALISTA    : ADRIANO LIMA
-- DATA CRIACAO: 23/11/2020
-- MANUTENCOES : PROJ-760/PSD-11865 - DATA: 23/11/2020 ¿ ANALISTA: ADRIANO LIMA/RENATO DAVI

-- MANUTENCOES : PROJ-3677 - DATA: 02/02/2021 ¿ ANALISTA: ADRIANO LIMA - DESCRICAO: EXECUTAR A PROCEDURE PARA AJUSTES DO EXTRATO PREVIDENCIARIO:
--               ESSA MANUTENCAO CONSISTE EM INSERIR O BDS NO EXTRATO PREVIDENCIARIO (19 PSAP/Eletropaulo)             

-- MANUTENCOES : PSD-32395 - DATA: 09/03/2021 ANALISTA ADRIANO LIMA CRIACAO DA PROCEDURE PROC_EXT_PREV_TIETE
--               ESSA MANUTENCAO CONSISTE EM INSERIR O BDS NO EXTRATO PREVIDENCIARIO (31 PSAP/TIETE)
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                           
/*    PROCEDURE PRC_INICIALIZA_VARIAVEIS IS
    BEGIN
      -- captura o usuario logado no BD para gravar na tabela
      -- de log do processamento de integracao
      --
      SELECT SYS_CONTEXT('USERENV', 'MODULE')       AS MODULE
            ,SYS_CONTEXT('USERENV', 'OS_USER')      AS OS_USER
            ,SYS_CONTEXT('USERENV', 'TERMINAL')     AS TERMINAL
            ,SYS_CONTEXT('USERENV', 'CURRENT_USER') AS "CURRENT_USER" 
            ,SYS_CONTEXT('USERENV', 'IP_ADDRESS')   AS IP_ADDRESS    
        INTO G_MODULE
            ,G_OS_USER
            ,G_TERMINAL
            ,G_CURRENT_USER
            ,G_IP_ADDRESS
        FROM DUAL;

      -- utilizado para definir o host para o disparo de emails
      --
      SELECT I.HOST_NAME
        INTO G_HOST_NAME
        FROM SYS.V_$INSTANCE I;
      --
    END PRC_INICIALIZA_VARIAVEIS;

    PROCEDURE PRC_GRAVA_LOG ( P_COD_LOG_CARGA_EXTRATO NUMBER    DEFAULT NULL
                             ,P_TPO_DADO              NUMBER    DEFAULT NULL
                             ,P_COD_EMPRS             NUMBER    DEFAULT NULL
                             ,P_NUM_RGTRO_EMPRG       VARCHAR2  DEFAULT NULL
                             ,P_DTA_FIM_EXTR          DATE      DEFAULT NULL
                             ,P_QTD_LINHAS            NUMBER    DEFAULT NULL
                             ,P_DT_INCLUSAO           TIMESTAMP DEFAULT NULL
                             ,P_STATUS                CHAR      DEFAULT NULL
                             ,P_OBSERVACAO            VARCHAR2  DEFAULT NULL
                             ,P_MODULE                VARCHAR2  DEFAULT NULL
                             ,P_OS_USER               VARCHAR2  DEFAULT NULL
                             ,P_TERMINAL              VARCHAR2  DEFAULT NULL
                             ,P_CURRENT_USER          VARCHAR2  DEFAULT NULL
                             ,P_IP_ADDRESS            VARCHAR2  DEFAULT NULL
                           )
    IS
    BEGIN
      --
      INSERT INTO OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO (  COD_LOG_CARGA_EXTRATO
                                                          ,TPO_DADO
                                                          ,COD_EMPRS
                                                          ,NUM_RGTRO_EMPRG
                                                          ,DTA_FIM_EXTR
                                                          ,QTD_LINHAS
                                                          ,DT_INCLUSAO
                                                          ,STATUS
                                                          ,OBSERVACAO
                                                          ,MODULE
                                                          ,OS_USER
                                                          ,TERMINAL
                                                          ,CURRENT_USER
                                                          ,IP_ADDRESS
                                                        )
                                                VALUES ( OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO_SEQ.NEXTVAL
                                                        ,P_TPO_DADO             
                                                        ,P_COD_EMPRS            
                                                        ,P_NUM_RGTRO_EMPRG      
                                                        ,P_DTA_FIM_EXTR         
                                                        ,P_QTD_LINHAS           
                                                        ,P_DT_INCLUSAO          
                                                        ,P_STATUS               
                                                        ,P_OBSERVACAO           
                                                        ,P_MODULE               
                                                        ,P_OS_USER              
                                                        ,P_TERMINAL             
                                                        ,P_CURRENT_USER         
                                                        ,P_IP_ADDRESS
                                                       );
    END PRC_GRAVA_LOG;
    
    FUNCTION FN_VALIDA_TABELA( P_FN_TABELA VARCHAR2
                              ,P_FN_OWNER  VARCHAR2 ) RETURN BOOLEAN IS
      L_EXIST NUMBER;
    BEGIN
       SELECT 1
         INTO L_EXIST
         FROM ALL_TABLES DT
        WHERE TRIM(UPPER(DT.TABLE_NAME)) = 'FC_PRE_TBL_BASE_EXTRAT_CTB' --P_FN_TABELA
          AND TRIM(UPPER(DT.OWNER))      = 'ATT'; --P_FN_OWNER;

        IF L_EXIST = 1 THEN
          RETURN TRUE;
        ELSE
          RETURN FALSE;
        END IF;
    EXCEPTION
      WHEN OTHERS THEN
        PRC_GRAVA_LOG( P_DT_INCLUSAO  => SYSDATE
                      ,P_STATUS       => 'E'
                      ,P_OBSERVACAO   => 'TABELA: '                             || G_TABLE ||
                                         ' NAO EXISTE - '                       ||
                                         ' ERRO NA FUNCAO: FN_VALIDA_TABELA - ' || SQLERRM
                      ,P_MODULE       => G_MODULE
                      ,P_OS_USER      => G_OS_USER
                      ,P_TERMINAL     => G_TERMINAL
                      ,P_CURRENT_USER => G_CURRENT_USER
                      ,P_IP_ADDRESS   => G_IP_ADDRESS );
                      
        RETURN FALSE;
    END FN_VALIDA_TABELA;

    FUNCTION FN_EXECUTE(P_SP_DDL IN CLOB) RETURN BOOLEAN IS
       L_CURSOR     PLS_INTEGER := DBMS_SQL.OPEN_CURSOR;
     L_FEEDBACK   PLS_INTEGER;
     
           R_RETURN_EXECUTE BOOLEAN:= FALSE;
      L_EXIST NUMBER := 0;
    BEGIN
      --
      -- EXECUTE IMMEDIATE P_SP_DDL;
      BEGIN
        EXECUTE IMMEDIATE TO_CLOB(P_SP_DDL);
        
        SELECT 1
          INTO L_EXIST
          FROM ALL_TABLES DT
         WHERE DT.OWNER      = G_OWNER
           AND DT.TABLE_NAME = 'EXT_TB_' || G_TABLE;
           
         IF L_EXIST = 1 THEN
           R_RETURN_EXECUTE := TRUE;
         ELSE
           RAISE E_NOT_EXIST;
           R_RETURN_EXECUTE := FALSE;         
         END IF;
      END;
      --
  \*    DBMS_SQL.PARSE(L_CURSOR, P_SP_DDL,DBMS_SQL.NATIVE);
      L_FEEDBACK := DBMS_SQL.EXECUTE (L_CURSOR);    
      DBMS_SQL.CLOSE_CURSOR (L_CURSOR);*\
    EXCEPTION
      WHEN E_NOT_EXIST THEN 
        PRC_GRAVA_LOG( P_DT_INCLUSAO  => SYSDATE
                      ,P_STATUS       => 'E'
                      ,P_OBSERVACAO   => 'TABELA EXTERNA: EXT_TB_'        || G_TABLE ||
                                         ' NAO FOI CRIADA - '             || 
                                         ' ERRO NA FUNCAO: FN_EXECUTE - ' || SQLERRM
                      ,P_MODULE       => G_MODULE
                      ,P_OS_USER      => G_OS_USER
                      ,P_TERMINAL     => G_TERMINAL
                      ,P_CURRENT_USER => G_CURRENT_USER
                      ,P_IP_ADDRESS   => G_IP_ADDRESS
                      );
        R_RETURN_EXECUTE := FALSE; 
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
        R_RETURN_EXECUTE := FALSE;        
    END FN_EXECUTE;

    FUNCTION FN_CARGA_ARQUIVO 
    RETURN BOOLEAN IS
      R_RETURN_CARGA BOOLEAN := FALSE;  
    
      L_CONTEUDO CLOB := EMPTY_CLOB(); --VARCHAR2(10000); -- 8788
      L_COUNT NUMBER := 0;
      l_byte number;
    BEGIN
      DBMS_OUTPUT.ENABLE (1000000);
    
      -- se a tabela fisica existir no dicionario de dados
      IF FN_VALIDA_TABELA(G_TABLE, G_OWNER) THEN
        G_DDL_CREATE_TABLE := G_DDL_CREATE_TABLE || G_TABLE || CHR(13) || '(';

        DBMS_LOB.CREATETEMPORARY(G_DDL_COLUMNN_TABLE,TRUE,DBMS_LOB.SESSION);
        DBMS_LOB.OPEN(G_DDL_COLUMNN_TABLE, DBMS_LOB.LOB_READWRITE);

        FOR RG_TAB_DIN IN ( SELECT DTC.COLUMN_NAME
                              FROM ALL_TAB_COLS DTC
                             WHERE DTC.TABLE_NAME = G_TABLE
                               AND DTC.OWNER      = G_OWNER
                             ORDER BY 1 )
        LOOP
           --
           --G_DDL_COLUMNN_TABLE := G_DDL_COLUMNN_TABLE || TO_CHAR(RG_TAB_DIN.COLUMN_NAME) || ' VARCHAR2(2000), ';
           --G_DDL_COLUMNN_TABLE := G_DDL_COLUMNN_TABLE || TO_CLOB(TO_CHAR(RG_TAB_DIN.COLUMN_NAME) || ' VARCHAR2(2000), ');
           --
           --G_SQL := G_SQL || TO_CHAR(RG_TAB_DIN.COLUMN_NAME) || ' VARCHAR2(2000), ';
           --DBMS_LOB.WRITE(G_DDL_COLUMNN_TABLE,LENGTH(G_SQL),1,G_SQL);
           --L_COUNT := L_COUNT + 1;
               
           G_SQL := G_SQL || TO_CHAR(RG_TAB_DIN.COLUMN_NAME) || ' VARCHAR2(2000), ';
                                      
           WHILE L_LEN < 32767
           LOOP
             L_LEN := L_LEN + LENGTH(G_SQL);
                    
             DBMS_LOB.WRITE(G_DDL_COLUMNN_TABLE,LENGTH(G_SQL),1,G_SQL);         
           END LOOP;
           --
           -- ALTER TABLE

           L_COUNT := L_COUNT + 1;         
        END LOOP;

  \*      L_CONTEUDO := TO_CLOB( REPLACE( G_DDL_CREATE_TABLE ||
                               G_DDL_COLUMNN_TABLE || ')' || CHR(13) ||
                               REPLACE(G_EXT_CONST,'<FILENAME>','''''' || G_NAME || '''''')
                              ,'), )',') )') );*\

        L_CONTEUDO :=  REPLACE( G_DDL_CREATE_TABLE ||
                               TO_CLOB(G_DDL_COLUMNN_TABLE || ')') || CHR(13) ||
                               REPLACE(G_EXT_CONST,'<FILENAME>','''''' || G_NAME || '''''')
                              ,'), )',') )') ;
                              
        dbms_output.put_line(to_char(L_COUNT));
        dbms_output.put_line(L_CONTEUDO);
                                            
        --DBMS_OUTPUT.PUT_LINE('Tamanho do CLOB: '||DBMS_LOB.GETLENGTH(G_DDL_COLUMNN_TABLE)||', conteudo: '||DBMS_LOB.SUBSTR(G_DDL_COLUMNN_TABLE));
        --l_byte := DBMS_LOB.GETLENGTH(G_DDL_COLUMNN_TABLE);
        --dbms_output.put_line(to_char(l_byte));
        --DBMS_OUTPUT.PUT_LINE('conteudo: '||DBMS_LOB.SUBSTR(G_DDL_COLUMNN_TABLE));
        --
        --EXEC_SQL.PARSE(connection_id, cursor_number, sql_string, exec_sql.V7);
        
        DBMS_LOB.CLOSE(G_DDL_COLUMNN_TABLE);
        DBMS_LOB.FREETEMPORARY(G_DDL_COLUMNN_TABLE);
        
        --SP_EXECUTE( '''' || TRIM(L_CONTEUDO) || '''');
        
        IF FN_EXECUTE(TRIM(L_CONTEUDO)) THEN
          R_RETURN_CARGA := TRUE;
        ELSE
          RAISE E_ERROR_CARGA_ARQUIVO;
          R_RETURN_CARGA := FALSE;
        END IF;
      ELSE
        DBMS_OUTPUT.PUT_LINE('N');
        R_RETURN_CARGA := FALSE;
      END IF;
    EXCEPTION
      WHEN E_ERROR_CARGA_ARQUIVO THEN
        PRC_GRAVA_LOG( P_DT_INCLUSAO  => SYSDATE
                      ,P_STATUS       => 'E'
                      ,P_OBSERVACAO   => ' ARQUIVO NAO FOI CARREGADO '          ||
                                         ' NAO FOI CRIADA - '                   || 
                                         ' ERRO NA FUNCAO: FN_CARGA_ARQUIVO - ' || SQLERRM 
                      ,P_MODULE       => G_MODULE
                      ,P_OS_USER      => G_OS_USER
                      ,P_TERMINAL     => G_TERMINAL
                      ,P_CURRENT_USER => G_CURRENT_USER
                      ,P_IP_ADDRESS    => G_IP_ADDRESS );
                              
        R_RETURN_CARGA := FALSE;
      WHEN OTHERS THEN
        PRC_GRAVA_LOG( P_DT_INCLUSAO  => SYSDATE
                      ,P_STATUS       => 'E'
                      ,P_OBSERVACAO   => ' ARQUIVO NAO FOI CARREGADO '          ||
                                         ' NAO FOI CRIADA - '                   || 
                                         ' ERRO NA FUNCAO: FN_CARGA_ARQUIVO - ' || SQLERRM 
                      ,P_MODULE       => G_MODULE
                      ,P_OS_USER      => G_OS_USER
                      ,P_TERMINAL     => G_TERMINAL
                      ,P_CURRENT_USER => G_CURRENT_USER
                      ,P_IP_ADDRESS    => G_IP_ADDRESS );
                      
        R_RETURN_CARGA := FALSE;
      
    END FN_CARGA_ARQUIVO;*/
    
    -- PROCEDURE PRC_TRATA_ARQUIVO
    FUNCTION FN_TRATA_ARQUIVO
      RETURN BOOLEAN
        --R_VALIDA BOOLEAN;
    IS
      CURSOR C_TRATA_DADOS IS
        SELECT DECODE(TO_NUMBER(TPO_DADO),1,2)                                                         AS TPO_DADO       
              ,TO_NUMBER(COD_EMPRS)                                                                    AS COD_EMPRS         
              ,NUM_RGTRO_EMPRG                                                                         AS NUM_RGTRO_EMPRG -- 
              ,NOM_EMPRG                                                                               AS NOM_EMPRG    
              ,TO_DATE(REPLACE(DTA_EMISS,'Fev','Feb'),'DD/MM/RRRR')                                    AS DTA_EMISS 
              ,TO_NUMBER(NUM_FOLHA)                                                                    AS NUM_FOLHA
              ,TRIM(DCR_PLANO)                                                                         AS DCR_PLANO 
              ,PER_INIC_EXTR                                                                           AS PER_INIC_EXTR -- 
              ,PER_FIM_EXTR                                                                            AS PER_FIM_EXTR  -- 
              ,TO_DATE(REPLACE(DTA_INIC_EXTR,'Dez','Dec'),'DD/MM/RRRR')                                AS DTA_INIC_EXTR          
              ,TO_DATE(DTA_FIM_EXTR,'DD/MM/RRRR')                                                      AS DTA_FIM_EXTR
              ,TRIM(DCR_SLD_MOV_SALDADO)                                                               AS DCR_SLD_MOV_SALDADO
              ,TO_NUMBER(REPLACE(REPLACE(SLD_PL_SALDADO_MOV_INIC,'.',''),',','.'))                     AS SLD_PL_SALDADO_MOV_INIC 
              ,TO_NUMBER(REPLACE(REPLACE(CTB_PL_SALDADO_MOV,',',''),',','.'))                          AS CTB_PL_SALDADO_MOV
              ,TO_NUMBER(REPLACE(REPLACE(RENT_PL_SALDADO_MOV,',',''),',','.'))                         AS RENT_PL_SALDADO_MOV
              ,TO_NUMBER(REPLACE(REPLACE(SLD_PL_SALDADO_MOV_FIM,'.',''),',','.'))                      AS SLD_PL_SALDADO_MOV_FIM
              ,TRIM(DCR_SLD_MOV_BD)                                                                    AS DCR_SLD_MOV_BD
              ,TO_NUMBER(REPLACE(REPLACE(SLD_PL_BD_INIC,'.',''),',','.'))                              AS SLD_PL_BD_INIC
              ,TO_NUMBER(REPLACE(REPLACE(CTB_PL_MOV_BD,'.',''),',','.'))                               AS CTB_PL_MOV_BD
              ,TO_NUMBER(REPLACE(REPLACE(RENT_PL_MOV_BD,'.',''),',','.'))                              AS RENT_PL_MOV_BD
              ,TO_NUMBER(REPLACE(REPLACE(SLD_PL_BD_MOV_FIM,'.',''),',','.'))                           AS SLD_PL_BD_MOV_FIM
              ,TRIM(DCR_SLD_MOV_CV)                                                                    AS DCR_SLD_MOV_CV
              ,TO_NUMBER(REPLACE(REPLACE(SLD_PL_CV_MOV_INIC,'.',''),',','.'))                          AS SLD_PL_CV_MOV_INIC
              ,TO_NUMBER(REPLACE(REPLACE(CTB_PL_MOV_CV,'.',''),',','.'))                               AS CTB_PL_MOV_CV
              ,TO_NUMBER(REPLACE(REPLACE(RENT_PL_MOV_CV,'.',''),',','.'))                              AS RENT_PL_MOV_CV
              ,TO_NUMBER(REPLACE(REPLACE(SLD_PL_CV_MOV_FIM,'.',''),',','.'))                           AS SLD_PL_CV_MOV_FIM
              ,TRIM(DCR_CTA_OBRIG_PARTIC)                                                              AS DCR_CTA_OBRIG_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_OBRIG_PARTIC,'.',''),',','.'))                        AS SLD_CTA_OBRIG_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(CTB_CTA_OBRIG_PARTIC,'.',''),',','.'))                        AS CTB_CTA_OBRIG_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(RENT_CTA_OBRIG_PARTIC,'.',''),',','.'))                       AS RENT_CTA_OBRIG_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_OBRIG_PARTIC_FIM,'.',''),',','.'))                    AS SLD_CTA_OBRIG_PARTIC_FIM
              ,TRIM(DCR_CTA_NORM_PATROC)                                                               AS DCR_CTA_NORM_PATROC 
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_NORM_PATROC,'.',''),',','.'))                         AS SLD_CTA_NORM_PATROC  
              ,TO_NUMBER(REPLACE(REPLACE(CTB_CTA_NORM_PATROC,'.',''),',','.'))                         AS CTB_CTA_NORM_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(RENT_NORM_PATROC,'.',''),',','.'))                            AS RENT_NORM_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_NORM_PATROC_INIC,'.',''),',','.'))                        AS SLD_NORM_PATROC_INIC                                                 
              ,TRIM(DCR_CTA_ESPEC_PARTIC)                                                              AS DCR_CTA_ESPEC_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_ESPEC_PARTIC,'.',''),',','.'))                        AS SLD_CTA_ESPEC_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(CTB_CTA_ESPEC_PARTIC,'.',''),',','.'))                        AS CTB_CTA_ESPEC_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(RENT_CTA_ESPEC_PARTIC,'.',''),',','.'))                       AS RENT_CTA_ESPEC_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_ESPEC_PARTIC_INIC,'.',''),',','.'))                   AS SLD_CTA_ESPEC_PARTIC_INIC
              ,TRIM(DCR_CTA_ESPEC_PATROC)                                                              AS DCR_CTA_ESPEC_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_ESPEC_PATROC,'.',''),',','.'))                        AS SLD_CTA_ESPEC_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(CTB_CTA_ESPEC_PATROC,'.',''),',','.'))                        AS CTB_CTA_ESPEC_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(RENT_CTA_ESPEC_PATROC,'.',''),',','.'))                       AS RENT_CTA_ESPEC_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_CTA_ESPEC_PATROC_INIC,'.',''),',','.'))                   AS SLD_CTA_ESPEC_PATROC_INIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_TOT_INIC,'.',''),',','.'))                                AS SLD_TOT_INIC
              ,TO_NUMBER(REPLACE(REPLACE(CTB_TOT_INIC,'.',''),',','.'))                                AS CTB_TOT_INIC
              ,TO_NUMBER(REPLACE(REPLACE(RENT_PERIODO,'.',''),',','.'))                                AS RENT_PERIODO
              ,TO_NUMBER(REPLACE(REPLACE(SLD_TOT_FIM,'.',''),',','.'))                                 AS SLD_TOT_FIM
              ,TRIM(PRM_MES_PERIODO_CTB)                                                               AS PRM_MES_PERIODO_CTB
              ,TRIM(SEG_MES_PERIODO_CTB)                                                               AS SEG_MES_PERIODO_CTB
              ,TRIM(TER_MES_PERIODO_CTB)                                                               AS TER_MES_PERIODO_CTB
              ,TRIM(DCR_TOT_CTB_BD)                                                                    AS DCR_TOT_CTB_BD
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_BD_PRM_MES,'.',''),',','.'))                      AS VLR_TOT_CTB_BD_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_BD_SEG_MES,'.',''),',','.'))                      AS VLR_TOT_CTB_BD_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_BD_TER_MES,'.',''),',','.'))                      AS VLR_TOT_CTB_BD_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_BD_PERIODO,'.',''),',','.'))                      AS VLR_TOT_CTB_BD_PERIODO
              ,TRIM(DCR_TOT_CTB_CV)                                                                    AS DCR_TOT_CTB_CV
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_CV_PRM_MES,'.',''),',','.'))                      AS VLR_TOT_CTB_CV_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_CV_SEG_MES,'.',''),',','.'))                      AS VLR_TOT_CTB_CV_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_CV_TER_MES,'.',''),',','.'))                      AS VLR_TOT_CTB_CV_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_CV_PERIODO,'.',''),',','.'))                      AS VLR_TOT_CTB_CV_PERIODO
              ,TRIM(DCR_TPO_CTB_VOL_PARTIC)                                                            AS DCR_TPO_CTB_VOL_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PARTIC_PRM_MES,'.',''),',','.'))                  AS VLR_CTB_VOL_PARTIC_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PARTIC_SEG_MES,'.',''),',','.'))                  AS VLR_CTB_VOL_PARTIC_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PARTIC_TER_MES,'.',''),',','.'))                  AS VLR_CTB_VOL_PARTIC_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PARTIC_PERIODO,'.',''),',','.'))                  AS VLR_CTB_VOL_PARTIC_PERIODO
              ,TRIM(DCR_TPO_CTB_VOL_PATROC)                                                            AS DCR_TPO_CTB_VOL_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PATROC_PRM_MES,'.',''),',','.'))                  AS VLR_CTB_VOL_PATROC_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PATROC_SEG_MES,'.',''),',','.'))                  AS VLR_CTB_VOL_PATROC_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PATROC_TER_MES,'.',''),',','.'))                  AS VLR_CTB_VOL_PATROC_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_VOL_PATROC_PERIODO,'.',''),',','.'))                  AS VLR_CTB_VOL_PATROC_PERIODO
              ,TRIM(DCR_TPO_CTB_OBRIG_PARTIC)                                                          AS DCR_TPO_CTB_OBRIG_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PARTIC_PRM_MES,'.',''),',','.'))                AS VLR_CTB_OBRIG_PARTIC_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PARTIC_SEG_MES,'.',''),',','.'))                AS VLR_CTB_OBRIG_PARTIC_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PARTIC_TER_MES,'.',''),',','.'))                AS VLR_CTB_OBRIG_PARTIC_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PARTIC_PERIODO,'.',''),',','.'))                AS VLR_CTB_OBRIG_PARTIC_PERIODO
              ,TRIM(DCR_TPO_CTB_OBRIG_PATROC)                                                          AS DCR_TPO_CTB_OBRIG_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PATROC_PRM_MES,'.',''),',','.'))                AS VLR_CTB_OBRIG_PATROC_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PATROC_SEG_MES,'.',''),',','.'))                AS VLR_CTB_OBRIG_PATROC_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PATROC_TER_MES,'.',''),',','.'))                AS VLR_CTB_OBRIG_PATROC_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_OBRIG_PATROC_PERIODO,'.',''),',','.'))                AS VLR_CTB_OBRIG_PATROC_PERIODO
              ,TRIM(DCR_TPO_CTB_ESPOR_PATROC)                                                          AS DCR_TPO_CTB_ESPOR_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PATROC_PRM_MES,'.',''),',','.'))                AS VLR_CTB_ESPOR_PATROC_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PATROC_SEG_MES,'.',''),',','.'))                AS VLR_CTB_ESPOR_PATROC_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PATROC_TER_MES,'.',''),',','.'))                AS VLR_CTB_ESPOR_PATROC_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PATROC_PERIODO,'.',''),',','.'))                AS VLR_CTB_ESPOR_PATROC_PERIODO
              ,TRIM(DCR_TPO_CTB_ESPOR_PARTIC)                                                          AS DCR_TPO_CTB_ESPOR_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PARTIC_PRM_MES,'.',''),',','.'))                AS VLR_CTB_ESPOR_PARTIC_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PARTIC_SEG_MES,'.',''),',','.'))                AS VLR_CTB_ESPOR_PARTIC_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PARTIC_TER_MES,'.',''),',','.'))                AS VLR_CTB_ESPOR_PARTIC_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_ESPOR_PARTIC_PERIODO,'.',''),',','.'))                AS VLR_CTB_ESPOR_PARTIC_PERIODO
              ,TO_NUMBER(REPLACE(REPLACE(TOT_CTB_PRM_MES,'.',''),',','.'))                             AS TOT_CTB_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(TOT_CTB_SEG_MES,'.',''),',','.'))                             AS TOT_CTB_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(TOT_CTB_TER_MES,'.',''),',','.'))                             AS TOT_CTB_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(TOT_CTB_EXTRATO,'.',''),',','.'))                             AS TOT_CTB_EXTRATO
              ,TRIM(PRM_MES_PERIODO_RENT)                                                              AS PRM_MES_PERIODO_RENT
              ,TRIM(SEG_MES_PERIODO_RENT)                                                              AS SEG_MES_PERIODO_RENT
              ,TRIM(TER_MES_PERIODO_RENT)                                                              AS TER_MES_PERIODO_RENT
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_REAL_PRM_MES,'.',''),',','.'))                       AS PCT_RENT_REAL_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_REAL_SEG_MES,'.',''),',','.'))                       AS PCT_RENT_REAL_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_REAL_TER_MES,'.',''),',','.'))                       AS PCT_RENT_REAL_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_REAL_TOT_MES,'.',''),',','.'))                       AS PCT_RENT_REAL_TOT_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_LMTD_PRM_MES,'.',''),',','.'))                       AS PCT_RENT_LMTD_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_LMTD_SEG_MES,'.',''),',','.'))                       AS PCT_RENT_LMTD_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_LMTD_TER_MES,'.',''),',','.'))                       AS PCT_RENT_LMTD_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_LMTD_TOT_MES,'.',''),',','.'))                       AS PCT_RENT_LMTD_TOT_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_IGPDI_PRM_MES,'.',''),',','.'))                      AS PCT_RENT_IGPDI_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_IGPDI_SEG_MES,'.',''),',','.'))                      AS PCT_RENT_IGPDI_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_IGPDI_TER_MES,'.',''),',','.'))                      AS PCT_RENT_IGPDI_TER_MES   
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_IGPDI_TOT_MES,'.',''),',','.'))                      AS PCT_RENT_IGPDI_TOT_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_URR_PRM_MES,'.',''),',','.'))                        AS PCT_RENT_URR_PRM_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_URR_SEG_MES,'.',''),',','.'))                        AS PCT_RENT_URR_SEG_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_URR_TER_MES,'.',''),',','.'))                        AS PCT_RENT_URR_TER_MES
              ,TO_NUMBER(REPLACE(REPLACE(PCT_RENT_URR_TOT_MES,'.',''),',','.'))                        AS PCT_RENT_URR_TOT_MES
              ,TO_DATE(DTA_APOS_PROP,'DD/MM/RRRR')                                                     AS DTA_APOS_PROP
              ,TO_DATE(DTA_APOS_INTE,'DD/MM/RRRR')                                                     AS DTA_APOS_INTE
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_PSAP_PROP,'.',''),',','.'))                         AS VLR_BENEF_PSAP_PROP
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_PSAP_INTE,'.',''),',','.'))                         AS VLR_BENEF_PSAP_INTE
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_BD_PROP,'.',''),',','.'))                           AS VLR_BENEF_BD_PROP
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_BD_INTE,'.',''),',','.'))                           AS VLR_BENEF_BD_INTE
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_CV_PROP,'.',''),',','.'))                           AS VLR_BENEF_CV_PROP
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_CV_INTE,'.',''),',','.'))                           AS VLR_BENEF_CV_INTE
              ,TO_NUMBER(REPLACE(REPLACE(RENDA_ESTIM_PROP,'.',''),',','.'))                            AS RENDA_ESTIM_PROP
              ,TO_NUMBER(REPLACE(REPLACE(RENDA_ESTIM_INT,'.',''),',','.'))                             AS RENDA_ESTIM_INT
              ,TO_NUMBER(REPLACE(REPLACE(VLR_RESERV_SALD_LQDA,'.',''),',','.'))                        AS VLR_RESERV_SALD_LQDA
              ,TRIM(TXT_PRM_MENS)                                                                      AS TXT_PRM_MENS
              ,TRIM(TXT_SEG_MENS)                                                                      AS TXT_SEG_MENS
              ,TRIM(TXT_TER_MENS)                                                                      AS TXT_TER_MENS
              ,TRIM(TXT_QUA_MENS)                                                                      AS TXT_QUA_MENS
              ,TO_NUMBER(IDADE_PROP_BSPS)                                                              AS IDADE_PROP_BSPS
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_PROP_BSPS,'.',''),',','.'))                           AS VLR_CTB_PROP_BSPS
              ,TO_NUMBER(IDADE_INT_BSPS)                                                               AS IDADE_INT_BSPS
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_INT_BSPS,'.',''),',','.'))                            AS VLR_CTB_INT_BSPS
              ,TO_NUMBER(IDADE_PROP_BD)                                                                AS IDADE_PROP_BD
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_PROP_BD,'.',''),',','.'))                             AS VLR_CTB_PROP_BD
              ,TO_NUMBER(IDADE_INT_BD)                                                                 AS IDADE_INT_BD
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_INT_BD,'.',''),',','.'))                              AS VLR_CTB_INT_BD
              ,TO_NUMBER(IDADE_PROP_CV)                                                                AS IDADE_PROP_CV
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_PROP_CV,'.',''),',','.'))                             AS VLR_CTB_PROP_CV
              ,TO_NUMBER(IDADE_INT_CV)                                                                 AS IDADE_INT_CV
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CTB_INT_CV,'.',''),',','.'))                              AS VLR_CTB_INT_CV
              ,TRIM(DCR_COTA_INDEX_PLAN_1)                                                             AS DCR_COTA_INDEX_PLAN_1
              ,TRIM(DCR_COTA_INDEX_PLAN_2)                                                             AS DCR_COTA_INDEX_PLAN_2
              ,TRIM(DCR_CTA_APOS_INDIV_VOL_PARTIC)                                                     AS DCR_CTA_APOS_INDIV_VOL_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INI_CTA_APO_INDI_VOL_PARTI,'.',''),',','.'))              AS SLD_INI_CTA_APO_INDI_VOL_PARTI
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_APO_INDI_VOL_PARTI,'.',''),',','.'))              AS VLR_TOT_CTB_APO_INDI_VOL_PARTI
              ,TO_NUMBER(REPLACE(REPLACE(REN_TOT_CTB_APO_INDI_VOL_PARTI,'.',''),',','.'))              AS REN_TOT_CTB_APO_INDI_VOL_PARTI
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_APO_INDI_VOL_PARTI,'.',''),',','.'))              AS SLD_FIM_CTA_APO_INDI_VOL_PARTI
              ,TRIM(DCR_CTA_APOS_INDIV_ESPO_PARTIC)                                                    AS DCR_CTA_APOS_INDIV_ESPO_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INI_CTA_APO_INDI_ESPOPARTI,'.',''),',','.'))              AS SLD_INI_CTA_APO_INDI_ESPOPARTI
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_APO_INDI_ESPOPARTI,'.',''),',','.'))              AS VLR_TOT_CTB_APO_INDI_ESPOPARTI
              ,TO_NUMBER(REPLACE(REPLACE(REN_TOT_CTB_APO_INDI_ESPOPARTI,'.',''),',','.'))              AS REN_TOT_CTB_APO_INDI_ESPOPARTI
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_APO_INDI_ESPOPARTI,'.',''),',','.'))              AS SLD_FIM_CTA_APO_INDI_ESPOPARTI
              ,TRIM(DCR_CTA_APOS_INDIV_VOL_PATROC)                                                     AS DCR_CTA_APOS_INDIV_VOL_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INI_CTA_APO_INDI_VOL_PATRO,'.',''),',','.'))              AS SLD_INI_CTA_APO_INDI_VOL_PATRO
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_APO_INDI_VOL_PATRO,'.',''),',','.'))              AS VLR_TOT_CTB_APO_INDI_VOL_PATRO
              ,TO_NUMBER(REPLACE(REPLACE(REN_TOT_CTB_APO_INDI_VOL_PATRO,'.',''),',','.'))              AS REN_TOT_CTB_APO_INDI_VOL_PATRO
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_APO_INDI_VOL_PATRO,'.',''),',','.'))              AS SLD_FIM_CTA_APO_INDI_VOL_PATRO
              ,TRIM(DCR_CTA_APOS_INDIV_SUPL_PATROC)                                                    AS DCR_CTA_APOS_INDIV_SUPL_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INI_CTA_APO_INDI_SUPLPATRO,'.',''),',','.'))              AS SLD_INI_CTA_APO_INDI_SUPLPATRO
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_APO_INDI_SUPLPATRO,'.',''),',','.'))              AS VLR_TOT_CTB_APO_INDI_SUPLPATRO
              ,TO_NUMBER(REPLACE(REPLACE(REN_TOT_CTB_APO_INDI_SUPLPATRO,'.',''),',','.'))              AS REN_TOT_CTB_APO_INDI_SUPLPATRO
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_APO_INDI_SUPLPATRO,'.',''),',','.'))              AS SLD_FIM_CTA_APO_INDI_SUPLPATRO
              ,TRIM(DCR_PORT_TOTAL)                                                                    AS DCR_PORT_TOTAL
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INIC_CTA_PORT_TOT,'.',''),',','.'))                       AS SLD_INIC_CTA_PORT_TOT
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_PORT_TOT,'.',''),',','.'))                        AS VLR_TOT_CTB_PORT_TOT
              ,TO_NUMBER(REPLACE(REPLACE(RENT_TOT_CTB_PORT_TOT,'.',''),',','.'))                       AS RENT_TOT_CTB_PORT_TOT
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_PORT_TOT,'.',''),',','.'))                        AS SLD_FIM_CTA_PORT_TOT
              ,TRIM(DCR_PORT_ABERTA)                                                                   AS DCR_PORT_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INIC_CTA_PORT_ABERTA,'.',''),',','.'))                    AS SLD_INIC_CTA_PORT_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_PORT_ABERTA,'.',''),',','.'))                     AS VLR_TOT_CTB_PORT_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(RENT_TOT_CTB_PORT_ABERTA,'.',''),',','.'))                    AS RENT_TOT_CTB_PORT_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_PORT_ABERTA,'.',''),',','.'))                     AS SLD_FIM_CTA_PORT_ABERTA
              ,TRIM(DCR_PORT_FECHADA)                                                                  AS DCR_PORT_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INIC_CTA_PORT_FECHADA,'.',''),',','.'))                   AS SLD_INIC_CTA_PORT_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_PORT_FECHADA,'.',''),',','.'))                    AS VLR_TOT_CTB_PORT_FECHADA      
              ,TO_NUMBER(REPLACE(REPLACE(RENT_TOT_CTB_PORT_FECHADA,'.',''),',','.'))                   AS RENT_TOT_CTB_PORT_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_PORT_FECHADA,'.',''),',','.'))                    AS SLD_FIM_CTA_PORT_FECHADA
              ,TRIM(DCR_PORT_JOIA_ABERTA)                                                              AS DCR_PORT_JOIA_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INIC_CTA_PORT_JOIA_ABERTA,'.',''),',','.'))               AS SLD_INIC_CTA_PORT_JOIA_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_PORT_JOIA_ABERTA,'.',''),',','.'))                AS VLR_TOT_CTB_PORT_JOIA_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(RENT_TOT_CTB_PORT_JOIA_ABERTA,'.',''),',','.'))               AS RENT_TOT_CTB_PORT_JOIA_ABERTA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_PORT_JOIA_ABERTA,'.',''),',','.'))                AS SLD_FIM_CTA_PORT_JOIA_ABERTA
              ,TRIM(DCR_PORT_JOIA_FECHADA)                                                             AS DCR_PORT_JOIA_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INIC_CTA_PORT_JOIA_FECHADA,'.',''),',','.'))              AS SLD_INIC_CTA_PORT_JOIA_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_PORT_JOIA_FECHADA,'.',''),',','.'))               AS VLR_TOT_CTB_PORT_JOIA_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(RENT_TOT_CTB_PORT_JOIA_FECHADA,'.',''),',','.'))              AS RENT_TOT_CTB_PORT_JOIA_FECHADA
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_PORT_JOIA_FECHADA,'.',''),',','.'))               AS SLD_FIM_CTA_PORT_JOIA_FECHADA
              ,TRIM(DCR_DISTR_FUND_PREV_PARTIC)                                                        AS DCR_DISTR_FUND_PREV_PARTIC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INI_DIST_FUND_PREV_PARTI,'.',''),',','.'))                AS SLD_INI_DIST_FUND_PREV_PARTI
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_DIST_FUND_PREV_PARTI,'.',''),',','.'))                AS VLR_TOT_DIST_FUND_PREV_PARTI
              ,TO_NUMBER(REPLACE(REPLACE(REN_TOT_DIST_FUND_PREV_PARTI,'.',''),',','.'))                AS REN_TOT_DIST_FUND_PREV_PARTI
              ,TO_NUMBER(REPLACE(REPLACE(SLDFIM_CTA_DISTFUNDPREVPARTI,'.',''),',','.'))                AS SLDFIM_CTA_DISTFUNDPREVPARTI
              ,TRIM(DCR_DISTR_FUND_PREV_PATROC)                                                        AS DCR_DISTR_FUND_PREV_PATROC
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INI_DIST_FUND_PREV_PATRO,'.',''),',','.'))                AS SLD_INI_DIST_FUND_PREV_PATRO
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_DIST_FUND_PREV_PATRO,'.',''),',','.'))                AS VLR_TOT_DIST_FUND_PREV_PATRO           
              ,TO_NUMBER(REPLACE(REPLACE(REN_TOT_DIST_FUND_PREV_PATRO,'.',''),',','.'))                AS REN_TOT_DIST_FUND_PREV_PATRO
              ,TO_NUMBER(REPLACE(REPLACE(SLDFIM_CTA_DISTFUNDPREVPATRO,'.',''),',','.'))                AS SLDFIM_CTA_DISTFUNDPREVPATRO
              ,TRIM(DCR_PORT_FINAL)                                                                    AS DCR_PORT_FINAL
              ,TO_NUMBER(REPLACE(REPLACE(SLD_INIC_CTA_PORT_FIM,'.',''),',','.'))                       AS SLD_INIC_CTA_PORT_FIM
              ,TO_NUMBER(REPLACE(REPLACE(VLR_TOT_CTB_PORT_FIM,'.',''),',','.'))                        AS VLR_TOT_CTB_PORT_FIM
              ,TO_NUMBER(REPLACE(REPLACE(RENT_TOT_CTB_PORT_FIM,'.',''),',','.'))                       AS RENT_TOT_CTB_PORT_FIM
              ,TO_NUMBER(REPLACE(REPLACE(SLD_FIM_CTA_PORT_FIM,'.',''),',','.'))                        AS SLD_FIM_CTA_PORT_FIM
              ,TRIM(DCR_SLD_PROJETADO)                                                                 AS DCR_SLD_PROJETADO
              ,TO_NUMBER(REPLACE(REPLACE(REPLACE(E.VLR_SLD_PROJETADO, '.',''), CHR(13), ''), ',','.')) AS VLR_SLD_PROJETADO
              ,TO_NUMBER(REPLACE(REPLACE(VLR_SLD_ADICIONAL,'.',''),',','.'))                           AS VLR_SLD_ADICIONAL
              ,TO_NUMBER(REPLACE(REPLACE(VLR_BENEF_ADICIONAL,'.',''),',','.'))                         AS VLR_BENEF_ADICIONAL 
              ,TO_DATE(DTA_ULT_ATUAL,'DD/MM/RRRR')                                                     AS DTA_ULT_ATUAL
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CONTRIB_RISCO,'.',''),',','.'))                           AS VLR_CONTRIB_RISCO
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CONTRIB_PATRC,'.',''),',','.'))                           AS VLR_CONTRIB_PATRC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CAPIT_SEGURADO,'.',''),',','.'))                          AS VLR_CAPIT_SEGURADO
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CONTRIB_ADM,'.',''),',','.'))                             AS VLR_CONTRIB_ADM
              ,TO_NUMBER(REPLACE(REPLACE(VLR_CONTRIB_ADM_PATRC,'.',''),',','.'))                       AS VLR_CONTRIB_ADM_PATRC
              ,TO_NUMBER(REPLACE(REPLACE(VLR_SIMUL_BENEF_PORCETAGEM,'.',''),',','.'))                  AS VLR_SIMUL_BENEF_PORCETAGEM
              ,TO_DATE(DTA_ELEGIB_BENEF_PORCETAGEM,'DD/MM/RRRR')                                       AS DTA_ELEGIB_BENEF_PORCETAGEM
              ,TO_NUMBER(REPLACE(REPLACE(IDADE_ELEGIB_PORCETAGEM,'.',''),',','.'))                     AS IDADE_ELEGIB_PORCETAGEM
              ,TO_DATE(DTA_EXAURIM_BENEF_PORCETAGEM,'DD/MM/RRRR')                                      AS DTA_EXAURIM_BENEF_PORCETAGEM
              ,TO_NUMBER(REPLACE(REPLACE(VLR_SIMUL_BENEF_PRAZO,'.',''),',','.'))                       AS VLR_SIMUL_BENEF_PRAZO
              ,TO_DATE(DTA_ELEGIB_BENEF_PRAZO,'DD/MM/RRRR')                                            AS DTA_ELEGIB_BENEF_PRAZO
              ,TO_NUMBER(REPLACE(REPLACE(IDADE_ELEGIB_BENEF_PRAZO,'.',''),',','.'))                    AS IDADE_ELEGIB_BENEF_PRAZO
              ,TO_DATE(DTA_EXAURIM_BENEF_PRAZO,'DD/MM/RRRR')                                           AS DTA_EXAURIM_BENEF_PRAZO           
        FROM F02860.EXT_TB_BASE_EXTRAT_CTB E     
        WHERE TPO_DADO = '1';
      
    BEGIN
      --
      DBMS_OUTPUT.PUT_LINE('PRC_TRATA_ARQUIVO');
      
      /*
      FOR RG_TRATA_DADOS IN C_TRATA_DADOS
      LOOP
        -- INSERT 
      END LOOP;
      */
      --
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END FN_TRATA_ARQUIVO;
    
       -- -------------------------------------------------------------------------------------------
       -- FUN_CALC_VLR
       -- Descricao: 
       -- -------------------------------------------------------------------------------------------    
       FUNCTION FUN_CALC_VLR(  P_NUM_MATR  ATT.HIST_VALOR_BNF.NUM_MATR_PARTF          %TYPE
                              ,P_DTA_FIM   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE
                              ,P_NUM_PLBNF ATT.HIST_VALOR_BNF.NUM_PLBNF               %TYPE DEFAULT NULL
                              ,P_DESC_PLBNF ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                              ,P_CALC      NUMBER
                               --
                              ,P_COD_EMPRS  ATT.EMPRESA.COD_EMPRS%TYPE
                              --,P_DT_MOV     DATE
                              ,P_COD_NATBNF ATT.HIST_VALOR_BNF.COD_NATBNF%TYPE
                              ,P_NUM_CTFSS  ATT.SLD_CONTA_PARTIC_FSS.NUM_CTFSS%TYPE
                              ,P_COD_UM     ATT.SLD_CONTA_PARTIC_FSS.COD_UM%TYPE
                            )
        RETURN NUMBER IS
          R_VLR_BENEF1       ATT.HIST_VALOR_BNF.VLR_BENEF1_HTBNF%TYPE;
          R_RENDA_ESTIM_PROP ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_PROP%TYPE;
          R_RENDA_ESTIM_INT  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_INT%TYPE;
          R_RES1             NUMBER(17,5);
          R_RES2             ATT.COTACAO_DIA_UM.VLR_CDIAUM%TYPE;
          R_RES3             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_SLD_ADICIONAL%TYPE;
          --
              R_VLR_BENEF_BD_PROP   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_PROP%TYPE;
              R_VLR_BENEF_BD_INTE   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_INTE%TYPE;
              --R_RENDA_ESTIM_PROP    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_PROP%TYPE;
              --R_RENDA_ESTIM_INT     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_INT%TYPE;          
        BEGIN
          --
          IF P_COD_EMPRS IN (40, 60) THEN
            -- CALCULO PARA AS EMPRESAS: ELETROPAULO/TIM
            
            IF (P_CALC = 1) THEN
              -- CALCULA VLR_DR
              --
              SELECT NVL(MAX(VLR_BENEF1_HTBNF),0)
                INTO R_VLR_BENEF1
                FROM HIST_VALOR_BNF
               WHERE NUM_MATR_PARTF = P_NUM_MATR
                 AND COD_NATBNF     = P_COD_NATBNF -- 4
                 AND NUM_PLBNF      = P_NUM_PLBNF
                 AND TO_CHAR(DAT_INIVG_HTBNF,'YYYYMM') = TO_CHAR(P_DTA_FIM,'YYYYMM');            

              IF (R_VLR_BENEF1 IS NOT NULL) THEN
                RETURN R_VLR_BENEF1;
              END IF;
              --
            ELSIF (P_CALC = 2) THEN
              -- CALCULA VLR_DU
              --
              SELECT NVL(SUM(VLR_BENEF_PSAP_PROP + VLR_BENEF_BD_PROP + VLR_BENEF_CV_PROP),0)
                INTO R_RENDA_ESTIM_PROP
                FROM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB P
               INNER JOIN ATT.PARTICIPANTE_FSS      Y  ON Y.COD_EMPRS       = P.COD_EMPRS
                                                      AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(P.NUM_RGTRO_EMPRG,1,LENGTH(P.NUM_RGTRO_EMPRG) - 2))
               WHERE P.COD_EMPRS        = P_COD_EMPRS
                 AND UPPER(P.DCR_PLANO) = UPPER(P_DESC_PLBNF)
                 AND P.DTA_FIM_EXTR     = P_DTA_FIM
                 AND Y.NUM_MATR_PARTF   = P_NUM_MATR;
                 
              IF (R_RENDA_ESTIM_PROP IS NOT NULL) THEN
                RETURN R_RENDA_ESTIM_PROP;
              END IF;
              --
            ELSIF (P_CALC = 3) THEN
              -- CALCULA VLR_DV
              --
              SELECT NVL(SUM(VLR_BENEF_PSAP_INTE + VLR_BENEF_BD_INTE + VLR_BENEF_CV_INTE),0)
                INTO R_RENDA_ESTIM_INT
                FROM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB P
               INNER JOIN ATT.PARTICIPANTE_FSS           Y  ON Y.COD_EMPRS       = P.COD_EMPRS
                                                           AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(P.NUM_RGTRO_EMPRG,1,LENGTH(P.NUM_RGTRO_EMPRG) - 2))
               WHERE P.COD_EMPRS        = P_COD_EMPRS
                 AND UPPER(P.DCR_PLANO) = UPPER(P_DESC_PLBNF)
                 AND P.DTA_FIM_EXTR     = P_DTA_FIM
                 AND Y.NUM_MATR_PARTF   = P_NUM_MATR;
                 
              IF (R_RENDA_ESTIM_INT IS NOT NULL) THEN
                RETURN R_RENDA_ESTIM_INT;
              END IF;
              --
            ELSE
              --
              SELECT NVL(SUM(SCPF.VLR_SDANT_SDCTPR + SCPF.VLR_ENTMES_SDCTPR - SCPF.VLR_SAIMES_SDCTPR),0) AS VLR_RES1
                INTO R_RES1
                FROM  ATT.SLD_CONTA_PARTIC_FSS    SCPF
                     ,ATT.PARTICIPANTE_FSS        PF  
                     ,ATT.ADESAO_PLANO_PARTIC_FSS APPF
                     ,ATT.CONTA_FSS               CF  
               WHERE SCPF.NUM_MATR_PARTF = PF.NUM_MATR_PARTF
                 AND SCPF.NUM_CTFSS      = CF.NUM_CTFSS
                 AND SCPF.COD_UM         = CF.COD_UMARMZ_CTFSS
                 AND PF.NUM_MATR_PARTF   = APPF.NUM_MATR_PARTF
                 --
                 AND APPF.NUM_PLBNF  = P_NUM_PLBNF -- 19
                 AND SCPF.NUM_CTFSS  = P_NUM_CTFSS -- 976
                 AND SCPF.COD_UM     = P_COD_UM    -- 248
                 AND PF.COD_EMPRS    = P_COD_EMPRS -- IN (40,60)
                 AND SCPF.ANOMES_MOVIM_SDCTPR = TO_NUMBER(TRUNC(TO_CHAR(P_DTA_FIM,'YYYYMM')));
              --
              SELECT NVL(MAX(A.VLR_CDIAUM),0) AS VLR_CDIAUM
                INTO R_RES2
                FROM COTACAO_DIA_UM A
               WHERE A.COD_UM     = P_COD_UM -- 248
                 AND A.DAT_CDIAUM = TO_DATE(TRUNC(P_DTA_FIM));
               
              IF (     R_RES1 IS NOT NULL
                   AND R_RES2 IS NOT NULL ) THEN
                R_RES3 := ROUND(R_RES1 * R_RES2);
                RETURN NVL(R_RES3,0);
              END IF;
            END IF;
            --
         ELSE
           -- CALCULO PARA A EMPRESA: TIETE
            IF (P_CALC = 1) THEN -- Valor do BDS - Modulo Saldado:                
              -- CALCULA VLR_BENEF_BD_PROP
              --
              SELECT REPLACE(FPT.VLR_BENEF_BD_PROP,FPT.VLR_BENEF_BD_PROP,0)AS VLR_BENEF_BD_PROP
                 INTO R_VLR_BENEF_BD_PROP
                FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB FPT
               WHERE DCR_PLANO          = P_DESC_PLBNF    --'PSAP/Eletropaulo'
                AND FPT.DTA_FIM_EXTR    = P_DTA_FIM      --TO_DATE('31/12/2020','DD/MM/RRRR')
                AND FPT.NUM_RGTRO_EMPRG = P_NUM_MATR     --'0000096920-7' -- EXEMPLO DE TESTE --> Livia Nascimento Silva
                AND FPT.COD_EMPRS       = P_COD_EMPRS;  --44

              IF (R_VLR_BENEF_BD_PROP IS NOT NULL) THEN
                RETURN  R_VLR_BENEF_BD_PROP;
              END IF;

            ELSIF (P_CALC = 2) THEN -- Valor do BDS - Modulo Saldado:              
            -- CALCULA VLR_BENEF_BD_INTE
            --
              SELECT BS.VLR_BNF1TT_BNFSLD
                 INTO R_VLR_BENEF_BD_INTE
                FROM ATT.BENEFICIO_SALDADO BS
                INNER JOIN ATT.PARTICIPANTE_FSS P
                                            ON (P.NUM_MATR_PARTF = BS.NUM_MATR_PARTF)
              WHERE P.NUM_MATR_PARTF = P_NUM_MATR   --91687
              AND   BS.NUM_PLBNF     = P_NUM_PLBNF;  --31;

              IF (R_VLR_BENEF_BD_INTE IS NOT NULL) THEN
                RETURN R_VLR_BENEF_BD_INTE;
              END IF;

            ELSIF (P_CALC = 3) THEN -- Valor Total dos Beneficios:
            --RENDA_ESTIM_PROP
            --
              SELECT NVL(SUM(FPT.VLR_BENEF_PSAP_PROP + FPT.VLR_BENEF_BD_PROP + FPT.VLR_BENEF_CV_PROP),0) AS RENDA_ESTIM_PROP
                 INTO R_RENDA_ESTIM_PROP
                FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB FPT
                 INNER JOIN ATT.PARTICIPANTE_FSS Y ON Y.COD_EMPRS = FPT.COD_EMPRS
                                        AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(FPT.NUM_RGTRO_EMPRG,1,LENGTH(FPT.NUM_RGTRO_EMPRG) - 2))
                     WHERE FPT.COD_EMPRS        = P_COD_EMPRS --44
                       AND UPPER(FPT.DCR_PLANO) = P_DESC_PLBNF   --UPPER('PSAP/Eletropaulo) -- Tim
                       AND Y.NUM_MATR_PARTF     = P_NUM_MATR    --91687
                       AND FPT.DTA_FIM_EXTR     = P_DTA_FIM;     --TO_DATE('31/12/2020','DD/MM/YYYY');

              IF (R_RENDA_ESTIM_PROP IS NOT NULL) THEN
                RETURN R_RENDA_ESTIM_PROP;
              END IF;

            ELSE --(P_CALC = 4)THEN -- Valor Total dos Beneficios:
            --RENDA_ESTIM_INT
            --
              SELECT NVL(SUM(FPT.VLR_BENEF_PSAP_INTE + FPT.VLR_BENEF_BD_INTE + FPT.VLR_BENEF_CV_INTE),0) AS RENDA_ESTIM_PROP
                INTO R_RENDA_ESTIM_INT
               FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB FPT
                 INNER JOIN ATT.PARTICIPANTE_FSS Y  ON Y.COD_EMPRS = FPT.COD_EMPRS
                                        AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(FPT.NUM_RGTRO_EMPRG,1,LENGTH(FPT.NUM_RGTRO_EMPRG) - 2))
                       WHERE FPT.COD_EMPRS        = P_COD_EMPRS --44
                         AND UPPER(FPT.DCR_PLANO) = P_DESC_PLBNF   --UPPER('PSAP/Eletropaulo') Tim
                         AND Y.NUM_MATR_PARTF     = P_NUM_MATR    --91687
                         AND FPT.DTA_FIM_EXTR     = P_DTA_FIM;     --TO_DATE('31/12/2020','DD/MM/YYYY');

              IF (R_RENDA_ESTIM_INT IS NOT NULL) THEN
                RETURN R_RENDA_ESTIM_INT;
              END IF;

            END IF;
         END IF;
        EXCEPTION
          WHEN OTHERS THEN
            --RETURN NULL;
            DBMS_OUTPUT.put_line(SQLCODE || ' - ' || SQLERRM);
        END FUN_CALC_VLR;
        
   -- PATROCINADORA: PSAP/TIETE
   PROCEDURE PROC_EXT_PREV_TIETE(P_COD_EMPRESA   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE,
                                 P_DCR_PLANO     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE,
                                 P_DTA_MOV       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE)IS

    BEGIN
        DECLARE

           L_DTA_FIM     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE;
           --L_NUM_PLBNF   ATT.BENEFICIO_SALDADO.NUM_PLBNF%TYPE:=31;

         TYPE REC_BASE IS RECORD(   VLR_BENEF_BD_PROP   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_PROP%TYPE
                                   ,VLR_BENEF_BD_INTE   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_INTE%TYPE
                                   ,RENDA_ESTIM_PROP    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_PROP%TYPE
                                   ,RENDA_ESTIM_INT     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_INT%TYPE
                                   ,VLR_CTB_PROP_BD     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_PROP_BD%TYPE
                                   ,VLR_CTB_INT_BD      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_INT_BD%TYPE
                                 );

           TB_REC_BASE REC_BASE;

           L_COUNT NUMBER :=0;
           L_C_UPD NUMBER :=0;

         CURSOR C_BASE( P_CR_CODEMPRS  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                       ,P_CR_DCR_PLANO ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                       ,P_DTA_FIM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE ) IS
              SELECT X.COD_EMPRS       AS COD_EMPRS
                    ,X.NUM_RGTRO_EMPRG AS NUM_RGTRO_EMPRG
                    ,X.DCR_PLANO       AS DCR_PLANO
                    ,Y.NUM_MATR_PARTF  AS NUM_MATR_PARTF
                    ,31                AS COD_PLANO
                     --
                    ,X.TPO_DADO
                FROM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB X
               INNER JOIN ATT.PARTICIPANTE_FSS           Y  ON Y.COD_EMPRS = X.COD_EMPRS
                                                     AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(X.NUM_RGTRO_EMPRG,1,LENGTH(X.NUM_RGTRO_EMPRG) - 2))
               WHERE X.COD_EMPRS        = P_CR_CODEMPRS
                 AND UPPER(X.DCR_PLANO) = UPPER(P_CR_DCR_PLANO)
                 --AND Y.NUM_MATR_PARTF   = 91687
                 AND X.DTA_FIM_EXTR     = P_DTA_FIM;


      BEGIN

            IF (P_DTA_MOV IS NULL)THEN
            -- FC_PRE_TBL_BASE_EXTRAT_CTB
            -- PEGA MAIOR DATA
            SELECT MAX(DTA_FIM_EXTR)AS DTA_FIM_EXTR
               INTO L_DTA_FIM
               FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB
             WHERE COD_EMPRS        = P_COD_EMPRESA --44
              AND UPPER(DCR_PLANO)  = P_DCR_PLANO;   --UPPER('PSAP/Eletropaulo')
            --
            ELSE
            --
              L_DTA_FIM := P_DTA_MOV;

            END IF;

            FOR RG IN C_BASE( P_COD_EMPRESA
                             ,P_DCR_PLANO
                             ,L_DTA_FIM)

            LOOP
            L_COUNT := L_COUNT + 1;
            --
            /*
            TB_REC_BASE.VLR_BENEF_BD_PROP      := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, C_DESC_PLANO_ELETROPAULO, 1, P_COD_EMPRESA, --VLR_BENEF_BD_PROP
            TB_REC_BASE.VLR_BENEF_BD_INTE      := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, 2); --VLR_BENEF_BD_INTE
            TB_REC_BASE.RENDA_ESTIM_PROP       := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, 3); --RENDA_ESTIM_PROP
            TB_REC_BASE.RENDA_ESTIM_INT        := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, 4); --RENDA_ESTIM_INT
            TB_REC_BASE.VLR_CTB_PROP_BD        := 0;                                             --VLR_CTB_PROP_BD
            TB_REC_BASE.VLR_CTB_INT_BD         := ATT.FCESP_VLR_CTB_ASSIST(RG.COD_PLANO, TB_REC_BASE.VLR_BENEF_BD_INTE); --VLR_CTB_INT_BD
            */

           UPDATE ATT.FC_PRE_TBL_BASE_EXTRAT_CTB
              SET  VLR_BENEF_BD_PROP  = TB_REC_BASE.VLR_BENEF_BD_PROP
                  ,VLR_BENEF_BD_INTE  = TB_REC_BASE.VLR_BENEF_BD_INTE
                  ,RENDA_ESTIM_PROP   = TB_REC_BASE.RENDA_ESTIM_PROP
                  ,RENDA_ESTIM_INT    = TB_REC_BASE.RENDA_ESTIM_INT
                  ,VLR_CTB_PROP_BD    = TB_REC_BASE.VLR_CTB_PROP_BD
                  ,VLR_CTB_INT_BD     = TB_REC_BASE.VLR_CTB_INT_BD 
               --
              WHERE TPO_DADO          = RG.TPO_DADO
                AND COD_EMPRS         = RG.COD_EMPRS
                AND NUM_RGTRO_EMPRG   = RG.NUM_RGTRO_EMPRG
                AND DTA_FIM_EXTR      = L_DTA_FIM;

            L_C_UPD := SQL%ROWCOUNT;

            IF L_C_UPD > 0 THEN

               IF (L_COUNT = L_C_UPD) THEN
                  DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: '||TO_CHAR(L_C_UPD));
               END IF;

            END IF;
            --DBMS_OUTPUT.PUT_LINE(TB_REC_BASE.VLR_BENEF_BD_PROP);
            END LOOP;
            --
      END;

        EXCEPTION
          WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
           DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);



   END PROC_EXT_PREV_TIETE;
   --
   --   
   
  -- PATROCINADORA: (PSAP/ELETROPAULO - TIM)
  PROCEDURE PRE_PRC_EXT_PREV_ELETROPAULO(PCOD_EMPRESA ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE,
                                         PDCR_PLANO   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE,
                                         PDTA_MOV     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL)
  IS
  BEGIN
    DECLARE
      L_DTA_FIM    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE;
      --L_NUM_PLBNF  ATT.ADESAO_PLANO_PARTIC_FSS.NUM_PLBNF%TYPE := 19;
      L_NUM_CTFSS  ATT.SLD_CONTA_PARTIC_FSS.NUM_CTFSS%TYPE    := 976;
      L_COD_UM     ATT.SLD_CONTA_PARTIC_FSS.COD_UM%TYPE       := 248;
      L_COD_NATBNF ATT.HIST_VALOR_BNF.COD_NATBNF%TYPE         := 4;
      
      TYPE REC_BASE IS RECORD ( VLR_BENEF_BD_PROP   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_PROP%TYPE   -- VLR_DQ 
                               ,VLR_BENEF_BD_INTE   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_INTE%TYPE   -- VLR_DR 
                               ,RENDA_ESTIM_PROP    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_PROP%TYPE    -- VLR_DU 
                               ,RENDA_ESTIM_INT     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_INT%TYPE     -- VLR_DV 
                               ,VLR_CTB_INT_BD      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_INT_BD%TYPE      -- VLR_EI 
                               ,VLR_CTB_PROP_BD     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_PROP_BD%TYPE     -- VLR_EG 
                               ,VLR_SLD_ADICIONAL   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_SLD_ADICIONAL%TYPE   -- VLR_RES1 * VLR_RES2 = VLR_RES3
                               ,VLR_BENEF_ADICIONAL ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_ADICIONAL%TYPE -- VLR_RES3 / 130
                              );

      TB_REC_BASE REC_BASE;

      L_C_INS  NUMBER := 0;
      L_C_UPD  NUMBER := 0;

      CURSOR C_BASE( P_CR_CODEMPRS  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                    ,P_CR_DCR_PLANO ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                    ,P_DTA_FIM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE ) IS
        SELECT X.COD_EMPRS       AS COD_EMPRS
              ,X.NUM_RGTRO_EMPRG AS NUM_RGTRO_EMPRG
              ,X.DCR_PLANO       AS DCR_PLANO
              ,Y.NUM_MATR_PARTF  AS NUM_MATR_PARTF
              ,19                AS COD_PLANO
               --
              ,X.TPO_DADO
          FROM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB X
         INNER JOIN ATT.PARTICIPANTE_FSS           Y  ON Y.COD_EMPRS = X.COD_EMPRS
                                                     AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(X.NUM_RGTRO_EMPRG,1,LENGTH(X.NUM_RGTRO_EMPRG) - 2))
         WHERE X.COD_EMPRS        = P_CR_CODEMPRS
           AND UPPER(X.DCR_PLANO) = UPPER(P_CR_DCR_PLANO)
           --AND Y.NUM_MATR_PARTF   = 79910
           AND X.DTA_FIM_EXTR     = P_DTA_FIM;
  -- ----------------------------------------------------------------------------------------------------
    BEGIN
      IF (PDTA_MOV IS NULL) THEN
        -- FC_PRE_TBL_BASE_EXTRAT_CTB
        -- PEGA A MAIOR DATA
        SELECT MAX(X.DTA_FIM_EXTR) AS MAX_DTA_FIM_EXTR
          INTO L_DTA_FIM
          FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB X
        WHERE X.COD_EMPRS         = PCOD_EMPRESA
           AND UPPER(X.DCR_PLANO) = UPPER(PDCR_PLANO);
        --
      ELSE
        --
        L_DTA_FIM := PDTA_MOV;
        --
      END IF;
      --
      --
      FOR RG IN C_BASE( PCOD_EMPRESA
                       ,PDCR_PLANO
                       ,L_DTA_FIM )
      LOOP
        L_C_INS := L_C_INS + 1;
        --
        TB_REC_BASE.VLR_BENEF_BD_INTE   := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, C_DESC_PLANO_ELETROPAULO, 1, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM); -- VLR_DR
        TB_REC_BASE.RENDA_ESTIM_PROP    := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, C_DESC_PLANO_ELETROPAULO, 2, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM);         -- VLR_DU
        TB_REC_BASE.RENDA_ESTIM_INT     := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, C_DESC_PLANO_ELETROPAULO, 3, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM);         -- VLR_DV
        TB_REC_BASE.VLR_CTB_INT_BD      := ATT.FCESP_VLR_CTB_ASSIST(RG.COD_PLANO, TB_REC_BASE.VLR_BENEF_BD_INTE); -- EI
        TB_REC_BASE.VLR_CTB_PROP_BD     := 0;                                                           -- EG
        TB_REC_BASE.VLR_SLD_ADICIONAL   := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, C_DESC_PLANO_ELETROPAULO, 4, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM);         -- RES3
        TB_REC_BASE.VLR_BENEF_ADICIONAL := TB_REC_BASE.VLR_SLD_ADICIONAL / 130;                         -- RES4
        --
        UPDATE ATT.FC_PRE_TBL_BASE_EXTRAT_CTB A
           SET  VLR_BENEF_BD_PROP   = 0
               ,VLR_BENEF_BD_INTE   = TB_REC_BASE.VLR_BENEF_BD_INTE
               ,RENDA_ESTIM_PROP    = TB_REC_BASE.RENDA_ESTIM_PROP
               ,RENDA_ESTIM_INT     = TB_REC_BASE.RENDA_ESTIM_INT
               ,VLR_CTB_INT_BD      = TB_REC_BASE.VLR_CTB_INT_BD
               ,VLR_CTB_PROP_BD     = 0
               ,VLR_SLD_ADICIONAL   = TB_REC_BASE.VLR_SLD_ADICIONAL
               ,VLR_BENEF_ADICIONAL = TB_REC_BASE.VLR_SLD_ADICIONAL / 130
          WHERE TPO_DADO        = RG.TPO_DADO
            AND COD_EMPRS       = RG.COD_EMPRS
            AND NUM_RGTRO_EMPRG = RG.NUM_RGTRO_EMPRG
            AND DTA_FIM_EXTR    = L_DTA_FIM;
          
          L_C_UPD := SQL%ROWCOUNT;
          
          IF L_C_UPD > 0 THEN
            IF (L_C_INS = L_C_UPD) THEN
              DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: ' || TO_CHAR(L_C_UPD));
            END IF;
          END IF;
      END LOOP;
      --
    END;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(SQLCODE || ' - ' || SQLERRM);

  END PRE_PRC_EXT_PREV_ELETROPAULO;

  PROCEDURE PRE_INICIA_PROCESSAMENTO( P_PRC_PROCESSO NUMBER -- 1: ELETROPAULO / 2: TIM / 3: TIETE 
                                     ,P_PRC_DATA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE)
  IS
  BEGIN
     IF (P_PRC_PROCESSO = 1) THEN
        --
        PRE_PRC_EXT_PREV_ELETROPAULO(40,'PSAP/ELETROPAULO', P_PRC_DATA); -- ELETROPAULO
        --
        ELSIF (P_PRC_PROCESSO = 2) THEN
        --        
        PRE_PRC_EXT_PREV_ELETROPAULO(60,'PSAP/ELETROPAULO', P_PRC_DATA); -- TIM
        --
        ELSIF (P_PRC_PROCESSO = 3) THEN
        --        
        PROC_EXT_PREV_TIETE(44,'PSAP/TIETE', P_PRC_DATA); -- TIETE
        --
        ELSE
        --
        DBMS_OUTPUT.PUT_LINE('4');
        --
      END IF;
  END PRE_INICIA_PROCESSAMENTO;
  
END PKG_EXT_PREVIDENCIARIO;
/
