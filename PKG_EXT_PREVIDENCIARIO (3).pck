CREATE OR REPLACE PACKAGE OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO IS

 -- VARIAVEIS
  G_HOST_NAME    VARCHAR2(64);
  
  G_MODULE       VARCHAR2(255) := '';
  G_OS_USER      VARCHAR2(255) := '';
  G_TERMINAL     VARCHAR2(255) := '';
  G_CURRENT_USER VARCHAR2(255) := '';
  G_IP_ADDRESS   VARCHAR2(255) := '';

  G_ARQ UTL_FILE.FILE_TYPE;
  G_DIR VARCHAR2(50)           := '/dados/oracle/NEWDEV/work';
  G_NAME VARCHAR2(255)         := 'GeracaoExtratoCorreio_440_031_012021.txt'; 
  G_READ CHAR(1)               := 'R';
  G_SIZE NUMBER                := 32767;
  --G_DDL_CREATE_TABLE CLOB; 
  --G_SEPARADOR CHAR(1) :=';';
  --G_DDL_EXECUTE BOOLEAN;
  
   
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
                           );    
    
    --PROCEDURE PRC_CARGA_ARQUIVO(P_NAME VARCHAR2);  
    
    PROCEDURE PRC_CARGA_ARQUIVO;
        
    --FUNCTION FN_TRATA_ARQUIVO RETURN BOOLEAN;       
    
    PROCEDURE PROC_EXT_PREV_TIETE(  P_COD_EMPRESA   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                                   ,P_DCR_PLANO     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                                   ,P_DTA_MOV       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL);


    PROCEDURE PRE_PRC_EXT_PREV_ELETROPAULO(  PCOD_EMPRESA ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                                            ,PDCR_PLANO   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                                            ,PDTA_MOV     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL);

   
    PROCEDURE PRE_INICIA_PROCESSAMENTO( P_PRC_PROCESSO NUMBER DEFAULT NULL
                                        ,P_PRC_DATA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL);


 
END PKG_EXT_PREVIDENCIARIO;
/
CREATE OR REPLACE PACKAGE BODY OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO IS

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SISTEMA     : AMADEUS CAPITALIZACAO
-- DESCRICAO   : GERACAO DOS EXTRATO PREVIDENCIARIO DOS SALDADOS - PSAP/ELETROPAULO, TIM, PSAP/TIETE
-- ANALISTA    : ADRIANO LIMA
-- DATA CRIACAO: 26/04/2021
-- MANUTENCOES : 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                           

    PROCEDURE PRC_INICIALIZA_VARIAVEIS IS
    
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
      -- TABELA DE LOG
      INSERT INTO OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO (  COD_LOG_CARGA_EXTRATO
                                                          ,TPO_DADO
                                                          ,COD_EMPRS
                                                          ,NUM_RGTRO_EMPRG
                                                          ,DTA_FIM_EXTR
                                                          ,QTD_LINHAS
                                                          ,DT_INCLUSAO
                                                          ,STATUS
                                                          ,OBSERVACAO
                                                           --
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
                                                         --           
                                                        ,P_MODULE               
                                                        ,P_OS_USER              
                                                        ,P_TERMINAL             
                                                        ,P_CURRENT_USER         
                                                        ,P_IP_ADDRESS
                                                       );
                                                       
    END PRC_GRAVA_LOG;
    
    
    PROCEDURE PRC_CARGA_ARQUIVO IS
    
       L_REGISTRO      CLOB;
       L_CONTEUDO      CLOB;
       L_IDX           NUMBER := 0;
       L_INSTR_I       NUMBER := 0;
       L_QTD_SEPARADOR NUMBER := 0;
             
      -- CRIAR UM TYPE RECORD
       TYPE REC_CARGA IS RECORD( TPO_DADO                          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TPO_DADO%TYPE
                                ,COD_EMPRS                         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                                ,NUM_RGTRO_EMPRG                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.NUM_RGTRO_EMPRG%TYPE
                                ,NOM_EMPRG                         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.NOM_EMPRG%TYPE
                                ,DTA_EMISS                         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_EMISS%TYPE
                                ,NUM_FOLHA                         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.NUM_FOLHA%TYPE
                                ,DCR_PLANO                         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                                ,PER_INIC_EXTR                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PER_INIC_EXTR%TYPE
                                ,PER_FIM_EXTR                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PER_FIM_EXTR%TYPE
                                ,DTA_INIC_EXTR                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_INIC_EXTR%TYPE
                                ,DTA_FIM_EXTR                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE
                                ,DCR_SLD_MOV_SALDADO               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_SLD_MOV_SALDADO%TYPE
                                ,SLD_PL_SALDADO_MOV_INIC           ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_PL_SALDADO_MOV_INIC%TYPE
                                ,CTB_PL_SALDADO_MOV                ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_PL_SALDADO_MOV%TYPE
                                ,RENT_PL_SALDADO_MOV               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_PL_SALDADO_MOV%TYPE
                                ,SLD_PL_SALDADO_MOV_FIM            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_PL_SALDADO_MOV_FIM%TYPE
                                ,DCR_SLD_MOV_BD                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_SLD_MOV_BD%TYPE
                                ,SLD_PL_BD_INIC                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_PL_BD_INIC%TYPE
                                ,CTB_PL_MOV_BD                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_PL_MOV_BD%TYPE
                                ,RENT_PL_MOV_BD                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_PL_MOV_BD%TYPE
                                ,SLD_PL_BD_MOV_FIM                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_PL_BD_MOV_FIM%TYPE
                                ,DCR_SLD_MOV_CV                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_SLD_MOV_CV%TYPE
                                ,SLD_PL_CV_MOV_INIC                ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_PL_CV_MOV_INIC%TYPE
                                ,CTB_PL_MOV_CV                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_PL_MOV_CV%TYPE
                                ,RENT_PL_MOV_CV                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_PL_MOV_CV%TYPE
                                ,SLD_PL_CV_MOV_FIM                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_PL_CV_MOV_FIM%TYPE
                                ,DCR_CTA_OBRIG_PARTIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_OBRIG_PARTIC%TYPE
                                ,SLD_CTA_OBRIG_PARTIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_OBRIG_PARTIC%TYPE
                                ,CTB_CTA_OBRIG_PARTIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_CTA_OBRIG_PARTIC%TYPE
                                ,RENT_CTA_OBRIG_PARTIC             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_CTA_OBRIG_PARTIC%TYPE
                                ,SLD_CTA_OBRIG_PARTIC_FIM          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_OBRIG_PARTIC_FIM%TYPE
                                ,DCR_CTA_NORM_PATROC               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_NORM_PATROC%TYPE
                                ,SLD_CTA_NORM_PATROC               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_NORM_PATROC%TYPE
                                ,CTB_CTA_NORM_PATROC               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_CTA_NORM_PATROC%TYPE
                                ,RENT_NORM_PATROC                  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_NORM_PATROC%TYPE
                                ,SLD_NORM_PATROC_INIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_NORM_PATROC_INIC%TYPE
                                ,DCR_CTA_ESPEC_PARTIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_ESPEC_PARTIC%TYPE
                                ,SLD_CTA_ESPEC_PARTIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_ESPEC_PARTIC%TYPE
                                ,CTB_CTA_ESPEC_PARTIC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_CTA_ESPEC_PARTIC%TYPE
                                ,RENT_CTA_ESPEC_PARTIC             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_CTA_ESPEC_PARTIC%TYPE
                                ,SLD_CTA_ESPEC_PARTIC_INIC         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_ESPEC_PARTIC_INIC%TYPE
                                ,DCR_CTA_ESPEC_PATROC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_ESPEC_PATROC%TYPE
                                ,SLD_CTA_ESPEC_PATROC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_ESPEC_PATROC%TYPE
                                ,CTB_CTA_ESPEC_PATROC              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_CTA_ESPEC_PATROC%TYPE
                                ,RENT_CTA_ESPEC_PATROC             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_CTA_ESPEC_PATROC%TYPE
                                ,SLD_CTA_ESPEC_PATROC_INIC         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_CTA_ESPEC_PATROC_INIC%TYPE
                                ,SLD_TOT_INIC                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_TOT_INIC%TYPE
                                ,CTB_TOT_INIC                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.CTB_TOT_INIC%TYPE
                                ,RENT_PERIODO                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_PERIODO%TYPE
                                ,SLD_TOT_FIM                       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_TOT_FIM%TYPE
                                ,PRM_MES_PERIODO_CTB               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PRM_MES_PERIODO_CTB%TYPE
                                ,SEG_MES_PERIODO_CTB               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SEG_MES_PERIODO_CTB%TYPE
                                ,TER_MES_PERIODO_CTB               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TER_MES_PERIODO_CTB%TYPE
                                ,DCR_TOT_CTB_BD                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TOT_CTB_BD%TYPE
                                ,VLR_TOT_CTB_BD_PRM_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_BD_PRM_MES%TYPE
                                ,VLR_TOT_CTB_BD_SEG_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_BD_SEG_MES%TYPE
                                ,VLR_TOT_CTB_BD_TER_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_BD_TER_MES%TYPE
                                ,VLR_TOT_CTB_BD_PERIODO            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_BD_PERIODO%TYPE
                                ,DCR_TOT_CTB_CV                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TOT_CTB_CV%TYPE
                                ,VLR_TOT_CTB_CV_PRM_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_CV_PRM_MES%TYPE
                                ,VLR_TOT_CTB_CV_SEG_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_CV_SEG_MES%TYPE
                                ,VLR_TOT_CTB_CV_TER_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_CV_TER_MES%TYPE
                                ,VLR_TOT_CTB_CV_PERIODO            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_CV_PERIODO%TYPE
                                ,DCR_TPO_CTB_VOL_PARTIC            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TPO_CTB_VOL_PARTIC%TYPE
                                ,VLR_CTB_VOL_PARTIC_PRM_MES        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PARTIC_PRM_MES%TYPE
                                ,VLR_CTB_VOL_PARTIC_SEG_MES        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PARTIC_SEG_MES%TYPE
                                ,VLR_CTB_VOL_PARTIC_TER_MES        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PARTIC_TER_MES%TYPE
                                ,VLR_CTB_VOL_PARTIC_PERIODO        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PARTIC_PERIODO%TYPE
                                ,DCR_TPO_CTB_VOL_PATROC            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TPO_CTB_VOL_PATROC%TYPE
                                ,VLR_CTB_VOL_PATROC_PRM_MES        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PATROC_PRM_MES%TYPE
                                ,VLR_CTB_VOL_PATROC_SEG_MES        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PATROC_SEG_MES%TYPE
                                ,VLR_CTB_VOL_PATROC_TER_MES        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PATROC_TER_MES%TYPE
                                ,VLR_CTB_VOL_PATROC_PERIODO        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_VOL_PATROC_PERIODO%TYPE
                                ,DCR_TPO_CTB_OBRIG_PARTIC          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TPO_CTB_OBRIG_PARTIC%TYPE
                                ,VLR_CTB_OBRIG_PARTIC_PRM_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PARTIC_PRM_MES%TYPE
                                ,VLR_CTB_OBRIG_PARTIC_SEG_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PARTIC_SEG_MES%TYPE
                                ,VLR_CTB_OBRIG_PARTIC_TER_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PARTIC_TER_MES%TYPE
                                ,VLR_CTB_OBRIG_PARTIC_PERIODO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PARTIC_PERIODO%TYPE
                                ,DCR_TPO_CTB_OBRIG_PATROC          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TPO_CTB_OBRIG_PATROC%TYPE
                                ,VLR_CTB_OBRIG_PATROC_PRM_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PATROC_PRM_MES%TYPE
                                ,VLR_CTB_OBRIG_PATROC_SEG_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PATROC_SEG_MES%TYPE
                                ,VLR_CTB_OBRIG_PATROC_TER_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PATROC_TER_MES%TYPE
                                ,VLR_CTB_OBRIG_PATROC_PERIODO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_OBRIG_PATROC_PERIODO%TYPE
                                ,DCR_TPO_CTB_ESPOR_PATROC          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TPO_CTB_ESPOR_PATROC%TYPE
                                ,VLR_CTB_ESPOR_PATROC_PRM_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PATROC_PRM_MES%TYPE
                                ,VLR_CTB_ESPOR_PATROC_SEG_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PATROC_SEG_MES%TYPE
                                ,VLR_CTB_ESPOR_PATROC_TER_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PATROC_TER_MES%TYPE
                                ,VLR_CTB_ESPOR_PATROC_PERIODO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PATROC_PERIODO%TYPE
                                ,DCR_TPO_CTB_ESPOR_PARTIC          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_TPO_CTB_ESPOR_PARTIC%TYPE
                                ,VLR_CTB_ESPOR_PARTIC_PRM_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PARTIC_PRM_MES%TYPE
                                ,VLR_CTB_ESPOR_PARTIC_SEG_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PARTIC_SEG_MES%TYPE
                                ,VLR_CTB_ESPOR_PARTIC_TER_MES      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PARTIC_TER_MES%TYPE
                                ,VLR_CTB_ESPOR_PARTIC_PERIODO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_ESPOR_PARTIC_PERIODO%TYPE
                                ,TOT_CTB_PRM_MES                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TOT_CTB_PRM_MES%TYPE
                                ,TOT_CTB_SEG_MES                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TOT_CTB_SEG_MES%TYPE
                                ,TOT_CTB_TER_MES                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TOT_CTB_TER_MES%TYPE
                                ,TOT_CTB_EXTRATO                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TOT_CTB_EXTRATO%TYPE
                                ,PRM_MES_PERIODO_RENT              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PRM_MES_PERIODO_RENT%TYPE
                                ,SEG_MES_PERIODO_RENT              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SEG_MES_PERIODO_RENT%TYPE
                                ,TER_MES_PERIODO_RENT              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TER_MES_PERIODO_RENT%TYPE
                                ,PCT_RENT_REAL_PRM_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_REAL_PRM_MES%TYPE
                                ,PCT_RENT_REAL_SEG_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_REAL_SEG_MES%TYPE
                                ,PCT_RENT_REAL_TER_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_REAL_TER_MES%TYPE
                                ,PCT_RENT_REAL_TOT_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_REAL_TOT_MES%TYPE
                                ,PCT_RENT_LMTD_PRM_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_LMTD_PRM_MES%TYPE
                                ,PCT_RENT_LMTD_SEG_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_LMTD_SEG_MES%TYPE
                                ,PCT_RENT_LMTD_TER_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_LMTD_TER_MES%TYPE
                                ,PCT_RENT_LMTD_TOT_MES             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_LMTD_TOT_MES%TYPE
                                ,PCT_RENT_IGPDI_PRM_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_IGPDI_PRM_MES%TYPE
                                ,PCT_RENT_IGPDI_SEG_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_IGPDI_SEG_MES%TYPE
                                ,PCT_RENT_IGPDI_TER_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_IGPDI_TER_MES%TYPE
                                ,PCT_RENT_IGPDI_TOT_MES            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_IGPDI_TOT_MES%TYPE
                                ,PCT_RENT_URR_PRM_MES              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_URR_PRM_MES%TYPE
                                ,PCT_RENT_URR_SEG_MES              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_URR_SEG_MES%TYPE
                                ,PCT_RENT_URR_TER_MES              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_URR_TER_MES%TYPE
                                ,PCT_RENT_URR_TOT_MES              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.PCT_RENT_URR_TOT_MES%TYPE
                                ,DTA_APOS_PROP                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_APOS_PROP%TYPE
                                ,DTA_APOS_INTE                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_APOS_INTE%TYPE
                                ,VLR_BENEF_PSAP_PROP               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_PSAP_PROP%TYPE
                                ,VLR_BENEF_PSAP_INTE               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_PSAP_INTE%TYPE
                                ,VLR_BENEF_BD_PROP                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_PROP%TYPE
                                ,VLR_BENEF_BD_INTE                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_INTE%TYPE
                                ,VLR_BENEF_CV_PROP                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_CV_PROP%TYPE
                                ,VLR_BENEF_CV_INTE                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_CV_INTE%TYPE
                                ,RENDA_ESTIM_PROP                  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_PROP%TYPE
                                ,RENDA_ESTIM_INT                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENDA_ESTIM_INT%TYPE
                                ,VLR_RESERV_SALD_LQDA              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_RESERV_SALD_LQDA%TYPE
                                ,TXT_PRM_MENS                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TXT_PRM_MENS%TYPE
                                ,TXT_SEG_MENS                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TXT_SEG_MENS%TYPE
                                ,TXT_TER_MENS                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TXT_TER_MENS%TYPE
                                ,TXT_QUA_MENS                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TXT_QUA_MENS%TYPE
                                ,IDADE_PROP_BSPS                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_PROP_BSPS%TYPE
                                ,VLR_CTB_PROP_BSPS                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_PROP_BSPS%TYPE
                                ,IDADE_INT_BSPS                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_INT_BSPS%TYPE
                                ,VLR_CTB_INT_BSPS                  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_INT_BSPS%TYPE
                                ,IDADE_PROP_BD                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_PROP_BD%TYPE
                                ,VLR_CTB_PROP_BD                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_PROP_BD%TYPE
                                ,IDADE_INT_BD                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_INT_BD%TYPE
                                ,VLR_CTB_INT_BD                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_INT_BD%TYPE
                                ,IDADE_PROP_CV                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_PROP_CV%TYPE
                                ,VLR_CTB_PROP_CV                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_PROP_CV%TYPE
                                ,IDADE_INT_CV                      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_INT_CV%TYPE
                                ,VLR_CTB_INT_CV                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CTB_INT_CV%TYPE
                                ,DCR_COTA_INDEX_PLAN_1             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_COTA_INDEX_PLAN_1%TYPE
                                ,DCR_COTA_INDEX_PLAN_2             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_COTA_INDEX_PLAN_2%TYPE
                                ,DCR_CTA_APOS_INDIV_VOL_PARTIC     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_APOS_INDIV_VOL_PARTIC%TYPE
                                ,SLD_INI_CTA_APO_INDI_VOL_PARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INI_CTA_APO_INDI_VOL_PARTI%TYPE
                                ,VLR_TOT_CTB_APO_INDI_VOL_PARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_APO_INDI_VOL_PARTI%TYPE
                                ,REN_TOT_CTB_APO_INDI_VOL_PARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.REN_TOT_CTB_APO_INDI_VOL_PARTI%TYPE
                                ,SLD_FIM_CTA_APO_INDI_VOL_PARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_APO_INDI_VOL_PARTI%TYPE
                                ,DCR_CTA_APOS_INDIV_ESPO_PARTIC    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_APOS_INDIV_ESPO_PARTIC%TYPE
                                ,SLD_INI_CTA_APO_INDI_ESPOPARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INI_CTA_APO_INDI_ESPOPARTI%TYPE
                                ,VLR_TOT_CTB_APO_INDI_ESPOPARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_APO_INDI_ESPOPARTI%TYPE
                                ,REN_TOT_CTB_APO_INDI_ESPOPARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.REN_TOT_CTB_APO_INDI_ESPOPARTI%TYPE
                                ,SLD_FIM_CTA_APO_INDI_ESPOPARTI    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_APO_INDI_ESPOPARTI%TYPE
                                ,DCR_CTA_APOS_INDIV_VOL_PATROC     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_APOS_INDIV_VOL_PATROC%TYPE
                                ,SLD_INI_CTA_APO_INDI_VOL_PATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INI_CTA_APO_INDI_VOL_PATRO%TYPE
                                ,VLR_TOT_CTB_APO_INDI_VOL_PATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_APO_INDI_VOL_PATRO%TYPE
                                ,REN_TOT_CTB_APO_INDI_VOL_PATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.REN_TOT_CTB_APO_INDI_VOL_PATRO%TYPE
                                ,SLD_FIM_CTA_APO_INDI_VOL_PATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_APO_INDI_VOL_PATRO%TYPE
                                ,DCR_CTA_APOS_INDIV_SUPL_PATROC    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_CTA_APOS_INDIV_SUPL_PATROC%TYPE
                                ,SLD_INI_CTA_APO_INDI_SUPLPATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INI_CTA_APO_INDI_SUPLPATRO%TYPE
                                ,VLR_TOT_CTB_APO_INDI_SUPLPATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_APO_INDI_SUPLPATRO%TYPE
                                ,REN_TOT_CTB_APO_INDI_SUPLPATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.REN_TOT_CTB_APO_INDI_SUPLPATRO%TYPE
                                ,SLD_FIM_CTA_APO_INDI_SUPLPATRO    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_APO_INDI_SUPLPATRO%TYPE
                                ,DCR_PORT_TOTAL                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PORT_TOTAL%TYPE
                                ,SLD_INIC_CTA_PORT_TOT             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INIC_CTA_PORT_TOT%TYPE
                                ,VLR_TOT_CTB_PORT_TOT              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_PORT_TOT%TYPE
                                ,RENT_TOT_CTB_PORT_TOT             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_TOT_CTB_PORT_TOT%TYPE
                                ,SLD_FIM_CTA_PORT_TOT              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_PORT_TOT%TYPE
                                ,DCR_PORT_ABERTA                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PORT_ABERTA%TYPE
                                ,SLD_INIC_CTA_PORT_ABERTA          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INIC_CTA_PORT_ABERTA%TYPE
                                ,VLR_TOT_CTB_PORT_ABERTA           ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_PORT_ABERTA%TYPE
                                ,RENT_TOT_CTB_PORT_ABERTA          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_TOT_CTB_PORT_ABERTA%TYPE
                                ,SLD_FIM_CTA_PORT_ABERTA           ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_PORT_ABERTA%TYPE
                                ,DCR_PORT_FECHADA                  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PORT_FECHADA%TYPE
                                ,SLD_INIC_CTA_PORT_FECHADA         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INIC_CTA_PORT_FECHADA%TYPE
                                ,VLR_TOT_CTB_PORT_FECHADA          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_PORT_FECHADA%TYPE
                                ,RENT_TOT_CTB_PORT_FECHADA         ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_TOT_CTB_PORT_FECHADA%TYPE
                                ,SLD_FIM_CTA_PORT_FECHADA          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_PORT_FECHADA%TYPE
                                ,DCR_PORT_JOIA_ABERTA              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PORT_JOIA_ABERTA%TYPE
                                ,SLD_INIC_CTA_PORT_JOIA_ABERTA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INIC_CTA_PORT_JOIA_ABERTA%TYPE
                                ,VLR_TOT_CTB_PORT_JOIA_ABERTA      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_PORT_JOIA_ABERTA%TYPE
                                ,RENT_TOT_CTB_PORT_JOIA_ABERTA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_TOT_CTB_PORT_JOIA_ABERTA%TYPE
                                ,SLD_FIM_CTA_PORT_JOIA_ABERTA      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_PORT_JOIA_ABERTA%TYPE
                                ,DCR_PORT_JOIA_FECHADA             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PORT_JOIA_FECHADA%TYPE
                                ,SLD_INIC_CTA_PORT_JOIA_FECHADA    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INIC_CTA_PORT_JOIA_FECHADA%TYPE
                                ,VLR_TOT_CTB_PORT_JOIA_FECHADA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_PORT_JOIA_FECHADA%TYPE
                                ,RENT_TOT_CTB_PORT_JOIA_FECHADA    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_TOT_CTB_PORT_JOIA_FECHADA%TYPE
                                ,SLD_FIM_CTA_PORT_JOIA_FECHADA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_PORT_JOIA_FECHADA%TYPE
                                ,DCR_DISTR_FUND_PREV_PARTIC        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_DISTR_FUND_PREV_PARTIC%TYPE
                                ,SLD_INI_DIST_FUND_PREV_PARTI      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INI_DIST_FUND_PREV_PARTI%TYPE
                                ,VLR_TOT_DIST_FUND_PREV_PARTI      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_DIST_FUND_PREV_PARTI%TYPE
                                ,REN_TOT_DIST_FUND_PREV_PARTI      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.REN_TOT_DIST_FUND_PREV_PARTI%TYPE
                                ,SLDFIM_CTA_DISTFUNDPREVPARTI      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLDFIM_CTA_DISTFUNDPREVPARTI%TYPE
                                ,DCR_DISTR_FUND_PREV_PATROC        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_DISTR_FUND_PREV_PATROC%TYPE
                                ,SLD_INI_DIST_FUND_PREV_PATRO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INI_DIST_FUND_PREV_PATRO%TYPE
                                ,VLR_TOT_DIST_FUND_PREV_PATRO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_DIST_FUND_PREV_PATRO%TYPE
                                ,REN_TOT_DIST_FUND_PREV_PATRO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.REN_TOT_DIST_FUND_PREV_PATRO%TYPE
                                ,SLDFIM_CTA_DISTFUNDPREVPATRO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLDFIM_CTA_DISTFUNDPREVPATRO%TYPE
                                ,DCR_PORT_FINAL                    ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PORT_FINAL%TYPE
                                ,SLD_INIC_CTA_PORT_FIM             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_INIC_CTA_PORT_FIM%TYPE
                                ,VLR_TOT_CTB_PORT_FIM              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_TOT_CTB_PORT_FIM%TYPE
                                ,RENT_TOT_CTB_PORT_FIM             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.RENT_TOT_CTB_PORT_FIM%TYPE
                                ,SLD_FIM_CTA_PORT_FIM              ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.SLD_FIM_CTA_PORT_FIM%TYPE
                                ,DCR_SLD_PROJETADO                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_SLD_PROJETADO%TYPE
                                ,VLR_SLD_PROJETADO                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_SLD_PROJETADO%TYPE
                                ,VLR_SLD_ADICIONAL                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_SLD_ADICIONAL%TYPE
                                ,VLR_BENEF_ADICIONAL               ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_ADICIONAL%TYPE
                                ,DTA_ULT_ATUAL                     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_ULT_ATUAL%TYPE
                                ,VLR_CONTRIB_RISCO                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CONTRIB_RISCO%TYPE
                                ,VLR_CONTRIB_PATRC                 ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CONTRIB_PATRC%TYPE
                                ,VLR_CAPIT_SEGURADO                ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CAPIT_SEGURADO%TYPE
                                ,VLR_CONTRIB_ADM                   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CONTRIB_ADM%TYPE
                                ,VLR_CONTRIB_ADM_PATRC             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_CONTRIB_ADM_PATRC%TYPE
                                ,VLR_SIMUL_BENEF_PORCETAGEM        ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_SIMUL_BENEF_PORCETAGEM%TYPE
                                ,DTA_ELEGIB_BENEF_PORCETAGEM       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_ELEGIB_BENEF_PORCETAGEM%TYPE
                                ,IDADE_ELEGIB_PORCETAGEM           ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_ELEGIB_PORCETAGEM%TYPE
                                ,DTA_EXAURIM_BENEF_PORCETAGEM      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_EXAURIM_BENEF_PORCETAGEM%TYPE
                                ,VLR_SIMUL_BENEF_PRAZO             ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_SIMUL_BENEF_PRAZO%TYPE
                                ,DTA_ELEGIB_BENEF_PRAZO            ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_ELEGIB_BENEF_PRAZO%TYPE
                                ,IDADE_ELEGIB_BENEF_PRAZO          ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.IDADE_ELEGIB_BENEF_PRAZO%TYPE
                                ,DTA_EXAURIM_BENEF_PRAZO           ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_EXAURIM_BENEF_PRAZO%TYPE
                               );

            TB_REC_CARGA REC_CARGA;
      
      
                 
        
    BEGIN
        G_ARQ := UTL_FILE.FOPEN(G_DIR,G_NAME,G_READ,G_SIZE); 
        --DBMS_OUTPUT.PUT_LINE();             
        --FOR RG IN REG_DESTINO  
        
          LOOP       
            UTL_FILE.GET_LINE(G_ARQ, L_REGISTRO);
            --DBMS_OUTPUT.PUT_LINE(L_REGISTRO);
          
            L_INSTR_I := INSTR(L_REGISTRO,';');                                                       -- PEGA POSICAO DO 1� VALOR
            L_QTD_SEPARADOR := LENGTH(TRIM(L_REGISTRO)) - LENGTH(TRIM(REPLACE(L_REGISTRO, ';', ''))); -- QTDE DE SEPARADORES                             
            DBMS_OUTPUT.PUT_LINE('L_INSTR_I.....' || L_INSTR_I || CHR(13)||
                                 'L_QTD_SEPARADOR.....'|| L_QTD_SEPARADOR || CHR(13));
                                 
                --WHILE L_IDX < L_QTD_SEPARADOR
                  --LOOP 
                    --IF (L_INSTR_I = 1 ) THEN
                  
                    --L_CONTEUDO := SUBSTR(L_REGISTRO, 1, L_INSTR_I -1);
                    
                                        
                    --ELSE
                    
                    --L_CONTEUDO := SUBSTR(L_REGISTRO, L_INSTR_I + 1, INSTR(SUBSTR(L_REGISTRO, L_INSTR_I, LENGTH(L_REGISTRO) - 1), ';'));
                    
                    --END IF;
                    
                    
                   --L_IDX     := L_IDX + 1;
                   --L_INSTR_I := L_INSTR_I + 1;
                --END LOOP;
                
                  DBMS_OUTPUT.PUT_LINE(L_CONTEUDO);  
                  --INSERT INTO OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO 
                  --SET TB_REC_CARGA;
                 
                  
          END LOOP;
        
      EXCEPTION
        WHEN UTL_FILE.INVALID_PATH THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Diret�rio Inv�lido');
        WHEN UTL_FILE.INVALID_OPERATION THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Opera��o invalida no arquivo'); 
        WHEN UTL_FILE.WRITE_ERROR THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Erro de grava��o no arquivo'); 
        WHEN UTL_FILE.INVALID_MODE THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Modo de acesso inv�lido');
        WHEN OTHERS THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
            DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    
    END;
    
/*      -- CODIGO ERRO: -1704 - MSG: ORA-01704: string literal too long -- Mesmo erro do processo do Renato...    
        -- PROCEDURE CRIA TABELA EXTERNA:
    PROCEDURE PRC_CARGA_ARQUIVO (P_NAME VARCHAR2)
    IS        
      
    BEGIN
    
      --EXECUTE IMMEDIATE'
     G_DDL_CREATE_TABLE := '
    
        CREATE TABLE FC_PRE_TBL_CARGA_EXTRATO (  TPO_DADO                       VARCHAR2(2000),
                                                 COD_EMPRS                      VARCHAR2(2000),
                                                 NUM_RGTRO_EMPRG                VARCHAR2(2000),
                                                 NOM_EMPRG                      VARCHAR2(2000),
                                                 DTA_EMISS                      VARCHAR2(2000),
                                                 NUM_FOLHA                      VARCHAR2(2000),
                                                 DCR_PLANO                      VARCHAR2(2000),
                                                 PER_INIC_EXTR                  VARCHAR2(2000),
                                                 PER_FIM_EXTR                   VARCHAR2(2000),
                                                 DTA_INIC_EXTR                  VARCHAR2(2000),
                                                 DTA_FIM_EXTR                   VARCHAR2(2000),
                                                 DCR_SLD_MOV_SALDADO            VARCHAR2(2000),
                                                 SLD_PL_SALDADO_MOV_INIC        VARCHAR2(2000),
                                                 CTB_PL_SALDADO_MOV             VARCHAR2(2000),
                                                 RENT_PL_SALDADO_MOV            VARCHAR2(2000),
                                                 SLD_PL_SALDADO_MOV_FIM         VARCHAR2(2000),
                                                 DCR_SLD_MOV_BD                 VARCHAR2(2000),
                                                 SLD_PL_BD_INIC                 VARCHAR2(2000),
                                                 CTB_PL_MOV_BD                  VARCHAR2(2000),
                                                 RENT_PL_MOV_BD                 VARCHAR2(2000),
                                                 SLD_PL_BD_MOV_FIM              VARCHAR2(2000),
                                                 DCR_SLD_MOV_CV                 VARCHAR2(2000),
                                                 SLD_PL_CV_MOV_INIC             VARCHAR2(2000),
                                                 CTB_PL_MOV_CV                  VARCHAR2(2000),
                                                 RENT_PL_MOV_CV                 VARCHAR2(2000),
                                                 SLD_PL_CV_MOV_FIM              VARCHAR2(2000),
                                                 DCR_CTA_OBRIG_PARTIC           VARCHAR2(2000),
                                                 SLD_CTA_OBRIG_PARTIC           VARCHAR2(2000),
                                                 CTB_CTA_OBRIG_PARTIC           VARCHAR2(2000),
                                                 RENT_CTA_OBRIG_PARTIC          VARCHAR2(2000),
                                                 SLD_CTA_OBRIG_PARTIC_FIM       VARCHAR2(2000),
                                                 DCR_CTA_NORM_PATROC            VARCHAR2(2000),
                                                 SLD_CTA_NORM_PATROC            VARCHAR2(2000),
                                                 CTB_CTA_NORM_PATROC            VARCHAR2(2000),
                                                 RENT_NORM_PATROC               VARCHAR2(2000),
                                                 SLD_NORM_PATROC_INIC           VARCHAR2(2000),
                                                 DCR_CTA_ESPEC_PARTIC           VARCHAR2(2000),
                                                 SLD_CTA_ESPEC_PARTIC           VARCHAR2(2000),
                                                 CTB_CTA_ESPEC_PARTIC           VARCHAR2(2000),
                                                 RENT_CTA_ESPEC_PARTIC          VARCHAR2(2000),
                                                 SLD_CTA_ESPEC_PARTIC_INIC      VARCHAR2(2000),
                                                 DCR_CTA_ESPEC_PATROC           VARCHAR2(2000),
                                                 SLD_CTA_ESPEC_PATROC           VARCHAR2(2000),
                                                 CTB_CTA_ESPEC_PATROC           VARCHAR2(2000),
                                                 RENT_CTA_ESPEC_PATROC          VARCHAR2(2000),
                                                 SLD_CTA_ESPEC_PATROC_INIC      VARCHAR2(2000),
                                                 SLD_TOT_INIC                   VARCHAR2(2000),
                                                 CTB_TOT_INIC                   VARCHAR2(2000),
                                                 RENT_PERIODO                   VARCHAR2(2000),
                                                 SLD_TOT_FIM                    VARCHAR2(2000),
                                                 PRM_MES_PERIODO_CTB            VARCHAR2(2000),
                                                 SEG_MES_PERIODO_CTB            VARCHAR2(2000),
                                                 TER_MES_PERIODO_CTB            VARCHAR2(2000),
                                                 DCR_TOT_CTB_BD                 VARCHAR2(2000),
                                                 VLR_TOT_CTB_BD_PRM_MES         VARCHAR2(2000),
                                                 VLR_TOT_CTB_BD_SEG_MES         VARCHAR2(2000),
                                                 VLR_TOT_CTB_BD_TER_MES         VARCHAR2(2000),
                                                 VLR_TOT_CTB_BD_PERIODO         VARCHAR2(2000),
                                                 DCR_TOT_CTB_CV                 VARCHAR2(2000),
                                                 VLR_TOT_CTB_CV_PRM_MES         VARCHAR2(2000),
                                                 VLR_TOT_CTB_CV_SEG_MES         VARCHAR2(2000),
                                                 VLR_TOT_CTB_CV_TER_MES         VARCHAR2(2000),
                                                 VLR_TOT_CTB_CV_PERIODO         VARCHAR2(2000),
                                                 DCR_TPO_CTB_VOL_PARTIC         VARCHAR2(2000),
                                                 VLR_CTB_VOL_PARTIC_PRM_MES     VARCHAR2(2000),
                                                 VLR_CTB_VOL_PARTIC_SEG_MES     VARCHAR2(2000),
                                                 VLR_CTB_VOL_PARTIC_TER_MES     VARCHAR2(2000),
                                                 VLR_CTB_VOL_PARTIC_PERIODO     VARCHAR2(2000),
                                                 DCR_TPO_CTB_VOL_PATROC         VARCHAR2(2000),
                                                 VLR_CTB_VOL_PATROC_PRM_MES     VARCHAR2(2000),
                                                 VLR_CTB_VOL_PATROC_SEG_MES     VARCHAR2(2000),
                                                 VLR_CTB_VOL_PATROC_TER_MES     VARCHAR2(2000),
                                                 VLR_CTB_VOL_PATROC_PERIODO     VARCHAR2(2000),
                                                 DCR_TPO_CTB_OBRIG_PARTIC       VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PARTIC_PRM_MES   VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PARTIC_SEG_MES   VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PARTIC_TER_MES   VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PARTIC_PERIODO   VARCHAR2(2000),
                                                 DCR_TPO_CTB_OBRIG_PATROC       VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PATROC_PRM_MES   VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PATROC_SEG_MES   VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PATROC_TER_MES   VARCHAR2(2000),
                                                 VLR_CTB_OBRIG_PATROC_PERIODO   VARCHAR2(2000),
                                                 DCR_TPO_CTB_ESPOR_PATROC       VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PATROC_PRM_MES   VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PATROC_SEG_MES   VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PATROC_TER_MES   VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PATROC_PERIODO   VARCHAR2(2000),
                                                 DCR_TPO_CTB_ESPOR_PARTIC       VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PARTIC_PRM_MES   VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PARTIC_SEG_MES   VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PARTIC_TER_MES   VARCHAR2(2000),
                                                 VLR_CTB_ESPOR_PARTIC_PERIODO   VARCHAR2(2000),
                                                 TOT_CTB_PRM_MES                VARCHAR2(2000),
                                                 TOT_CTB_SEG_MES                VARCHAR2(2000),
                                                 TOT_CTB_TER_MES                VARCHAR2(2000),
                                                 TOT_CTB_EXTRATO                VARCHAR2(2000),
                                                 PRM_MES_PERIODO_RENT           VARCHAR2(2000),
                                                 SEG_MES_PERIODO_RENT           VARCHAR2(2000),
                                                 TER_MES_PERIODO_RENT           VARCHAR2(2000),
                                                 PCT_RENT_REAL_PRM_MES          VARCHAR2(2000),
                                                 PCT_RENT_REAL_SEG_MES          VARCHAR2(2000),
                                                 PCT_RENT_REAL_TER_MES          VARCHAR2(2000),
                                                 PCT_RENT_REAL_TOT_MES          VARCHAR2(2000),
                                                 PCT_RENT_LMTD_PRM_MES          VARCHAR2(2000),
                                                 PCT_RENT_LMTD_SEG_MES          VARCHAR2(2000),
                                                 PCT_RENT_LMTD_TER_MES          VARCHAR2(2000),
                                                 PCT_RENT_LMTD_TOT_MES          VARCHAR2(2000),
                                                 PCT_RENT_IGPDI_PRM_MES         VARCHAR2(2000),
                                                 PCT_RENT_IGPDI_SEG_MES         VARCHAR2(2000),
                                                 PCT_RENT_IGPDI_TER_MES         VARCHAR2(2000),
                                                 PCT_RENT_IGPDI_TOT_MES         VARCHAR2(2000),
                                                 PCT_RENT_URR_PRM_MES           VARCHAR2(2000),
                                                 PCT_RENT_URR_SEG_MES           VARCHAR2(2000),
                                                 PCT_RENT_URR_TER_MES           VARCHAR2(2000),
                                                 PCT_RENT_URR_TOT_MES           VARCHAR2(2000),
                                                 DTA_APOS_PROP                  VARCHAR2(2000),
                                                 DTA_APOS_INTE                  VARCHAR2(2000),
                                                 VLR_BENEF_PSAP_PROP            VARCHAR2(2000),
                                                 VLR_BENEF_PSAP_INTE            VARCHAR2(2000),
                                                 VLR_BENEF_BD_PROP              VARCHAR2(2000),
                                                 VLR_BENEF_BD_INTE              VARCHAR2(2000),
                                                 VLR_BENEF_CV_PROP              VARCHAR2(2000),
                                                 VLR_BENEF_CV_INTE              VARCHAR2(2000),
                                                 RENDA_ESTIM_PROP               VARCHAR2(2000),
                                                 RENDA_ESTIM_INT                VARCHAR2(2000),
                                                 VLR_RESERV_SALD_LQDA           VARCHAR2(2000),
                                                 TXT_PRM_MENS                   VARCHAR2(2000),
                                                 TXT_SEG_MENS                   VARCHAR2(2000),
                                                 TXT_TER_MENS                   VARCHAR2(2000),
                                                 TXT_QUA_MENS                   VARCHAR2(2000),
                                                 IDADE_PROP_BSPS                VARCHAR2(2000),
                                                 VLR_CTB_PROP_BSPS              VARCHAR2(2000),
                                                 IDADE_INT_BSPS                 VARCHAR2(2000),
                                                 VLR_CTB_INT_BSPS               VARCHAR2(2000),
                                                 IDADE_PROP_BD                  VARCHAR2(2000),
                                                 VLR_CTB_PROP_BD                VARCHAR2(2000),
                                                 IDADE_INT_BD                   VARCHAR2(2000),
                                                 VLR_CTB_INT_BD                 VARCHAR2(2000),
                                                 IDADE_PROP_CV                  VARCHAR2(2000),
                                                 VLR_CTB_PROP_CV                VARCHAR2(2000),
                                                 IDADE_INT_CV                   VARCHAR2(2000),
                                                 VLR_CTB_INT_CV                 VARCHAR2(2000),
                                                 DCR_COTA_INDEX_PLAN_1          VARCHAR2(2000),
                                                 DCR_COTA_INDEX_PLAN_2          VARCHAR2(2000),
                                                 DCR_CTA_APOS_INDIV_VOL_PARTIC  VARCHAR2(2000),
                                                 SLD_INI_CTA_APO_INDI_VOL_PARTI VARCHAR2(2000),
                                                 VLR_TOT_CTB_APO_INDI_VOL_PARTI VARCHAR2(2000),
                                                 REN_TOT_CTB_APO_INDI_VOL_PARTI VARCHAR2(2000),
                                                 SLD_FIM_CTA_APO_INDI_VOL_PARTI VARCHAR2(2000),
                                                 DCR_CTA_APOS_INDIV_ESPO_PARTIC VARCHAR2(2000),
                                                 SLD_INI_CTA_APO_INDI_ESPOPARTI VARCHAR2(2000),
                                                 VLR_TOT_CTB_APO_INDI_ESPOPARTI VARCHAR2(2000),
                                                 REN_TOT_CTB_APO_INDI_ESPOPARTI VARCHAR2(2000),
                                                 SLD_FIM_CTA_APO_INDI_ESPOPARTI VARCHAR2(2000),
                                                 DCR_CTA_APOS_INDIV_VOL_PATROC  VARCHAR2(2000),
                                                 SLD_INI_CTA_APO_INDI_VOL_PATRO VARCHAR2(2000),
                                                 VLR_TOT_CTB_APO_INDI_VOL_PATRO VARCHAR2(2000),
                                                 REN_TOT_CTB_APO_INDI_VOL_PATRO VARCHAR2(2000),
                                                 SLD_FIM_CTA_APO_INDI_VOL_PATRO VARCHAR2(2000),
                                                 DCR_CTA_APOS_INDIV_SUPL_PATROC VARCHAR2(2000),
                                                 SLD_INI_CTA_APO_INDI_SUPLPATRO VARCHAR2(2000),
                                                 VLR_TOT_CTB_APO_INDI_SUPLPATRO VARCHAR2(2000),
                                                 REN_TOT_CTB_APO_INDI_SUPLPATRO VARCHAR2(2000),
                                                 SLD_FIM_CTA_APO_INDI_SUPLPATRO VARCHAR2(2000),
                                                 DCR_PORT_TOTAL                 VARCHAR2(2000),
                                                 SLD_INIC_CTA_PORT_TOT          VARCHAR2(2000),
                                                 VLR_TOT_CTB_PORT_TOT           VARCHAR2(2000),
                                                 RENT_TOT_CTB_PORT_TOT          VARCHAR2(2000),
                                                 SLD_FIM_CTA_PORT_TOT           VARCHAR2(2000),
                                                 DCR_PORT_ABERTA                VARCHAR2(2000),
                                                 SLD_INIC_CTA_PORT_ABERTA       VARCHAR2(2000),
                                                 VLR_TOT_CTB_PORT_ABERTA        VARCHAR2(2000),
                                                 RENT_TOT_CTB_PORT_ABERTA       VARCHAR2(2000),
                                                 SLD_FIM_CTA_PORT_ABERTA        VARCHAR2(2000),
                                                 DCR_PORT_FECHADA               VARCHAR2(2000),
                                                 SLD_INIC_CTA_PORT_FECHADA      VARCHAR2(2000),
                                                 VLR_TOT_CTB_PORT_FECHADA       VARCHAR2(2000),
                                                 RENT_TOT_CTB_PORT_FECHADA      VARCHAR2(2000),
                                                 SLD_FIM_CTA_PORT_FECHADA       VARCHAR2(2000),
                                                 DCR_PORT_JOIA_ABERTA           VARCHAR2(2000),
                                                 SLD_INIC_CTA_PORT_JOIA_ABERTA  VARCHAR2(2000),
                                                 VLR_TOT_CTB_PORT_JOIA_ABERTA   VARCHAR2(2000),
                                                 RENT_TOT_CTB_PORT_JOIA_ABERTA  VARCHAR2(2000),
                                                 SLD_FIM_CTA_PORT_JOIA_ABERTA   VARCHAR2(2000),
                                                 DCR_PORT_JOIA_FECHADA          VARCHAR2(2000),
                                                 SLD_INIC_CTA_PORT_JOIA_FECHADA VARCHAR2(2000),
                                                 VLR_TOT_CTB_PORT_JOIA_FECHADA  VARCHAR2(2000),
                                                 RENT_TOT_CTB_PORT_JOIA_FECHADA VARCHAR2(2000),
                                                 SLD_FIM_CTA_PORT_JOIA_FECHADA  VARCHAR2(2000),
                                                 DCR_DISTR_FUND_PREV_PARTIC     VARCHAR2(2000),
                                                 SLD_INI_DIST_FUND_PREV_PARTI   VARCHAR2(2000),
                                                 VLR_TOT_DIST_FUND_PREV_PARTI   VARCHAR2(2000),
                                                 REN_TOT_DIST_FUND_PREV_PARTI   VARCHAR2(2000),
                                                 SLDFIM_CTA_DISTFUNDPREVPARTI   VARCHAR2(2000),
                                                 DCR_DISTR_FUND_PREV_PATROC     VARCHAR2(2000),
                                                 SLD_INI_DIST_FUND_PREV_PATRO   VARCHAR2(2000),
                                                 VLR_TOT_DIST_FUND_PREV_PATRO   VARCHAR2(2000),
                                                 REN_TOT_DIST_FUND_PREV_PATRO   VARCHAR2(2000),
                                                 SLDFIM_CTA_DISTFUNDPREVPATRO   VARCHAR2(2000),
                                                 DCR_PORT_FINAL                 VARCHAR2(2000),
                                                 SLD_INIC_CTA_PORT_FIM          VARCHAR2(2000),
                                                 VLR_TOT_CTB_PORT_FIM           VARCHAR2(2000),
                                                 RENT_TOT_CTB_PORT_FIM          VARCHAR2(2000),
                                                 SLD_FIM_CTA_PORT_FIM           VARCHAR2(2000),
                                                 DCR_SLD_PROJETADO              VARCHAR2(2000),
                                                 VLR_SLD_PROJETADO              VARCHAR2(2000),
                                                 VLR_SLD_ADICIONAL              VARCHAR2(2000),
                                                 VLR_BENEF_ADICIONAL            VARCHAR2(2000),
                                                 DTA_ULT_ATUAL                  VARCHAR2(2000),
                                                 VLR_CONTRIB_RISCO              VARCHAR2(2000),
                                                 VLR_CONTRIB_PATRC              VARCHAR2(2000),
                                                 VLR_CAPIT_SEGURADO             VARCHAR2(2000),
                                                 VLR_CONTRIB_ADM                VARCHAR2(2000),
                                                 VLR_CONTRIB_ADM_PATRC          VARCHAR2(2000),
                                                 VLR_SIMUL_BENEF_PORCETAGEM     VARCHAR2(2000),
                                                 DTA_ELEGIB_BENEF_PORCETAGEM    VARCHAR2(2000),
                                                 IDADE_ELEGIB_PORCETAGEM        VARCHAR2(2000),
                                                 DTA_EXAURIM_BENEF_PORCETAGEM   VARCHAR2(2000),
                                                 VLR_SIMUL_BENEF_PRAZO          VARCHAR2(2000),
                                                 DTA_ELEGIB_BENEF_PRAZO         VARCHAR2(2000),
                                                 IDADE_ELEGIB_BENEF_PRAZO       VARCHAR2(2000),
                                                 DTA_EXAURIM_BENEF_PRAZO        VARCHAR2(2000)
                                                )
        ORGANIZATION EXTERNAL (
          TYPE ORACLE_LOADER
          DEFAULT DIRECTORY DIR_WORK
          ACCESS PARAMETERS (
            RECORDS DELIMITED BY NEWLINE
            FIELDS TERMINATED BY G_SEPARADOR
            MISSING FIELD VALUES ARE NULL
          )    
          LOCATION ('||P_NAME||')
        )
        PARALLEL 5
        REJECT LIMIT UNLIMITED;''';
        
        
        EXECUTE IMMEDIATE '''' || G_DDL_CREATE_TABLE || '''''';
        --DBMS_OUTPUT.PUT_LINE(G_DDL_CREATE_TABLE);
        DBMS_OUTPUT.PUT_LINE('TABELA EXTERNA CRIADA COM SUCESSO! --> FC_PRE_TBL_CARGA_EXTRATO'); 
        
        EXCEPTION
          WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
           DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
            
        --
        --
     
    END PRC_CARGA_ARQUIVO;
*/
   
/*
    -- PROCEDURE PRC_TRATA_ARQUIVO
    FUNCTION FN_TRATA_ARQUIVO
      RETURN BOOLEAN
                
    IS
      V_COUNT NUMBER     :=0;
      V_CONT_TEMP NUMBER :=0;
      R_VALIDA BOOLEAN;
    
    
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
              ,TO_NUMBER(REPLACE(REPLACE(REPLACE(VLR_SLD_PROJETADO, '.',''), CHR(13), ''), ',','.'))   AS VLR_SLD_PROJETADO
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
        FROM FC_PRE_TBL_CARGA_EXTRATO   
        WHERE TPO_DADO = '1';
      
    BEGIN
      --
        BEGIN
             SELECT COUNT(*) INTO V_CONT_TEMP FROM FC_PRE_TBL_CARGA_EXTRATO  WHERE TPO_DADO = 1;
              
           EXCEPTION
             WHEN OTHERS THEN 
                DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO: ' || SQLCODE || ' MSG: ' ||SQLERRM);
                DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
             
        END;
        DBMS_OUTPUT.PUT_LINE('PRC_TRATA_ARQUIVO');
      
      --
      --
      FOR RG_TRATA_DADOS IN C_TRATA_DADOS 
         LOOP
           -- INSERT 
           --DBMS_OUTPUT.PUT_LINE(RG_TRATA_DADOS.COD_EMPRS||' - '||RG_TRATA_DADOS.NUM_RGTRO_EMPRG);
            INSERT INTO ATT.FC_PRE_TBL_BASE_EXTRAT_CTB VALUES( RG_TRATA_DADOS.TPO_DADO  
                                                              ,RG_TRATA_DADOS.COD_EMPRS  
                                                              ,RG_TRATA_DADOS.NUM_RGTRO_EMPRG  
                                                              ,RG_TRATA_DADOS.NOM_EMPRG  
                                                              ,RG_TRATA_DADOS.DTA_EMISS  
                                                              ,RG_TRATA_DADOS.NUM_FOLHA  
                                                              ,RG_TRATA_DADOS.DCR_PLANO  
                                                              ,RG_TRATA_DADOS.PER_INIC_EXTR  
                                                              ,RG_TRATA_DADOS.PER_FIM_EXTR  
                                                              ,RG_TRATA_DADOS.DTA_INIC_EXTR  
                                                              ,RG_TRATA_DADOS.DTA_FIM_EXTR  
                                                              ,RG_TRATA_DADOS.DCR_SLD_MOV_SALDADO  
                                                              ,RG_TRATA_DADOS.SLD_PL_SALDADO_MOV_INIC  
                                                              ,RG_TRATA_DADOS.CTB_PL_SALDADO_MOV  
                                                              ,RG_TRATA_DADOS.RENT_PL_SALDADO_MOV  
                                                              ,RG_TRATA_DADOS.SLD_PL_SALDADO_MOV_FIM  
                                                              ,RG_TRATA_DADOS.DCR_SLD_MOV_BD  
                                                              ,RG_TRATA_DADOS.SLD_PL_BD_INIC  
                                                              ,RG_TRATA_DADOS.CTB_PL_MOV_BD  
                                                              ,RG_TRATA_DADOS.RENT_PL_MOV_BD  
                                                              ,RG_TRATA_DADOS.SLD_PL_BD_MOV_FIM  
                                                              ,RG_TRATA_DADOS.DCR_SLD_MOV_CV  
                                                              ,RG_TRATA_DADOS.SLD_PL_CV_MOV_INIC  
                                                              ,RG_TRATA_DADOS.CTB_PL_MOV_CV  
                                                              ,RG_TRATA_DADOS.RENT_PL_MOV_CV  
                                                              ,RG_TRATA_DADOS.SLD_PL_CV_MOV_FIM  
                                                              ,RG_TRATA_DADOS.DCR_CTA_OBRIG_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_OBRIG_PARTIC  
                                                              ,RG_TRATA_DADOS.CTB_CTA_OBRIG_PARTIC  
                                                              ,RG_TRATA_DADOS.RENT_CTA_OBRIG_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_OBRIG_PARTIC_FIM  
                                                              ,RG_TRATA_DADOS.DCR_CTA_NORM_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_NORM_PATROC  
                                                              ,RG_TRATA_DADOS.CTB_CTA_NORM_PATROC  
                                                              ,RG_TRATA_DADOS.RENT_NORM_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_NORM_PATROC_INIC  
                                                              ,RG_TRATA_DADOS.DCR_CTA_ESPEC_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_ESPEC_PARTIC  
                                                              ,RG_TRATA_DADOS.CTB_CTA_ESPEC_PARTIC  
                                                              ,RG_TRATA_DADOS.RENT_CTA_ESPEC_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_ESPEC_PARTIC_INIC  
                                                              ,RG_TRATA_DADOS.DCR_CTA_ESPEC_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_ESPEC_PATROC  
                                                              ,RG_TRATA_DADOS.CTB_CTA_ESPEC_PATROC  
                                                              ,RG_TRATA_DADOS.RENT_CTA_ESPEC_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_CTA_ESPEC_PATROC_INIC  
                                                              ,RG_TRATA_DADOS.SLD_TOT_INIC  
                                                              ,RG_TRATA_DADOS.CTB_TOT_INIC  
                                                              ,RG_TRATA_DADOS.RENT_PERIODO  
                                                              ,RG_TRATA_DADOS.SLD_TOT_FIM  
                                                              ,RG_TRATA_DADOS.PRM_MES_PERIODO_CTB  
                                                              ,RG_TRATA_DADOS.SEG_MES_PERIODO_CTB  
                                                              ,RG_TRATA_DADOS.TER_MES_PERIODO_CTB  
                                                              ,RG_TRATA_DADOS.DCR_TOT_CTB_BD  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_BD_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_BD_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_BD_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_BD_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TOT_CTB_CV  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_CV_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_CV_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_CV_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_CV_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TPO_CTB_VOL_PARTIC  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PARTIC_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PARTIC_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PARTIC_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PARTIC_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TPO_CTB_VOL_PATROC  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PATROC_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PATROC_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PATROC_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_VOL_PATROC_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TPO_CTB_OBRIG_PARTIC  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PARTIC_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PARTIC_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PARTIC_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PARTIC_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TPO_CTB_OBRIG_PATROC  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PATROC_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PATROC_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PATROC_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_OBRIG_PATROC_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TPO_CTB_ESPOR_PATROC  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PATROC_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PATROC_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PATROC_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PATROC_PERIODO  
                                                              ,RG_TRATA_DADOS.DCR_TPO_CTB_ESPOR_PARTIC  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PARTIC_PRM_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PARTIC_SEG_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PARTIC_TER_MES  
                                                              ,RG_TRATA_DADOS.VLR_CTB_ESPOR_PARTIC_PERIODO  
                                                              ,RG_TRATA_DADOS.TOT_CTB_PRM_MES  
                                                              ,RG_TRATA_DADOS.TOT_CTB_SEG_MES  
                                                              ,RG_TRATA_DADOS.TOT_CTB_TER_MES  
                                                              ,RG_TRATA_DADOS.TOT_CTB_EXTRATO  
                                                              ,RG_TRATA_DADOS.PRM_MES_PERIODO_RENT  
                                                              ,RG_TRATA_DADOS.SEG_MES_PERIODO_RENT  
                                                              ,RG_TRATA_DADOS.TER_MES_PERIODO_RENT  
                                                              ,RG_TRATA_DADOS.PCT_RENT_REAL_PRM_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_REAL_SEG_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_REAL_TER_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_REAL_TOT_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_LMTD_PRM_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_LMTD_SEG_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_LMTD_TER_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_LMTD_TOT_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_IGPDI_PRM_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_IGPDI_SEG_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_IGPDI_TER_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_IGPDI_TOT_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_URR_PRM_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_URR_SEG_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_URR_TER_MES  
                                                              ,RG_TRATA_DADOS.PCT_RENT_URR_TOT_MES  
                                                              ,RG_TRATA_DADOS.DTA_APOS_PROP  
                                                              ,RG_TRATA_DADOS.DTA_APOS_INTE  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_PSAP_PROP  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_PSAP_INTE  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_BD_PROP  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_BD_INTE  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_CV_PROP  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_CV_INTE  
                                                              ,RG_TRATA_DADOS.RENDA_ESTIM_PROP  
                                                              ,RG_TRATA_DADOS.RENDA_ESTIM_INT  
                                                              ,RG_TRATA_DADOS.VLR_RESERV_SALD_LQDA  
                                                              ,RG_TRATA_DADOS.TXT_PRM_MENS  
                                                              ,RG_TRATA_DADOS.TXT_SEG_MENS  
                                                              ,RG_TRATA_DADOS.TXT_TER_MENS  
                                                              ,RG_TRATA_DADOS.TXT_QUA_MENS  
                                                              ,RG_TRATA_DADOS.IDADE_PROP_BSPS  
                                                              ,RG_TRATA_DADOS.VLR_CTB_PROP_BSPS  
                                                              ,RG_TRATA_DADOS.IDADE_INT_BSPS  
                                                              ,RG_TRATA_DADOS.VLR_CTB_INT_BSPS  
                                                              ,RG_TRATA_DADOS.IDADE_PROP_BD  
                                                              ,RG_TRATA_DADOS.VLR_CTB_PROP_BD  
                                                              ,RG_TRATA_DADOS.IDADE_INT_BD  
                                                              ,RG_TRATA_DADOS.VLR_CTB_INT_BD  
                                                              ,RG_TRATA_DADOS.IDADE_PROP_CV  
                                                              ,RG_TRATA_DADOS.VLR_CTB_PROP_CV  
                                                              ,RG_TRATA_DADOS.IDADE_INT_CV  
                                                              ,RG_TRATA_DADOS.VLR_CTB_INT_CV  
                                                              ,RG_TRATA_DADOS.DCR_COTA_INDEX_PLAN_1  
                                                              ,RG_TRATA_DADOS.DCR_COTA_INDEX_PLAN_2  
                                                              ,RG_TRATA_DADOS.DCR_CTA_APOS_INDIV_VOL_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_INI_CTA_APO_INDI_VOL_PARTI  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_APO_INDI_VOL_PARTI  
                                                              ,RG_TRATA_DADOS.REN_TOT_CTB_APO_INDI_VOL_PARTI  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_APO_INDI_VOL_PARTI  
                                                              ,RG_TRATA_DADOS.DCR_CTA_APOS_INDIV_ESPO_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_INI_CTA_APO_INDI_ESPOPARTI  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_APO_INDI_ESPOPARTI  
                                                              ,RG_TRATA_DADOS.REN_TOT_CTB_APO_INDI_ESPOPARTI  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_APO_INDI_ESPOPARTI  
                                                              ,RG_TRATA_DADOS.DCR_CTA_APOS_INDIV_VOL_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_INI_CTA_APO_INDI_VOL_PATRO  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_APO_INDI_VOL_PATRO  
                                                              ,RG_TRATA_DADOS.REN_TOT_CTB_APO_INDI_VOL_PATRO  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_APO_INDI_VOL_PATRO  
                                                              ,RG_TRATA_DADOS.DCR_CTA_APOS_INDIV_SUPL_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_INI_CTA_APO_INDI_SUPLPATRO  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_APO_INDI_SUPLPATRO  
                                                              ,RG_TRATA_DADOS.REN_TOT_CTB_APO_INDI_SUPLPATRO  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_APO_INDI_SUPLPATRO  
                                                              ,RG_TRATA_DADOS.DCR_PORT_TOTAL  
                                                              ,RG_TRATA_DADOS.SLD_INIC_CTA_PORT_TOT  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_PORT_TOT  
                                                              ,RG_TRATA_DADOS.RENT_TOT_CTB_PORT_TOT  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_PORT_TOT  
                                                              ,RG_TRATA_DADOS.DCR_PORT_ABERTA  
                                                              ,RG_TRATA_DADOS.SLD_INIC_CTA_PORT_ABERTA  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_PORT_ABERTA  
                                                              ,RG_TRATA_DADOS.RENT_TOT_CTB_PORT_ABERTA  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_PORT_ABERTA  
                                                              ,RG_TRATA_DADOS.DCR_PORT_FECHADA  
                                                              ,RG_TRATA_DADOS.SLD_INIC_CTA_PORT_FECHADA  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_PORT_FECHADA  
                                                              ,RG_TRATA_DADOS.RENT_TOT_CTB_PORT_FECHADA  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_PORT_FECHADA  
                                                              ,RG_TRATA_DADOS.DCR_PORT_JOIA_ABERTA  
                                                              ,RG_TRATA_DADOS.SLD_INIC_CTA_PORT_JOIA_ABERTA  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_PORT_JOIA_ABERTA  
                                                              ,RG_TRATA_DADOS.RENT_TOT_CTB_PORT_JOIA_ABERTA  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_PORT_JOIA_ABERTA  
                                                              ,RG_TRATA_DADOS.DCR_PORT_JOIA_FECHADA  
                                                              ,RG_TRATA_DADOS.SLD_INIC_CTA_PORT_JOIA_FECHADA  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_PORT_JOIA_FECHADA  
                                                              ,RG_TRATA_DADOS.RENT_TOT_CTB_PORT_JOIA_FECHADA  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_PORT_JOIA_FECHADA  
                                                              ,RG_TRATA_DADOS.DCR_DISTR_FUND_PREV_PARTIC  
                                                              ,RG_TRATA_DADOS.SLD_INI_DIST_FUND_PREV_PARTI  
                                                              ,RG_TRATA_DADOS.VLR_TOT_DIST_FUND_PREV_PARTI  
                                                              ,RG_TRATA_DADOS.REN_TOT_DIST_FUND_PREV_PARTI  
                                                              ,RG_TRATA_DADOS.SLDFIM_CTA_DISTFUNDPREVPARTI  
                                                              ,RG_TRATA_DADOS.DCR_DISTR_FUND_PREV_PATROC  
                                                              ,RG_TRATA_DADOS.SLD_INI_DIST_FUND_PREV_PATRO  
                                                              ,RG_TRATA_DADOS.VLR_TOT_DIST_FUND_PREV_PATRO  
                                                              ,RG_TRATA_DADOS.REN_TOT_DIST_FUND_PREV_PATRO  
                                                              ,RG_TRATA_DADOS.SLDFIM_CTA_DISTFUNDPREVPATRO  
                                                              ,RG_TRATA_DADOS.DCR_PORT_FINAL  
                                                              ,RG_TRATA_DADOS.SLD_INIC_CTA_PORT_FIM  
                                                              ,RG_TRATA_DADOS.VLR_TOT_CTB_PORT_FIM  
                                                              ,RG_TRATA_DADOS.RENT_TOT_CTB_PORT_FIM  
                                                              ,RG_TRATA_DADOS.SLD_FIM_CTA_PORT_FIM  
                                                              ,RG_TRATA_DADOS.DCR_SLD_PROJETADO  
                                                              ,RG_TRATA_DADOS.VLR_SLD_PROJETADO  
                                                              ,RG_TRATA_DADOS.VLR_SLD_ADICIONAL  
                                                              ,RG_TRATA_DADOS.VLR_BENEF_ADICIONAL  
                                                              ,RG_TRATA_DADOS.DTA_ULT_ATUAL  
                                                              ,RG_TRATA_DADOS.VLR_CONTRIB_RISCO  
                                                              ,RG_TRATA_DADOS.VLR_CONTRIB_PATRC  
                                                              ,RG_TRATA_DADOS.VLR_CAPIT_SEGURADO  
                                                              ,RG_TRATA_DADOS.VLR_CONTRIB_ADM  
                                                              ,RG_TRATA_DADOS.VLR_CONTRIB_ADM_PATRC  
                                                              ,RG_TRATA_DADOS.VLR_SIMUL_BENEF_PORCETAGEM  
                                                              ,RG_TRATA_DADOS.DTA_ELEGIB_BENEF_PORCETAGEM  
                                                              ,RG_TRATA_DADOS.IDADE_ELEGIB_PORCETAGEM  
                                                              ,RG_TRATA_DADOS.DTA_EXAURIM_BENEF_PORCETAGEM  
                                                              ,RG_TRATA_DADOS.VLR_SIMUL_BENEF_PRAZO  
                                                              ,RG_TRATA_DADOS.DTA_ELEGIB_BENEF_PRAZO  
                                                              ,RG_TRATA_DADOS.IDADE_ELEGIB_BENEF_PRAZO  
                                                              ,RG_TRATA_DADOS.DTA_EXAURIM_BENEF_PRAZO
                                                            );
         IF SQL%ROWCOUNT > 0 THEN                                                                                                                                
              V_COUNT := V_COUNT + 1;                                                                                                                                                         
         END IF;
                   
      END LOOP;
      
      IF V_COUNT = V_CONT_TEMP THEN 
           R_VALIDA:= TRUE;    
        RETURN R_VALIDA;        
         COMMIT;
         -- CRIAR UM DDL PARA DROPAR A TABELA EXTERNA E OS COMANDOS DE GRANT EM TEMPO DE EXECUCAO...
          
            --LIMPAR A TABELA, NAO DROPAR ' FC_PRE_TBL_CARGA_EXTRATO'; 
          
      ELSE
         RETURN R_VALIDA;       
       ROLLBACK;
       
      END IF; 
      
        DBMS_OUTPUT.PUT_LINE('TOTAL DE REGISTROS INSERIDO: '||V_COUNT);             
      --
      EXCEPTION    
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO: ' || SQLCODE || ' MSG: ' ||SQLERRM);
            DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
         RETURN FALSE;
         NULL;
    END FN_TRATA_ARQUIVO;
*/   
       -- -------------------------------------------------------------------------------------------
       -- FUN_CALC_VLR
       -- Descricao: 
       -- -------------------------------------------------------------------------------------------    
       FUNCTION FUN_CALC_VLR(  P_NUM_MATR  ATT.HIST_VALOR_BNF.NUM_MATR_PARTF%TYPE
                              ,P_DTA_FIM   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE
                              ,P_NUM_PLBNF ATT.HIST_VALOR_BNF.NUM_PLBNF%TYPE
                              ,P_DESC_PLBNF ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                              ,P_CALC      NUMBER
                               --
                              ,P_COD_EMPRS  ATT.EMPRESA.COD_EMPRS%TYPE
                              --,P_DT_MOV     DATE
                              ,P_COD_NATBNF ATT.HIST_VALOR_BNF.COD_NATBNF%TYPE
                              ,P_NUM_CTFSS  ATT.SLD_CONTA_PARTIC_FSS.NUM_CTFSS%TYPE DEFAULT NULL
                              ,P_COD_UM     ATT.SLD_CONTA_PARTIC_FSS.COD_UM%TYPE DEFAULT NULL
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
          

        BEGIN
          --
          --
          -- CALCULO PARA AS EMPRESAS: ELETROPAULO/TIM
          IF (P_COD_EMPRS IN (40, 60) ) THEN
                      
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
                  --
                                                   
                  SELECT MAX(VLR_CDIAUM)AS VLR_CDIAUM
                    INTO R_RES2
                    FROM COTACAO_DIA_UM
                    WHERE COD_UM = 248
                    AND DAT_CDIAUM = (SELECT MAX(DAT_CDIAUM)
                                        FROM COTACAO_DIA_UM
                                       WHERE DAT_CDIAUM BETWEEN TO_DATE('01/'||TO_CHAR(P_DTA_FIM,'MM/RRRR'),'DD/MM/RRRR') 
                                                         AND TO_DATE(LAST_DAY(P_DTA_FIM),'DD/MM/RRRR')
                                                         AND COD_UM = 248);
                                      
                      IF (     R_RES1 IS NOT NULL
                         AND R_RES2 IS NOT NULL ) THEN
                      R_RES3 := ROUND(R_RES1 * R_RES2);
                      RETURN NVL(R_RES3,0);
                    END IF;                                                       
                  
                END IF;
             
            -----------------------------------
            -- CALCULO PARA A EMPRESA: TIETE --
            -----------------------------------
            
           ELSIF (P_COD_EMPRS IN (44) ) THEN
               --DBMS_OUTPUT.PUT_LINE('PATROCINADO TIETE');
           
                
              IF (P_CALC = 1) THEN -- Valor do BDS - Modulo Saldado:              
              -- CALCULA VLR_BENEF_BD_INTE
              --
              SELECT NVL(MAX(VLR_BENEF1_HTBNF),0)
                   INTO R_VLR_BENEF_BD_INTE
                FROM HIST_VALOR_BNF
                WHERE NUM_MATR_PARTF = P_NUM_MATR
                AND COD_NATBNF       = P_COD_NATBNF -- 4
                AND NUM_PLBNF        = P_NUM_PLBNF
                AND TO_CHAR(DAT_INIVG_HTBNF, 'YYYYMM') = TO_CHAR(P_DTA_FIM,'YYYYMM'); 
                
                           
                IF (R_VLR_BENEF_BD_INTE IS NOT NULL) THEN
                  RETURN R_VLR_BENEF_BD_INTE;
                END IF;

              ELSIF (P_CALC = 2) THEN -- Valor Total dos Beneficios:
              --RENDA_ESTIM_PROP
              --
                SELECT NVL(SUM(FPT.VLR_BENEF_PSAP_PROP + FPT.VLR_BENEF_BD_PROP + FPT.VLR_BENEF_CV_PROP),0) AS RENDA_ESTIM_PROP
                   INTO R_RENDA_ESTIM_PROP
                  FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB FPT
                   INNER JOIN ATT.PARTICIPANTE_FSS Y ON Y.COD_EMPRS = FPT.COD_EMPRS
                                          AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(FPT.NUM_RGTRO_EMPRG,1,LENGTH(FPT.NUM_RGTRO_EMPRG) - 2))
                       WHERE FPT.COD_EMPRS        = P_COD_EMPRS 
                         AND UPPER(FPT.DCR_PLANO) = P_DESC_PLBNF   
                         AND Y.NUM_MATR_PARTF     = P_NUM_MATR    
                         AND FPT.DTA_FIM_EXTR     = P_DTA_FIM;     

                IF (R_RENDA_ESTIM_PROP IS NOT NULL) THEN
                  RETURN R_RENDA_ESTIM_PROP;
                END IF;

              ELSIF (P_CALC = 3)THEN -- Valor Total dos Beneficios:
              --RENDA_ESTIM_INT
              --
                SELECT NVL(SUM(FPT.VLR_BENEF_PSAP_INTE + FPT.VLR_BENEF_BD_INTE + FPT.VLR_BENEF_CV_INTE),0) AS RENDA_ESTIM_PROP
                  INTO R_RENDA_ESTIM_INT
                 FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB FPT
                   INNER JOIN ATT.PARTICIPANTE_FSS Y  ON Y.COD_EMPRS = FPT.COD_EMPRS
                                          AND Y.NUM_RGTRO_EMPRG = TO_NUMBER(SUBSTR(FPT.NUM_RGTRO_EMPRG,1,LENGTH(FPT.NUM_RGTRO_EMPRG) - 2))
                         WHERE FPT.COD_EMPRS        = P_COD_EMPRS 
                           AND UPPER(FPT.DCR_PLANO) = P_DESC_PLBNF   
                           AND Y.NUM_MATR_PARTF     = P_NUM_MATR    
                           AND FPT.DTA_FIM_EXTR     = P_DTA_FIM;     

                IF (R_RENDA_ESTIM_INT IS NOT NULL) THEN
                  RETURN R_RENDA_ESTIM_INT;
                END IF;
                
                ELSE
                 DBMS_OUTPUT.PUT_LINE('------------------');               
                END IF;           
           
           -- AQUI SERA IMPLEMENTADO A REGRA PARA A NOVA PATROCINADORA...
          END IF;
          
          EXCEPTION
            WHEN OTHERS THEN
              RETURN NULL;
              DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO: ' || SQLCODE || ' MSG: ' ||SQLERRM);
              DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
          END FUN_CALC_VLR;
        
        
        
   -- PATROCINADORA: PSAP/TIETE
   PROCEDURE PROC_EXT_PREV_TIETE(P_COD_EMPRESA   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE,
                                 P_DCR_PLANO     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE,
                                 P_DTA_MOV       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE)IS

    BEGIN
        DECLARE

           L_DTA_FIM     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE;
           L_COD_NATBNF ATT.HIST_VALOR_BNF.COD_NATBNF%TYPE:=4;
    

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
               WHERE X.COD_EMPRS        = P_COD_EMPRESA
                 AND UPPER(X.DCR_PLANO) = UPPER(P_DCR_PLANO)
                 --AND Y.NUM_MATR_PARTF = 91687
                 AND X.DTA_FIM_EXTR     = P_DTA_MOV;


      BEGIN

            IF (P_DTA_MOV IS NULL)THEN 
            -- FC_PRE_TBL_BASE_EXTRAT_CTB
            -- PEGA MAIOR DATA
            SELECT MAX(DTA_FIM_EXTR)AS DTA_FIM_EXTR
               INTO L_DTA_FIM
               FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB
             WHERE COD_EMPRS        = P_COD_EMPRESA 
              AND UPPER(DCR_PLANO)  = P_DCR_PLANO;   
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
            
            TB_REC_BASE.VLR_BENEF_BD_PROP      := 0; --VLR_BENEF_BD_PROP
            TB_REC_BASE.VLR_BENEF_BD_INTE      := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, P_DCR_PLANO, 1, P_COD_EMPRESA, L_COD_NATBNF);  --VLR_BENEF_BD_INTE
            TB_REC_BASE.RENDA_ESTIM_PROP       := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, P_DCR_PLANO, 2, P_COD_EMPRESA, L_COD_NATBNF ); --RENDA_ESTIM_PROP
            TB_REC_BASE.RENDA_ESTIM_INT        := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, P_DCR_PLANO, 3, P_COD_EMPRESA, L_COD_NATBNF);  --RENDA_ESTIM_INT
            TB_REC_BASE.VLR_CTB_PROP_BD        := 0; --VLR_CTB_PROP_BD
            TB_REC_BASE.VLR_CTB_INT_BD         := ATT.FCESP_VLR_CTB_ASSIST(RG.COD_PLANO, TB_REC_BASE.VLR_BENEF_BD_INTE);                                  --VLR_CTB_INT_BD
            

             UPDATE ATT.FC_PRE_TBL_BASE_EXTRAT_CTB
                SET  VLR_BENEF_BD_PROP  = NVL(TB_REC_BASE.VLR_BENEF_BD_PROP,0)
                    ,VLR_BENEF_BD_INTE  = NVL(TB_REC_BASE.VLR_BENEF_BD_INTE,0)
                    ,RENDA_ESTIM_PROP   = NVL(TB_REC_BASE.RENDA_ESTIM_PROP,0)
                    ,RENDA_ESTIM_INT    = NVL(TB_REC_BASE.RENDA_ESTIM_INT,0)
                    ,VLR_CTB_PROP_BD    = NVL(TB_REC_BASE.VLR_CTB_PROP_BD,0)
                    ,VLR_CTB_INT_BD     = NVL(TB_REC_BASE.VLR_CTB_INT_BD,0) 
                 --
                WHERE TPO_DADO          = RG.TPO_DADO
                  AND COD_EMPRS         = RG.COD_EMPRS
                  AND NUM_RGTRO_EMPRG   = RG.NUM_RGTRO_EMPRG
                  AND DTA_FIM_EXTR      = L_DTA_FIM;

               L_C_UPD := SQL%ROWCOUNT;

              IF L_C_UPD > 0 THEN

                 IF (L_COUNT = L_C_UPD) THEN
                    DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: '||TO_CHAR('OK'));
                    --DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: '||TO_CHAR(L_COUNT));
                 END IF;

              END IF;
          
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
      L_NUM_PLBNF  ATT.ADESAO_PLANO_PARTIC_FSS.NUM_PLBNF%TYPE := 19;
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
        
        TB_REC_BASE.VLR_BENEF_BD_INTE   := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, RG.DCR_PLANO, 1, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM); -- VLR_DR
        TB_REC_BASE.RENDA_ESTIM_PROP    := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, RG.DCR_PLANO, 2, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM); -- VLR_DU
        TB_REC_BASE.RENDA_ESTIM_INT     := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, RG.DCR_PLANO, 3, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM); -- VLR_DV
        TB_REC_BASE.VLR_CTB_INT_BD      := ATT.FCESP_VLR_CTB_ASSIST(RG.COD_PLANO, TB_REC_BASE.VLR_BENEF_BD_INTE);                                                                  -- EI
        TB_REC_BASE.VLR_CTB_PROP_BD     := 0;                                                                                                                                      -- EG
        TB_REC_BASE.VLR_SLD_ADICIONAL   := FUN_CALC_VLR(RG.NUM_MATR_PARTF, L_DTA_FIM, RG.COD_PLANO, RG.DCR_PLANO, 4, PCOD_EMPRESA, L_COD_NATBNF, L_NUM_CTFSS, L_COD_UM); -- RES3
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
   
  --VAR_TESTE BOOLEAN;
  
  BEGIN
   --VAR_TESTE := FN_TRATA_ARQUIVO;
   --PRC_CARGA_ARQUIVO('<P_NAME>'); 
      
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