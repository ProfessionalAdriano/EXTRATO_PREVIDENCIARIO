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
    
    
    FUNCTION FUN_CARGA_STAGE (P_CALCULO NUMBER)
    RETURN VARCHAR2 IS
    
    -- VARIABLE TYPE TABLE:
    L_CARGA_STAGE   OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO%ROWTYPE;  
      
    
    BEGIN
       
       IF (P_CALCULO = 1) THEN      
    
              SELECT DADO
                INTO L_CARGA_STAGE.TPO_DADO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;    
           
             IF (L_CARGA_STAGE.TPO_DADO IS NOT NULL)THEN
                RETURN L_CARGA_STAGE.TPO_DADO;   
             END IF;
           
           ELSIF (P_CALCULO = 2)THEN                
             
              SELECT DADO
                INTO L_CARGA_STAGE.COD_EMPRS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;
              
             IF (L_CARGA_STAGE.COD_EMPRS IS NOT NULL)THEN
                RETURN L_CARGA_STAGE.COD_EMPRS;    
             END IF;
             
           ELSIF (P_CALCULO = 3)THEN         
                             
              SELECT DADO
                INTO L_CARGA_STAGE.NUM_RGTRO_EMPRG
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;    
              
             IF (L_CARGA_STAGE.NUM_RGTRO_EMPRG IS NOT NULL)THEN
                RETURN L_CARGA_STAGE.NUM_RGTRO_EMPRG;  
             END IF; 
              
           ELSIF (P_CALCULO = 4)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.NOM_EMPRG
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.NOM_EMPRG IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.NOM_EMPRG;  
             END IF; 
             
           ELSIF (P_CALCULO = 5)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_EMISS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_EMISS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_EMISS;
             END IF;   
             
           ELSIF (P_CALCULO = 6)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.NUM_FOLHA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.NUM_FOLHA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.NUM_FOLHA;
             END IF;  
                          
           ELSIF (P_CALCULO = 7)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PLANO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PLANO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PLANO;
             END IF;             
             
           ELSIF (P_CALCULO = 8)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PER_INIC_EXTR
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PER_INIC_EXTR IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PER_INIC_EXTR;
             END IF;
             
           ELSIF (P_CALCULO = 9)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PER_FIM_EXTR
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PER_FIM_EXTR IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PER_FIM_EXTR;
             END IF;      
             
           ELSIF (P_CALCULO = 10)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_INIC_EXTR
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_INIC_EXTR IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_INIC_EXTR;
             END IF;                                 

           ELSIF (P_CALCULO = 11)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_FIM_EXTR
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_FIM_EXTR IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_FIM_EXTR;
             END IF;             

           ELSIF (P_CALCULO = 12)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_SLD_MOV_SALDADO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_SLD_MOV_SALDADO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_SLD_MOV_SALDADO;
             END IF;

           ELSIF (P_CALCULO = 13)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_PL_SALDADO_MOV_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_PL_SALDADO_MOV_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_PL_SALDADO_MOV_INIC;
             END IF;                          
           
           ELSIF (P_CALCULO = 14)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_PL_SALDADO_MOV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_PL_SALDADO_MOV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_PL_SALDADO_MOV;
             END IF;

           ELSIF (P_CALCULO = 15)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_PL_SALDADO_MOV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_PL_SALDADO_MOV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_PL_SALDADO_MOV;
             END IF; 

           ELSIF (P_CALCULO = 16)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_PL_SALDADO_MOV_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_PL_SALDADO_MOV_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_PL_SALDADO_MOV_FIM;
             END IF;

           ELSIF (P_CALCULO = 17)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_SLD_MOV_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_SLD_MOV_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_SLD_MOV_BD;
             END IF;             
                                                    
           ELSIF (P_CALCULO = 18)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_PL_BD_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_PL_BD_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_PL_BD_INIC;
             END IF;
             
           ELSIF (P_CALCULO = 19)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_PL_MOV_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_PL_MOV_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_PL_MOV_BD;
             END IF;             

           ELSIF (P_CALCULO = 20)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_PL_MOV_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_PL_MOV_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_PL_MOV_BD;
             END IF;  
             
           ELSIF (P_CALCULO = 21)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_PL_BD_MOV_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_PL_BD_MOV_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_PL_BD_MOV_FIM;
             END IF;               
             
           ELSIF (P_CALCULO = 22)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_SLD_MOV_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_SLD_MOV_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_SLD_MOV_CV;
             END IF;                        
             
           ELSIF (P_CALCULO = 23)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_PL_CV_MOV_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_PL_CV_MOV_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_PL_CV_MOV_INIC;
             END IF; 

           ELSIF (P_CALCULO = 24)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_PL_MOV_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_PL_MOV_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_PL_MOV_CV;
             END IF;  
             
           ELSIF (P_CALCULO = 25)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_PL_MOV_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_PL_MOV_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_PL_MOV_CV;
             END IF;             
                      
           ELSIF (P_CALCULO = 26)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_PL_CV_MOV_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_PL_CV_MOV_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_PL_CV_MOV_FIM;
             END IF;                           

           ELSIF (P_CALCULO = 27)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_OBRIG_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_OBRIG_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_OBRIG_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 28)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_OBRIG_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_OBRIG_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_OBRIG_PARTIC;
             END IF; 
             
           ELSIF (P_CALCULO = 29)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_CTA_OBRIG_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_CTA_OBRIG_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_CTA_OBRIG_PARTIC;
             END IF;                         

           ELSIF (P_CALCULO = 30)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_CTA_OBRIG_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_CTA_OBRIG_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_CTA_OBRIG_PARTIC;
             END IF;     

           ELSIF (P_CALCULO = 31)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_OBRIG_PARTIC_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_OBRIG_PARTIC_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_OBRIG_PARTIC_FIM;
             END IF;             

           ELSIF (P_CALCULO = 32)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_NORM_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_NORM_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_NORM_PATROC;
             END IF;      

           ELSIF (P_CALCULO = 33)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_NORM_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_NORM_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_NORM_PATROC;
             END IF;  

           ELSIF (P_CALCULO = 34)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_CTA_NORM_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_CTA_NORM_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_CTA_NORM_PATROC;
             END IF;                                       

           ELSIF (P_CALCULO = 35)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_NORM_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_NORM_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_NORM_PATROC;
             END IF; 
             
           ELSIF (P_CALCULO = 36)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_NORM_PATROC_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_NORM_PATROC_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_NORM_PATROC_INIC;
             END IF;     
             

           ELSIF (P_CALCULO = 37)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_ESPEC_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_ESPEC_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_ESPEC_PARTIC;
             END IF;  
             
           ELSIF (P_CALCULO = 38)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_ESPEC_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_ESPEC_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_ESPEC_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 39)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_CTA_ESPEC_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_CTA_ESPEC_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_CTA_ESPEC_PARTIC;
             END IF;              

           ELSIF (P_CALCULO = 40)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_CTA_ESPEC_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_CTA_ESPEC_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_CTA_ESPEC_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 41)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_ESPEC_PARTIC_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_ESPEC_PARTIC_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_ESPEC_PARTIC_INIC;
             END IF;

           ELSIF (P_CALCULO = 42)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_ESPEC_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_ESPEC_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_ESPEC_PATROC;
             END IF;     

           ELSIF (P_CALCULO = 43)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_ESPEC_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_ESPEC_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_ESPEC_PATROC;
             END IF;

           ELSIF (P_CALCULO = 44)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_CTA_ESPEC_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_CTA_ESPEC_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_CTA_ESPEC_PATROC;
             END IF;

           ELSIF (P_CALCULO = 45)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_CTA_ESPEC_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_CTA_ESPEC_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_CTA_ESPEC_PATROC;
             END IF;

           ELSIF (P_CALCULO = 46)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_CTA_ESPEC_PATROC_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_CTA_ESPEC_PATROC_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_CTA_ESPEC_PATROC_INIC;
             END IF;  

           ELSIF (P_CALCULO = 47)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_TOT_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_TOT_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_TOT_INIC;
             END IF; 

           ELSIF (P_CALCULO = 48)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.CTB_TOT_INIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.CTB_TOT_INIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.CTB_TOT_INIC;
             END IF;

           ELSIF (P_CALCULO = 49)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 50)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_TOT_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_TOT_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_TOT_FIM;
             END IF;

           ELSIF (P_CALCULO = 51)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PRM_MES_PERIODO_CTB
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PRM_MES_PERIODO_CTB IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PRM_MES_PERIODO_CTB;
             END IF;                                                                                                                                       

           ELSIF (P_CALCULO = 52)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SEG_MES_PERIODO_CTB
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SEG_MES_PERIODO_CTB IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SEG_MES_PERIODO_CTB;
             END IF;

           ELSIF (P_CALCULO = 53)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TER_MES_PERIODO_CTB
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TER_MES_PERIODO_CTB IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TER_MES_PERIODO_CTB;
             END IF;

           ELSIF (P_CALCULO = 54)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TOT_CTB_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TOT_CTB_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TOT_CTB_BD;
             END IF;

           ELSIF (P_CALCULO = 55)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_BD_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_BD_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_BD_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 56)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_BD_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_BD_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_BD_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 57)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_BD_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_BD_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_BD_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 58)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_BD_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_BD_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_BD_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 59)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TOT_CTB_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TOT_CTB_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TOT_CTB_CV;
             END IF;

           ELSIF (P_CALCULO = 60)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_CV_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_CV_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_CV_PRM_MES;
             END IF;                                       

           ELSIF (P_CALCULO = 61)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_CV_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_CV_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_CV_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 62)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_CV_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_CV_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_CV_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 63)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_CV_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_CV_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_CV_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 64)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TPO_CTB_VOL_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TPO_CTB_VOL_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TPO_CTB_VOL_PARTIC;
             END IF;                                       

           ELSIF (P_CALCULO = 65)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 66)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 67)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 68)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PARTIC_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 69)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TPO_CTB_VOL_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TPO_CTB_VOL_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TPO_CTB_VOL_PATROC;
             END IF;                                                                                                                                                                                          

           ELSIF (P_CALCULO = 70)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PATROC_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PATROC_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PATROC_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 71)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PATROC_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PATROC_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PATROC_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 72)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PATROC_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PATROC_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PATROC_TER_MES;
             END IF;                          

           ELSIF (P_CALCULO = 73)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_VOL_PATROC_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_VOL_PATROC_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_VOL_PATROC_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 74)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TPO_CTB_OBRIG_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TPO_CTB_OBRIG_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TPO_CTB_OBRIG_PARTIC;
             END IF;             

           ELSIF (P_CALCULO = 75)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 76)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 77)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 78)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PARTIC_PERIODO;
             END IF;
             
           ELSIF (P_CALCULO = 79)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TPO_CTB_OBRIG_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TPO_CTB_OBRIG_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TPO_CTB_OBRIG_PATROC;
             END IF;

           ELSIF (P_CALCULO = 80)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_PRM_MES;
             END IF;  

           ELSIF (P_CALCULO = 81)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_SEG_MES;
             END IF;
             
           ELSIF (P_CALCULO = 82)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 83)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_OBRIG_PATROC_PERIODO;
             END IF; 

           ELSIF (P_CALCULO = 84)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TPO_CTB_ESPOR_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TPO_CTB_ESPOR_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TPO_CTB_ESPOR_PATROC;
             END IF;

           ELSIF (P_CALCULO = 85)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 86)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 87)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 88)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PATROC_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 89)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_TPO_CTB_ESPOR_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_TPO_CTB_ESPOR_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_TPO_CTB_ESPOR_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 90)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 91)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 92)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 93)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_PERIODO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_PERIODO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_ESPOR_PARTIC_PERIODO;
             END IF;

           ELSIF (P_CALCULO = 94)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TOT_CTB_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TOT_CTB_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TOT_CTB_PRM_MES;
             END IF;                                                                                                                                                                                                                                                    

           ELSIF (P_CALCULO = 95)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TOT_CTB_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TOT_CTB_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TOT_CTB_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 96)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TOT_CTB_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TOT_CTB_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TOT_CTB_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 97)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TOT_CTB_EXTRATO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TOT_CTB_EXTRATO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TOT_CTB_EXTRATO;
             END IF;

           ELSIF (P_CALCULO = 98)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PRM_MES_PERIODO_RENT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PRM_MES_PERIODO_RENT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PRM_MES_PERIODO_RENT;
             END IF;

           ELSIF (P_CALCULO = 99)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SEG_MES_PERIODO_RENT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SEG_MES_PERIODO_RENT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SEG_MES_PERIODO_RENT;
             END IF;

           ELSIF (P_CALCULO = 100)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TER_MES_PERIODO_RENT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TER_MES_PERIODO_RENT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TER_MES_PERIODO_RENT;
             END IF; 

           ELSIF (P_CALCULO = 101)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_REAL_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_REAL_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_REAL_PRM_MES;
             END IF; 

           ELSIF (P_CALCULO = 102)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_REAL_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_REAL_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_REAL_SEG_MES;
             END IF; 

           ELSIF (P_CALCULO = 103)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_REAL_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_REAL_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_REAL_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 104)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_REAL_TOT_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_REAL_TOT_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_REAL_TOT_MES;
             END IF;
             
           ELSIF (P_CALCULO = 105)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_LMTD_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_LMTD_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_LMTD_PRM_MES;
             END IF; 

           ELSIF (P_CALCULO = 106)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_LMTD_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_LMTD_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_LMTD_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 107)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_LMTD_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_LMTD_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_LMTD_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 108)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_LMTD_TOT_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_LMTD_TOT_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_LMTD_TOT_MES;
             END IF;

           ELSIF (P_CALCULO = 109)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_IGPDI_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_IGPDI_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_IGPDI_PRM_MES;
             END IF;

           ELSIF (P_CALCULO = 110)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_IGPDI_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_IGPDI_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_IGPDI_SEG_MES;
             END IF;

           ELSIF (P_CALCULO = 111)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_IGPDI_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_IGPDI_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_IGPDI_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 112)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_IGPDI_TOT_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_IGPDI_TOT_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_IGPDI_TOT_MES;
             END IF;

           ELSIF (P_CALCULO = 113)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_URR_PRM_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_URR_PRM_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_URR_PRM_MES;
             END IF; 

           ELSIF (P_CALCULO = 114)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_URR_SEG_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_URR_SEG_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_URR_SEG_MES;
             END IF; 

           ELSIF (P_CALCULO = 115)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_URR_TER_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_URR_TER_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_URR_TER_MES;
             END IF;

           ELSIF (P_CALCULO = 116)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.PCT_RENT_URR_TOT_MES
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.PCT_RENT_URR_TOT_MES IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.PCT_RENT_URR_TOT_MES;
             END IF;                                                                                                                                                                                                                                                                           

           ELSIF (P_CALCULO = 117)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_APOS_PROP
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_APOS_PROP IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_APOS_PROP;
             END IF;

           ELSIF (P_CALCULO = 118)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_APOS_INTE
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_APOS_INTE IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_APOS_INTE;
             END IF;

           ELSIF (P_CALCULO = 119)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_PSAP_PROP
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_PSAP_PROP IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_PSAP_PROP;
             END IF;

           ELSIF (P_CALCULO = 120)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_PSAP_INTE
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_PSAP_INTE IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_PSAP_INTE;
             END IF;

           ELSIF (P_CALCULO = 121)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_BD_PROP
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_BD_PROP IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_BD_PROP;
             END IF;

           ELSIF (P_CALCULO = 122)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_BD_INTE
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_BD_INTE IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_BD_INTE;
             END IF; 

           ELSIF (P_CALCULO = 123)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_CV_PROP
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_CV_PROP IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_CV_PROP;
             END IF;

           ELSIF (P_CALCULO = 124)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_CV_INTE
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_CV_INTE IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_CV_INTE;
             END IF; 

           ELSIF (P_CALCULO = 125)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENDA_ESTIM_PROP
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENDA_ESTIM_PROP IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENDA_ESTIM_PROP;
             END IF;

           ELSIF (P_CALCULO = 126)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENDA_ESTIM_INT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENDA_ESTIM_INT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENDA_ESTIM_INT;
             END IF;

           ELSIF (P_CALCULO = 127)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_RESERV_SALD_LQDA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_RESERV_SALD_LQDA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_RESERV_SALD_LQDA;
             END IF;

           ELSIF (P_CALCULO = 128)THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TXT_PRM_MENS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TXT_PRM_MENS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TXT_PRM_MENS;
             END IF;

           ELSIF (P_CALCULO = 129) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TXT_SEG_MENS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TXT_SEG_MENS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TXT_SEG_MENS;
             END IF;

           ELSIF (P_CALCULO = 130) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TXT_TER_MENS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TXT_TER_MENS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TXT_TER_MENS;
             END IF;

           ELSIF (P_CALCULO = 131) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.TXT_QUA_MENS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.TXT_QUA_MENS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.TXT_QUA_MENS;
             END IF;

           ELSIF (P_CALCULO = 132) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_PROP_BSPS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_PROP_BSPS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_PROP_BSPS;
             END IF;

           ELSIF (P_CALCULO = 133) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_PROP_BSPS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_PROP_BSPS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_PROP_BSPS;
             END IF; 

           ELSIF (P_CALCULO = 134) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_INT_BSPS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_INT_BSPS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_INT_BSPS;
             END IF;

           ELSIF (P_CALCULO = 135) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_INT_BSPS
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_INT_BSPS IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_INT_BSPS;
             END IF;

           ELSIF (P_CALCULO = 136) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_PROP_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_PROP_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_PROP_BD;
             END IF;

           ELSIF (P_CALCULO = 137) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_PROP_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_PROP_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_PROP_BD;
             END IF; 

           ELSIF (P_CALCULO = 138) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_INT_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_INT_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_INT_BD;
             END IF;  

           ELSIF (P_CALCULO = 139) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_INT_BD
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_INT_BD IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_INT_BD;
             END IF;                                                                                                                                                                                                                                                                                                                  
             
           ELSIF (P_CALCULO = 140) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_PROP_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_PROP_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_PROP_CV;
             END IF;

           ELSIF (P_CALCULO = 141) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_PROP_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_PROP_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_PROP_CV;
             END IF;
             
           ELSIF (P_CALCULO = 142) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_INT_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_INT_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_INT_CV;
             END IF;

           ELSIF (P_CALCULO = 143) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CTB_INT_CV
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CTB_INT_CV IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CTB_INT_CV;
             END IF;

           ELSIF (P_CALCULO = 144) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_COTA_INDEX_PLAN_1
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_COTA_INDEX_PLAN_1 IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_COTA_INDEX_PLAN_1;
             END IF; 

           ELSIF (P_CALCULO = 145) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_COTA_INDEX_PLAN_2
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_COTA_INDEX_PLAN_2 IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_COTA_INDEX_PLAN_2;
             END IF;

           ELSIF (P_CALCULO = 146) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_APOS_INDIV_VOL_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_APOS_INDIV_VOL_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_APOS_INDIV_VOL_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 147) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_VOL_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_VOL_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_VOL_PARTI;
             END IF;
             
           ELSIF (P_CALCULO = 148) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_VOL_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_VOL_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_VOL_PARTI;
             END IF;

           ELSIF (P_CALCULO = 149) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_VOL_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_VOL_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_VOL_PARTI;
             END IF;

           ELSIF (P_CALCULO = 150) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_VOL_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_VOL_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_VOL_PARTI;
             END IF;

           ELSIF (P_CALCULO = 151) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_APOS_INDIV_ESPO_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_APOS_INDIV_ESPO_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_APOS_INDIV_ESPO_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 152) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_ESPOPARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_ESPOPARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_ESPOPARTI;
             END IF;

           ELSIF (P_CALCULO = 153) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_ESPOPARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_ESPOPARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_ESPOPARTI;
             END IF;

           ELSIF (P_CALCULO = 154) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_ESPOPARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_ESPOPARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_ESPOPARTI;
             END IF;

           ELSIF (P_CALCULO = 155) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_ESPOPARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_ESPOPARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_ESPOPARTI;
             END IF;

           ELSIF (P_CALCULO = 156) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_APOS_INDIV_VOL_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_APOS_INDIV_VOL_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_APOS_INDIV_VOL_PATROC;
             END IF;

           ELSIF (P_CALCULO = 157) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_VOL_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_VOL_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_VOL_PATRO;
             END IF;                                                                                                                                                                                                                             

           ELSIF (P_CALCULO = 158) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_VOL_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_VOL_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_VOL_PATRO;
             END IF;

           ELSIF (P_CALCULO = 159) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_VOL_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_VOL_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_VOL_PATRO;
             END IF;

           ELSIF (P_CALCULO = 160) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_VOL_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_VOL_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_VOL_PATRO;
             END IF; 

           ELSIF (P_CALCULO = 161) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_CTA_APOS_INDIV_SUPL_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_CTA_APOS_INDIV_SUPL_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_CTA_APOS_INDIV_SUPL_PATROC;
             END IF;
             
           ELSIF (P_CALCULO = 162) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_SUPLPATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_SUPLPATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INI_CTA_APO_INDI_SUPLPATRO;
             END IF;             

           ELSIF (P_CALCULO = 163) THEN           
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_SUPLPATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_SUPLPATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_APO_INDI_SUPLPATRO;
             END IF;                                                    

           ELSIF (P_CALCULO = 164) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_SUPLPATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_SUPLPATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.REN_TOT_CTB_APO_INDI_SUPLPATRO;
             END IF;

           ELSIF (P_CALCULO = 165) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_SUPLPATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_SUPLPATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_APO_INDI_SUPLPATRO;
             END IF;

           ELSIF (P_CALCULO = 166) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PORT_TOTAL
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PORT_TOTAL IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PORT_TOTAL;
             END IF; 

           ELSIF (P_CALCULO = 167) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INIC_CTA_PORT_TOT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INIC_CTA_PORT_TOT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INIC_CTA_PORT_TOT;
             END IF;

           ELSIF (P_CALCULO = 168) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_PORT_TOT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_PORT_TOT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_PORT_TOT;
             END IF;

           ELSIF (P_CALCULO = 169) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_TOT_CTB_PORT_TOT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_TOT_CTB_PORT_TOT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_TOT_CTB_PORT_TOT;
             END IF;

           ELSIF (P_CALCULO = 170) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_PORT_TOT
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_PORT_TOT IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_PORT_TOT;
             END IF;

           ELSIF (P_CALCULO = 171) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PORT_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PORT_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PORT_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 172) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INIC_CTA_PORT_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INIC_CTA_PORT_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INIC_CTA_PORT_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 173) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_PORT_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_PORT_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_PORT_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 174) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_TOT_CTB_PORT_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_TOT_CTB_PORT_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_TOT_CTB_PORT_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 175) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_PORT_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_PORT_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_PORT_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 176) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PORT_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PORT_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PORT_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 177) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INIC_CTA_PORT_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INIC_CTA_PORT_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INIC_CTA_PORT_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 178) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_PORT_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_PORT_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_PORT_FECHADA;
             END IF; 

           ELSIF (P_CALCULO = 179) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_TOT_CTB_PORT_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_TOT_CTB_PORT_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_TOT_CTB_PORT_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 180) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_PORT_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_PORT_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_PORT_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 181) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PORT_JOIA_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PORT_JOIA_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PORT_JOIA_ABERTA;
             END IF; 

           ELSIF (P_CALCULO = 182) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INIC_CTA_PORT_JOIA_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INIC_CTA_PORT_JOIA_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INIC_CTA_PORT_JOIA_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 183) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_PORT_JOIA_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_PORT_JOIA_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_PORT_JOIA_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 184) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_TOT_CTB_PORT_JOIA_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_TOT_CTB_PORT_JOIA_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_TOT_CTB_PORT_JOIA_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 185) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_PORT_JOIA_ABERTA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_PORT_JOIA_ABERTA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_PORT_JOIA_ABERTA;
             END IF;

           ELSIF (P_CALCULO = 186) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PORT_JOIA_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PORT_JOIA_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PORT_JOIA_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 187) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INIC_CTA_PORT_JOIA_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INIC_CTA_PORT_JOIA_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INIC_CTA_PORT_JOIA_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 188) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_PORT_JOIA_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_PORT_JOIA_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_PORT_JOIA_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 189) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_TOT_CTB_PORT_JOIA_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_TOT_CTB_PORT_JOIA_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_TOT_CTB_PORT_JOIA_FECHADA;
             END IF;

           ELSIF (P_CALCULO = 190) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_PORT_JOIA_FECHADA
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_PORT_JOIA_FECHADA IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_PORT_JOIA_FECHADA;
             END IF; 

           ELSIF (P_CALCULO = 191) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_DISTR_FUND_PREV_PARTIC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_DISTR_FUND_PREV_PARTIC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_DISTR_FUND_PREV_PARTIC;
             END IF;

           ELSIF (P_CALCULO = 192) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INI_DIST_FUND_PREV_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INI_DIST_FUND_PREV_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INI_DIST_FUND_PREV_PARTI;
             END IF;

           ELSIF (P_CALCULO = 193) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_DIST_FUND_PREV_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_DIST_FUND_PREV_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_DIST_FUND_PREV_PARTI;
             END IF;

           ELSIF (P_CALCULO = 194) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.REN_TOT_DIST_FUND_PREV_PARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.REN_TOT_DIST_FUND_PREV_PARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.REN_TOT_DIST_FUND_PREV_PARTI;
             END IF;

           ELSIF (P_CALCULO = 195) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLDFIM_CTA_DISTFUNDPREVPARTI
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLDFIM_CTA_DISTFUNDPREVPARTI IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLDFIM_CTA_DISTFUNDPREVPARTI;
             END IF;                                                                                                                                                                                                                                                                                                                                                                                                                            

           ELSIF (P_CALCULO = 196) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_DISTR_FUND_PREV_PATROC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_DISTR_FUND_PREV_PATROC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_DISTR_FUND_PREV_PATROC;
             END IF;            

           ELSIF (P_CALCULO = 197) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INI_DIST_FUND_PREV_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INI_DIST_FUND_PREV_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INI_DIST_FUND_PREV_PATRO;
             END IF;
             
           ELSIF (P_CALCULO = 198) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_DIST_FUND_PREV_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_DIST_FUND_PREV_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_DIST_FUND_PREV_PATRO;
             END IF;

           ELSIF (P_CALCULO = 199) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.REN_TOT_DIST_FUND_PREV_PATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.REN_TOT_DIST_FUND_PREV_PATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.REN_TOT_DIST_FUND_PREV_PATRO;
             END IF;

           ELSIF (P_CALCULO = 200) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLDFIM_CTA_DISTFUNDPREVPATRO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLDFIM_CTA_DISTFUNDPREVPATRO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLDFIM_CTA_DISTFUNDPREVPATRO;
             END IF;

           ELSIF (P_CALCULO = 201) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_PORT_FINAL
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_PORT_FINAL IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_PORT_FINAL;
             END IF;  

           ELSIF (P_CALCULO = 202) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_INIC_CTA_PORT_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_INIC_CTA_PORT_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_INIC_CTA_PORT_FIM;
             END IF;

           ELSIF (P_CALCULO = 203) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_TOT_CTB_PORT_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_TOT_CTB_PORT_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_TOT_CTB_PORT_FIM;
             END IF;

           ELSIF (P_CALCULO = 204) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.RENT_TOT_CTB_PORT_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.RENT_TOT_CTB_PORT_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.RENT_TOT_CTB_PORT_FIM;
             END IF;

           ELSIF (P_CALCULO = 205) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.SLD_FIM_CTA_PORT_FIM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.SLD_FIM_CTA_PORT_FIM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.SLD_FIM_CTA_PORT_FIM;
             END IF;

           ELSIF (P_CALCULO = 206) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DCR_SLD_PROJETADO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DCR_SLD_PROJETADO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DCR_SLD_PROJETADO;
             END IF;

           ELSIF (P_CALCULO = 207) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_SLD_PROJETADO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_SLD_PROJETADO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_SLD_PROJETADO;
             END IF; 

           ELSIF (P_CALCULO = 208) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_SLD_ADICIONAL
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_SLD_ADICIONAL IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_SLD_ADICIONAL;
             END IF;

           ELSIF (P_CALCULO = 209) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_BENEF_ADICIONAL
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_BENEF_ADICIONAL IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_BENEF_ADICIONAL;
             END IF;

           ELSIF (P_CALCULO = 210) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_ULT_ATUAL
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_ULT_ATUAL IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_ULT_ATUAL;
             END IF;

           ELSIF (P_CALCULO = 211) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CONTRIB_RISCO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CONTRIB_RISCO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CONTRIB_RISCO;
             END IF;

           ELSIF (P_CALCULO = 212) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CONTRIB_PATRC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CONTRIB_PATRC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CONTRIB_PATRC;
             END IF;

           ELSIF (P_CALCULO = 213) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CAPIT_SEGURADO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CAPIT_SEGURADO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CAPIT_SEGURADO;
             END IF;

           ELSIF (P_CALCULO = 214) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CONTRIB_ADM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CONTRIB_ADM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CONTRIB_ADM;
             END IF; 

           ELSIF (P_CALCULO = 215) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_CONTRIB_ADM_PATRC
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_CONTRIB_ADM_PATRC IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_CONTRIB_ADM_PATRC;
             END IF;                                                                                                                                                                                                                         

           ELSIF (P_CALCULO = 216) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_SIMUL_BENEF_PORCETAGEM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_SIMUL_BENEF_PORCETAGEM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_SIMUL_BENEF_PORCETAGEM;
             END IF;

           ELSIF (P_CALCULO = 217) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_ELEGIB_BENEF_PORCETAGEM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_ELEGIB_BENEF_PORCETAGEM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_ELEGIB_BENEF_PORCETAGEM;
             END IF; 

           ELSIF (P_CALCULO = 218) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_ELEGIB_PORCETAGEM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_ELEGIB_PORCETAGEM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_ELEGIB_PORCETAGEM;
             END IF;

           ELSIF (P_CALCULO = 219) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_EXAURIM_BENEF_PORCETAGEM
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_EXAURIM_BENEF_PORCETAGEM IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_EXAURIM_BENEF_PORCETAGEM;
             END IF;

           ELSIF (P_CALCULO = 220) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.VLR_SIMUL_BENEF_PRAZO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.VLR_SIMUL_BENEF_PRAZO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.VLR_SIMUL_BENEF_PRAZO;
             END IF;

           ELSIF (P_CALCULO = 221) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_ELEGIB_BENEF_PRAZO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_ELEGIB_BENEF_PRAZO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_ELEGIB_BENEF_PRAZO;
             END IF;

           ELSIF (P_CALCULO = 222) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.IDADE_ELEGIB_BENEF_PRAZO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.IDADE_ELEGIB_BENEF_PRAZO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.IDADE_ELEGIB_BENEF_PRAZO;
             END IF;

           ELSIF (P_CALCULO = 223) THEN          
           
              SELECT DADO
                INTO L_CARGA_STAGE.DTA_EXAURIM_BENEF_PRAZO
              FROM OWN_FUNCESP.FC_CARGA_EXTRATO
              WHERE INDX = P_CALCULO;  
              
              IF ( L_CARGA_STAGE.DTA_EXAURIM_BENEF_PRAZO IS NOT NULL)THEN
                RETURN  L_CARGA_STAGE.DTA_EXAURIM_BENEF_PRAZO;
             END IF;               
                                                                                                      
                                                                             
           ELSE
             DBMS_OUTPUT.PUT_LINE('---------------');
       
       END IF; 
    
        EXCEPTION
          WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
           DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    
    END FUN_CARGA_STAGE;
    
    
    
    PROCEDURE PRC_CARGA_ARQUIVO IS
    
       L_REGISTRO      CLOB;
       L_CONTEUDO      CLOB;
       SSQL            CLOB; 
       
          
       -- VARIABLE TYPE TABLE:
       REC_CARGA       OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO%ROWTYPE;
          
   
    BEGIN
        G_ARQ := UTL_FILE.FOPEN(G_DIR,G_NAME,G_READ,G_SIZE);
                
          LOOP                

               UTL_FILE.GET_LINE(G_ARQ, L_REGISTRO);

          
            IF SUBSTR(L_REGISTRO, 1,1 ) = '1' THEN
               --DBMS_OUTPUT.PUT_LINE(L_REGISTRO);     
              

               DELETE FROM OWN_FUNCESP.FC_CARGA_EXTRATO;
                COMMIT;
                            
               SSQL := ' INSERT INTO OWN_FUNCESP.FC_CARGA_EXTRATO (DADO, INDX)' || 
                       ' with temp as '||
                       ' ( select ''' || replace(L_REGISTRO, '''', ' ') || '''  DADOS from dual )' ||
               
                       ' select distinct ' ||
                       '        trim(regexp_substr(t.DADOS, ''[^;]+'', 1, levels.column_value))  as DADOS, levels.column_value Nivel ' ||
                       ' from ' ||
                       '      temp t, ' ||
                       '      table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(t.DADOS, ''[^;]+''))  + 1) as sys.OdciNumberList)) levels';
                        
              
                EXECUTE IMMEDIATE(SSQL);
                COMMIT;
                                                                                                    
            REC_CARGA.TPO_DADO								:= FUN_CARGA_STAGE(1);
            REC_CARGA.COD_EMPRS                             := FUN_CARGA_STAGE(2);
            REC_CARGA.NUM_RGTRO_EMPRG                       := FUN_CARGA_STAGE(3);
            REC_CARGA.NOM_EMPRG                             := FUN_CARGA_STAGE(4);
            REC_CARGA.DTA_EMISS                             := FUN_CARGA_STAGE(5);
            REC_CARGA.NUM_FOLHA                             := FUN_CARGA_STAGE(6);
            REC_CARGA.DCR_PLANO                             := FUN_CARGA_STAGE(7);
            REC_CARGA.PER_INIC_EXTR                         := FUN_CARGA_STAGE(8);
            REC_CARGA.PER_FIM_EXTR                          := FUN_CARGA_STAGE(9);
            REC_CARGA.DTA_INIC_EXTR                         := FUN_CARGA_STAGE(10);
            REC_CARGA.DTA_FIM_EXTR                          := FUN_CARGA_STAGE(11);
            REC_CARGA.DCR_SLD_MOV_SALDADO                   := FUN_CARGA_STAGE(12);
            REC_CARGA.SLD_PL_SALDADO_MOV_INIC               := FUN_CARGA_STAGE(13);
            REC_CARGA.CTB_PL_SALDADO_MOV                    := FUN_CARGA_STAGE(14);
            REC_CARGA.RENT_PL_SALDADO_MOV                   := FUN_CARGA_STAGE(15);
            REC_CARGA.SLD_PL_SALDADO_MOV_FIM                := FUN_CARGA_STAGE(16);
            REC_CARGA.DCR_SLD_MOV_BD                        := FUN_CARGA_STAGE(17);
            REC_CARGA.SLD_PL_BD_INIC                        := FUN_CARGA_STAGE(18);
            REC_CARGA.CTB_PL_MOV_BD                         := FUN_CARGA_STAGE(19);
            REC_CARGA.RENT_PL_MOV_BD                        := FUN_CARGA_STAGE(20);
            REC_CARGA.SLD_PL_BD_MOV_FIM                     := FUN_CARGA_STAGE(21);
            REC_CARGA.DCR_SLD_MOV_CV                        := FUN_CARGA_STAGE(22);
            REC_CARGA.SLD_PL_CV_MOV_INIC                    := FUN_CARGA_STAGE(23);
            REC_CARGA.CTB_PL_MOV_CV                         := FUN_CARGA_STAGE(24);
            REC_CARGA.RENT_PL_MOV_CV                        := FUN_CARGA_STAGE(25);
            REC_CARGA.SLD_PL_CV_MOV_FIM                     := FUN_CARGA_STAGE(26);
            REC_CARGA.DCR_CTA_OBRIG_PARTIC                  := FUN_CARGA_STAGE(27);
            REC_CARGA.SLD_CTA_OBRIG_PARTIC                  := FUN_CARGA_STAGE(28);
            REC_CARGA.CTB_CTA_OBRIG_PARTIC                  := FUN_CARGA_STAGE(29);
            REC_CARGA.RENT_CTA_OBRIG_PARTIC                 := FUN_CARGA_STAGE(30);
            REC_CARGA.SLD_CTA_OBRIG_PARTIC_FIM              := FUN_CARGA_STAGE(31);
            REC_CARGA.DCR_CTA_NORM_PATROC                   := FUN_CARGA_STAGE(32);
            REC_CARGA.SLD_CTA_NORM_PATROC                   := FUN_CARGA_STAGE(33);
            REC_CARGA.CTB_CTA_NORM_PATROC                   := FUN_CARGA_STAGE(34);
            REC_CARGA.RENT_NORM_PATROC                      := FUN_CARGA_STAGE(35);
            REC_CARGA.SLD_NORM_PATROC_INIC                  := FUN_CARGA_STAGE(36);
            REC_CARGA.DCR_CTA_ESPEC_PARTIC                  := FUN_CARGA_STAGE(37);
            REC_CARGA.SLD_CTA_ESPEC_PARTIC                  := FUN_CARGA_STAGE(38);
            REC_CARGA.CTB_CTA_ESPEC_PARTIC                  := FUN_CARGA_STAGE(39);
            REC_CARGA.RENT_CTA_ESPEC_PARTIC                 := FUN_CARGA_STAGE(40);
            REC_CARGA.SLD_CTA_ESPEC_PARTIC_INIC             := FUN_CARGA_STAGE(41);
            REC_CARGA.DCR_CTA_ESPEC_PATROC                  := FUN_CARGA_STAGE(42);
            REC_CARGA.SLD_CTA_ESPEC_PATROC                  := FUN_CARGA_STAGE(43);
            REC_CARGA.CTB_CTA_ESPEC_PATROC                  := FUN_CARGA_STAGE(44);
            REC_CARGA.RENT_CTA_ESPEC_PATROC                 := FUN_CARGA_STAGE(45);
            REC_CARGA.SLD_CTA_ESPEC_PATROC_INIC             := FUN_CARGA_STAGE(46);
            REC_CARGA.SLD_TOT_INIC                          := FUN_CARGA_STAGE(47);
            REC_CARGA.CTB_TOT_INIC                          := FUN_CARGA_STAGE(48);
            REC_CARGA.RENT_PERIODO                          := FUN_CARGA_STAGE(49);
            REC_CARGA.SLD_TOT_FIM                           := FUN_CARGA_STAGE(50);
            REC_CARGA.PRM_MES_PERIODO_CTB                   := FUN_CARGA_STAGE(51);
            REC_CARGA.SEG_MES_PERIODO_CTB                   := FUN_CARGA_STAGE(52);
            REC_CARGA.TER_MES_PERIODO_CTB                   := FUN_CARGA_STAGE(53);
            REC_CARGA.DCR_TOT_CTB_BD                        := FUN_CARGA_STAGE(54);
            REC_CARGA.VLR_TOT_CTB_BD_PRM_MES                := FUN_CARGA_STAGE(55);
            REC_CARGA.VLR_TOT_CTB_BD_SEG_MES                := FUN_CARGA_STAGE(56);
            REC_CARGA.VLR_TOT_CTB_BD_TER_MES                := FUN_CARGA_STAGE(57);
            REC_CARGA.VLR_TOT_CTB_BD_PERIODO                := FUN_CARGA_STAGE(58);
            REC_CARGA.DCR_TOT_CTB_CV                        := FUN_CARGA_STAGE(59);
            REC_CARGA.VLR_TOT_CTB_CV_PRM_MES                := FUN_CARGA_STAGE(60);
            REC_CARGA.VLR_TOT_CTB_CV_SEG_MES                := FUN_CARGA_STAGE(61);
            REC_CARGA.VLR_TOT_CTB_CV_TER_MES                := FUN_CARGA_STAGE(62);
            REC_CARGA.VLR_TOT_CTB_CV_PERIODO                := FUN_CARGA_STAGE(63);
            REC_CARGA.DCR_TPO_CTB_VOL_PARTIC                := FUN_CARGA_STAGE(64);
            REC_CARGA.VLR_CTB_VOL_PARTIC_PRM_MES            := FUN_CARGA_STAGE(65);
            REC_CARGA.VLR_CTB_VOL_PARTIC_SEG_MES            := FUN_CARGA_STAGE(66);
            REC_CARGA.VLR_CTB_VOL_PARTIC_TER_MES            := FUN_CARGA_STAGE(67);
            REC_CARGA.VLR_CTB_VOL_PARTIC_PERIODO            := FUN_CARGA_STAGE(68);
            REC_CARGA.DCR_TPO_CTB_VOL_PATROC                := FUN_CARGA_STAGE(69);
            REC_CARGA.VLR_CTB_VOL_PATROC_PRM_MES            := FUN_CARGA_STAGE(70);
            REC_CARGA.VLR_CTB_VOL_PATROC_SEG_MES            := FUN_CARGA_STAGE(71);
            REC_CARGA.VLR_CTB_VOL_PATROC_TER_MES            := FUN_CARGA_STAGE(72);
            REC_CARGA.VLR_CTB_VOL_PATROC_PERIODO            := FUN_CARGA_STAGE(73);
            REC_CARGA.DCR_TPO_CTB_OBRIG_PARTIC              := FUN_CARGA_STAGE(74);
            REC_CARGA.VLR_CTB_OBRIG_PARTIC_PRM_MES          := FUN_CARGA_STAGE(75);
            REC_CARGA.VLR_CTB_OBRIG_PARTIC_SEG_MES          := FUN_CARGA_STAGE(76);
            REC_CARGA.VLR_CTB_OBRIG_PARTIC_TER_MES          := FUN_CARGA_STAGE(77);
            REC_CARGA.VLR_CTB_OBRIG_PARTIC_PERIODO          := FUN_CARGA_STAGE(78);
            REC_CARGA.DCR_TPO_CTB_OBRIG_PATROC              := FUN_CARGA_STAGE(79);
            REC_CARGA.VLR_CTB_OBRIG_PATROC_PRM_MES          := FUN_CARGA_STAGE(80);
            REC_CARGA.VLR_CTB_OBRIG_PATROC_SEG_MES          := FUN_CARGA_STAGE(81);
            REC_CARGA.VLR_CTB_OBRIG_PATROC_TER_MES          := FUN_CARGA_STAGE(82);
            REC_CARGA.VLR_CTB_OBRIG_PATROC_PERIODO          := FUN_CARGA_STAGE(83);
            REC_CARGA.DCR_TPO_CTB_ESPOR_PATROC              := FUN_CARGA_STAGE(84);
            REC_CARGA.VLR_CTB_ESPOR_PATROC_PRM_MES          := FUN_CARGA_STAGE(85);
            REC_CARGA.VLR_CTB_ESPOR_PATROC_SEG_MES          := FUN_CARGA_STAGE(86);
            REC_CARGA.VLR_CTB_ESPOR_PATROC_TER_MES          := FUN_CARGA_STAGE(87);
            REC_CARGA.VLR_CTB_ESPOR_PATROC_PERIODO          := FUN_CARGA_STAGE(88);
            REC_CARGA.DCR_TPO_CTB_ESPOR_PARTIC              := FUN_CARGA_STAGE(89);
            REC_CARGA.VLR_CTB_ESPOR_PARTIC_PRM_MES          := FUN_CARGA_STAGE(90);
            REC_CARGA.VLR_CTB_ESPOR_PARTIC_SEG_MES          := FUN_CARGA_STAGE(91);
            REC_CARGA.VLR_CTB_ESPOR_PARTIC_TER_MES          := FUN_CARGA_STAGE(92);
            REC_CARGA.VLR_CTB_ESPOR_PARTIC_PERIODO          := FUN_CARGA_STAGE(93);
            REC_CARGA.TOT_CTB_PRM_MES                       := FUN_CARGA_STAGE(94);
            REC_CARGA.TOT_CTB_SEG_MES                       := FUN_CARGA_STAGE(95);
            REC_CARGA.TOT_CTB_TER_MES                       := FUN_CARGA_STAGE(96);
            REC_CARGA.TOT_CTB_EXTRATO                       := FUN_CARGA_STAGE(97);
            REC_CARGA.PRM_MES_PERIODO_RENT                  := FUN_CARGA_STAGE(98);
            REC_CARGA.SEG_MES_PERIODO_RENT                  := FUN_CARGA_STAGE(99);
            REC_CARGA.TER_MES_PERIODO_RENT                  := FUN_CARGA_STAGE(100);
            REC_CARGA.PCT_RENT_REAL_PRM_MES                 := FUN_CARGA_STAGE(101);
            REC_CARGA.PCT_RENT_REAL_SEG_MES                 := FUN_CARGA_STAGE(102);
            REC_CARGA.PCT_RENT_REAL_TER_MES                 := FUN_CARGA_STAGE(103);
            REC_CARGA.PCT_RENT_REAL_TOT_MES                 := FUN_CARGA_STAGE(104);
            REC_CARGA.PCT_RENT_LMTD_PRM_MES                 := FUN_CARGA_STAGE(105);
            REC_CARGA.PCT_RENT_LMTD_SEG_MES                 := FUN_CARGA_STAGE(106);
            REC_CARGA.PCT_RENT_LMTD_TER_MES                 := FUN_CARGA_STAGE(107);
            REC_CARGA.PCT_RENT_LMTD_TOT_MES                 := FUN_CARGA_STAGE(108);
            REC_CARGA.PCT_RENT_IGPDI_PRM_MES                := FUN_CARGA_STAGE(109);
            REC_CARGA.PCT_RENT_IGPDI_SEG_MES                := FUN_CARGA_STAGE(110);
            REC_CARGA.PCT_RENT_IGPDI_TER_MES                := FUN_CARGA_STAGE(111);
            REC_CARGA.PCT_RENT_IGPDI_TOT_MES                := FUN_CARGA_STAGE(112);
            REC_CARGA.PCT_RENT_URR_PRM_MES                  := FUN_CARGA_STAGE(113);
            REC_CARGA.PCT_RENT_URR_SEG_MES                  := FUN_CARGA_STAGE(114);
            REC_CARGA.PCT_RENT_URR_TER_MES                  := FUN_CARGA_STAGE(115);
            REC_CARGA.PCT_RENT_URR_TOT_MES                  := FUN_CARGA_STAGE(116);
            REC_CARGA.DTA_APOS_PROP                         := FUN_CARGA_STAGE(117);
            REC_CARGA.DTA_APOS_INTE                         := FUN_CARGA_STAGE(118);
            REC_CARGA.VLR_BENEF_PSAP_PROP                   := FUN_CARGA_STAGE(119);
            REC_CARGA.VLR_BENEF_PSAP_INTE                   := FUN_CARGA_STAGE(120);
            REC_CARGA.VLR_BENEF_BD_PROP                     := FUN_CARGA_STAGE(121);
            REC_CARGA.VLR_BENEF_BD_INTE                     := FUN_CARGA_STAGE(122);
            REC_CARGA.VLR_BENEF_CV_PROP                     := FUN_CARGA_STAGE(123);
            REC_CARGA.VLR_BENEF_CV_INTE                     := FUN_CARGA_STAGE(124);
            REC_CARGA.RENDA_ESTIM_PROP                      := FUN_CARGA_STAGE(125);
            REC_CARGA.RENDA_ESTIM_INT                       := FUN_CARGA_STAGE(126);
            REC_CARGA.VLR_RESERV_SALD_LQDA                  := FUN_CARGA_STAGE(127);
            REC_CARGA.TXT_PRM_MENS                          := FUN_CARGA_STAGE(128);
            REC_CARGA.TXT_SEG_MENS                          := FUN_CARGA_STAGE(129);
            REC_CARGA.TXT_TER_MENS                          := FUN_CARGA_STAGE(130);
            REC_CARGA.TXT_QUA_MENS                          := FUN_CARGA_STAGE(131);
            REC_CARGA.IDADE_PROP_BSPS                       := FUN_CARGA_STAGE(132);
            REC_CARGA.VLR_CTB_PROP_BSPS                     := FUN_CARGA_STAGE(133);
            REC_CARGA.IDADE_INT_BSPS                        := FUN_CARGA_STAGE(134);
            REC_CARGA.VLR_CTB_INT_BSPS                      := FUN_CARGA_STAGE(135);
            REC_CARGA.IDADE_PROP_BD                         := FUN_CARGA_STAGE(136);
            REC_CARGA.VLR_CTB_PROP_BD                       := FUN_CARGA_STAGE(137);
            REC_CARGA.IDADE_INT_BD                          := FUN_CARGA_STAGE(138);
            REC_CARGA.VLR_CTB_INT_BD                        := FUN_CARGA_STAGE(139);
            REC_CARGA.IDADE_PROP_CV                         := FUN_CARGA_STAGE(140);
            REC_CARGA.VLR_CTB_PROP_CV                       := FUN_CARGA_STAGE(141);
            REC_CARGA.IDADE_INT_CV                          := FUN_CARGA_STAGE(142);
            REC_CARGA.VLR_CTB_INT_CV                        := FUN_CARGA_STAGE(143);
            REC_CARGA.DCR_COTA_INDEX_PLAN_1                 := FUN_CARGA_STAGE(144);
            REC_CARGA.DCR_COTA_INDEX_PLAN_2                 := FUN_CARGA_STAGE(145);
            REC_CARGA.DCR_CTA_APOS_INDIV_VOL_PARTIC         := FUN_CARGA_STAGE(146);
            REC_CARGA.SLD_INI_CTA_APO_INDI_VOL_PARTI        := FUN_CARGA_STAGE(147);
            REC_CARGA.VLR_TOT_CTB_APO_INDI_VOL_PARTI        := FUN_CARGA_STAGE(148);
            REC_CARGA.REN_TOT_CTB_APO_INDI_VOL_PARTI        := FUN_CARGA_STAGE(149);
            REC_CARGA.SLD_FIM_CTA_APO_INDI_VOL_PARTI        := FUN_CARGA_STAGE(150);
            REC_CARGA.DCR_CTA_APOS_INDIV_ESPO_PARTIC        := FUN_CARGA_STAGE(151);
            REC_CARGA.SLD_INI_CTA_APO_INDI_ESPOPARTI        := FUN_CARGA_STAGE(152);
            REC_CARGA.VLR_TOT_CTB_APO_INDI_ESPOPARTI        := FUN_CARGA_STAGE(153);
            REC_CARGA.REN_TOT_CTB_APO_INDI_ESPOPARTI        := FUN_CARGA_STAGE(154);
            REC_CARGA.SLD_FIM_CTA_APO_INDI_ESPOPARTI        := FUN_CARGA_STAGE(155);
            REC_CARGA.DCR_CTA_APOS_INDIV_VOL_PATROC         := FUN_CARGA_STAGE(156);
            REC_CARGA.SLD_INI_CTA_APO_INDI_VOL_PATRO        := FUN_CARGA_STAGE(157);
            REC_CARGA.VLR_TOT_CTB_APO_INDI_VOL_PATRO        := FUN_CARGA_STAGE(158);
            REC_CARGA.REN_TOT_CTB_APO_INDI_VOL_PATRO        := FUN_CARGA_STAGE(159);
            REC_CARGA.SLD_FIM_CTA_APO_INDI_VOL_PATRO        := FUN_CARGA_STAGE(160);
            REC_CARGA.DCR_CTA_APOS_INDIV_SUPL_PATROC        := FUN_CARGA_STAGE(161);
            REC_CARGA.SLD_INI_CTA_APO_INDI_SUPLPATRO        := FUN_CARGA_STAGE(162);
            REC_CARGA.VLR_TOT_CTB_APO_INDI_SUPLPATRO        := FUN_CARGA_STAGE(163);
            REC_CARGA.REN_TOT_CTB_APO_INDI_SUPLPATRO        := FUN_CARGA_STAGE(164);
            REC_CARGA.SLD_FIM_CTA_APO_INDI_SUPLPATRO        := FUN_CARGA_STAGE(165);
            REC_CARGA.DCR_PORT_TOTAL                        := FUN_CARGA_STAGE(166);
            REC_CARGA.SLD_INIC_CTA_PORT_TOT                 := FUN_CARGA_STAGE(167);
            REC_CARGA.VLR_TOT_CTB_PORT_TOT                  := FUN_CARGA_STAGE(168);
            REC_CARGA.RENT_TOT_CTB_PORT_TOT                 := FUN_CARGA_STAGE(169);
            REC_CARGA.SLD_FIM_CTA_PORT_TOT                  := FUN_CARGA_STAGE(170);
            REC_CARGA.DCR_PORT_ABERTA                       := FUN_CARGA_STAGE(171);
            REC_CARGA.SLD_INIC_CTA_PORT_ABERTA              := FUN_CARGA_STAGE(172);
            REC_CARGA.VLR_TOT_CTB_PORT_ABERTA               := FUN_CARGA_STAGE(173);
            REC_CARGA.RENT_TOT_CTB_PORT_ABERTA              := FUN_CARGA_STAGE(174);
            REC_CARGA.SLD_FIM_CTA_PORT_ABERTA               := FUN_CARGA_STAGE(175);
            REC_CARGA.DCR_PORT_FECHADA                      := FUN_CARGA_STAGE(176);
            REC_CARGA.SLD_INIC_CTA_PORT_FECHADA             := FUN_CARGA_STAGE(177);
            REC_CARGA.VLR_TOT_CTB_PORT_FECHADA              := FUN_CARGA_STAGE(178);
            REC_CARGA.RENT_TOT_CTB_PORT_FECHADA             := FUN_CARGA_STAGE(179);
            REC_CARGA.SLD_FIM_CTA_PORT_FECHADA              := FUN_CARGA_STAGE(180);
            REC_CARGA.DCR_PORT_JOIA_ABERTA                  := FUN_CARGA_STAGE(181);
            REC_CARGA.SLD_INIC_CTA_PORT_JOIA_ABERTA         := FUN_CARGA_STAGE(182);
            REC_CARGA.VLR_TOT_CTB_PORT_JOIA_ABERTA          := FUN_CARGA_STAGE(183);
            REC_CARGA.RENT_TOT_CTB_PORT_JOIA_ABERTA         := FUN_CARGA_STAGE(184);
            REC_CARGA.SLD_FIM_CTA_PORT_JOIA_ABERTA          := FUN_CARGA_STAGE(185);
            REC_CARGA.DCR_PORT_JOIA_FECHADA                 := FUN_CARGA_STAGE(186);
            REC_CARGA.SLD_INIC_CTA_PORT_JOIA_FECHADA        := FUN_CARGA_STAGE(187);
            REC_CARGA.VLR_TOT_CTB_PORT_JOIA_FECHADA         := FUN_CARGA_STAGE(188);
            REC_CARGA.RENT_TOT_CTB_PORT_JOIA_FECHADA        := FUN_CARGA_STAGE(189);
            REC_CARGA.SLD_FIM_CTA_PORT_JOIA_FECHADA         := FUN_CARGA_STAGE(190);
            REC_CARGA.DCR_DISTR_FUND_PREV_PARTIC            := FUN_CARGA_STAGE(191);
            REC_CARGA.SLD_INI_DIST_FUND_PREV_PARTI          := FUN_CARGA_STAGE(192);
            REC_CARGA.VLR_TOT_DIST_FUND_PREV_PARTI          := FUN_CARGA_STAGE(193);
            REC_CARGA.REN_TOT_DIST_FUND_PREV_PARTI          := FUN_CARGA_STAGE(194);
            REC_CARGA.SLDFIM_CTA_DISTFUNDPREVPARTI          := FUN_CARGA_STAGE(195);
            REC_CARGA.DCR_DISTR_FUND_PREV_PATROC            := FUN_CARGA_STAGE(196);
            REC_CARGA.SLD_INI_DIST_FUND_PREV_PATRO          := FUN_CARGA_STAGE(197);
            REC_CARGA.VLR_TOT_DIST_FUND_PREV_PATRO          := FUN_CARGA_STAGE(198);
            REC_CARGA.REN_TOT_DIST_FUND_PREV_PATRO          := FUN_CARGA_STAGE(199);
            REC_CARGA.SLDFIM_CTA_DISTFUNDPREVPATRO          := FUN_CARGA_STAGE(200);
            REC_CARGA.DCR_PORT_FINAL                        := FUN_CARGA_STAGE(201);
            REC_CARGA.SLD_INIC_CTA_PORT_FIM                 := FUN_CARGA_STAGE(202);
            REC_CARGA.VLR_TOT_CTB_PORT_FIM                  := FUN_CARGA_STAGE(203);
            REC_CARGA.RENT_TOT_CTB_PORT_FIM                 := FUN_CARGA_STAGE(204);
            REC_CARGA.SLD_FIM_CTA_PORT_FIM                  := FUN_CARGA_STAGE(205);
            REC_CARGA.DCR_SLD_PROJETADO                     := FUN_CARGA_STAGE(206);
            REC_CARGA.VLR_SLD_PROJETADO                     := FUN_CARGA_STAGE(207);
            REC_CARGA.VLR_SLD_ADICIONAL                     := FUN_CARGA_STAGE(208);
            REC_CARGA.VLR_BENEF_ADICIONAL                   := FUN_CARGA_STAGE(209);
            REC_CARGA.DTA_ULT_ATUAL                         := FUN_CARGA_STAGE(210);
            REC_CARGA.VLR_CONTRIB_RISCO                     := FUN_CARGA_STAGE(211);
            REC_CARGA.VLR_CONTRIB_PATRC                     := FUN_CARGA_STAGE(212);
            REC_CARGA.VLR_CAPIT_SEGURADO                    := FUN_CARGA_STAGE(213);
            REC_CARGA.VLR_CONTRIB_ADM                       := FUN_CARGA_STAGE(214);
            REC_CARGA.VLR_CONTRIB_ADM_PATRC                 := FUN_CARGA_STAGE(215);
            REC_CARGA.VLR_SIMUL_BENEF_PORCETAGEM            := FUN_CARGA_STAGE(216);
            REC_CARGA.DTA_ELEGIB_BENEF_PORCETAGEM           := FUN_CARGA_STAGE(217);
            REC_CARGA.IDADE_ELEGIB_PORCETAGEM               := FUN_CARGA_STAGE(218);
            REC_CARGA.DTA_EXAURIM_BENEF_PORCETAGEM          := FUN_CARGA_STAGE(219);
            REC_CARGA.VLR_SIMUL_BENEF_PRAZO                 := FUN_CARGA_STAGE(220);
            REC_CARGA.DTA_ELEGIB_BENEF_PRAZO                := FUN_CARGA_STAGE(221);
            REC_CARGA.IDADE_ELEGIB_BENEF_PRAZO              := FUN_CARGA_STAGE(222);
            REC_CARGA.DTA_EXAURIM_BENEF_PRAZO               := FUN_CARGA_STAGE(223);

                      
            DBMS_OUTPUT.PUT_LINE(REC_CARGA.TPO_DADO ||' '||
                                 REC_CARGA.COD_EMPRS ||' '||
                                 REC_CARGA.NUM_RGTRO_EMPRG ||' '||
                                 REC_CARGA.NOM_EMPRG );             
                                    
/*            INSERT INTO OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO VALUES (  REC_CARGA.TPO_DADO
                                                                      ,REC_CARGA.COD_EMPRS
                                                                      ,REC_CARGA.NUM_RGTRO_EMPRG
                                                                      ,REC_CARGA.NOM_EMPRG
                                                                      ,REC_CARGA.DTA_EMISS
                                                                      ,REC_CARGA.NUM_FOLHA
                                                                      ,REC_CARGA.DCR_PLANO
                                                                      ,REC_CARGA.PER_INIC_EXTR
                                                                      ,REC_CARGA.PER_FIM_EXTR
                                                                      ,REC_CARGA.DTA_INIC_EXTR
                                                                      ,REC_CARGA.DTA_FIM_EXTR
                                                                      ,REC_CARGA.DCR_SLD_MOV_SALDADO
                                                                      ,REC_CARGA.SLD_PL_SALDADO_MOV_INIC
                                                                      ,REC_CARGA.CTB_PL_SALDADO_MOV
                                                                      ,REC_CARGA.RENT_PL_SALDADO_MOV
                                                                      ,REC_CARGA.SLD_PL_SALDADO_MOV_FIM
                                                                      ,REC_CARGA.DCR_SLD_MOV_BD
                                                                      ,REC_CARGA.SLD_PL_BD_INIC
                                                                      ,REC_CARGA.CTB_PL_MOV_BD
                                                                      ,REC_CARGA.RENT_PL_MOV_BD
                                                                      ,REC_CARGA.SLD_PL_BD_MOV_FIM
                                                                      ,REC_CARGA.DCR_SLD_MOV_CV
                                                                      ,REC_CARGA.SLD_PL_CV_MOV_INIC
                                                                      ,REC_CARGA.CTB_PL_MOV_CV
                                                                      ,REC_CARGA.RENT_PL_MOV_CV
                                                                      ,REC_CARGA.SLD_PL_CV_MOV_FIM
                                                                      ,REC_CARGA.DCR_CTA_OBRIG_PARTIC
                                                                      ,REC_CARGA.SLD_CTA_OBRIG_PARTIC
                                                                      ,REC_CARGA.CTB_CTA_OBRIG_PARTIC
                                                                      ,REC_CARGA.RENT_CTA_OBRIG_PARTIC
                                                                      ,REC_CARGA.SLD_CTA_OBRIG_PARTIC_FIM
                                                                      ,REC_CARGA.DCR_CTA_NORM_PATROC
                                                                      ,REC_CARGA.SLD_CTA_NORM_PATROC
                                                                      ,REC_CARGA.CTB_CTA_NORM_PATROC
                                                                      ,REC_CARGA.RENT_NORM_PATROC
                                                                      ,REC_CARGA.SLD_NORM_PATROC_INIC
                                                                      ,REC_CARGA.DCR_CTA_ESPEC_PARTIC
                                                                      ,REC_CARGA.SLD_CTA_ESPEC_PARTIC
                                                                      ,REC_CARGA.CTB_CTA_ESPEC_PARTIC
                                                                      ,REC_CARGA.RENT_CTA_ESPEC_PARTIC
                                                                      ,REC_CARGA.SLD_CTA_ESPEC_PARTIC_INIC
                                                                      ,REC_CARGA.DCR_CTA_ESPEC_PATROC
                                                                      ,REC_CARGA.SLD_CTA_ESPEC_PATROC
                                                                      ,REC_CARGA.CTB_CTA_ESPEC_PATROC
                                                                      ,REC_CARGA.RENT_CTA_ESPEC_PATROC
                                                                      ,REC_CARGA.SLD_CTA_ESPEC_PATROC_INIC
                                                                      ,REC_CARGA.SLD_TOT_INIC
                                                                      ,REC_CARGA.CTB_TOT_INIC
                                                                      ,REC_CARGA.RENT_PERIODO
                                                                      ,REC_CARGA.SLD_TOT_FIM
                                                                      ,REC_CARGA.PRM_MES_PERIODO_CTB
                                                                      ,REC_CARGA.SEG_MES_PERIODO_CTB
                                                                      ,REC_CARGA.TER_MES_PERIODO_CTB
                                                                      ,REC_CARGA.DCR_TOT_CTB_BD
                                                                      ,REC_CARGA.VLR_TOT_CTB_BD_PRM_MES
                                                                      ,REC_CARGA.VLR_TOT_CTB_BD_SEG_MES
                                                                      ,REC_CARGA.VLR_TOT_CTB_BD_TER_MES
                                                                      ,REC_CARGA.VLR_TOT_CTB_BD_PERIODO
                                                                      ,REC_CARGA.DCR_TOT_CTB_CV
                                                                      ,REC_CARGA.VLR_TOT_CTB_CV_PRM_MES
                                                                      ,REC_CARGA.VLR_TOT_CTB_CV_SEG_MES
                                                                      ,REC_CARGA.VLR_TOT_CTB_CV_TER_MES
                                                                      ,REC_CARGA.VLR_TOT_CTB_CV_PERIODO
                                                                      ,REC_CARGA.DCR_TPO_CTB_VOL_PARTIC
                                                                      ,REC_CARGA.VLR_CTB_VOL_PARTIC_PRM_MES
                                                                      ,REC_CARGA.VLR_CTB_VOL_PARTIC_SEG_MES
                                                                      ,REC_CARGA.VLR_CTB_VOL_PARTIC_TER_MES
                                                                      ,REC_CARGA.VLR_CTB_VOL_PARTIC_PERIODO
                                                                      ,REC_CARGA.DCR_TPO_CTB_VOL_PATROC
                                                                      ,REC_CARGA.VLR_CTB_VOL_PATROC_PRM_MES
                                                                      ,REC_CARGA.VLR_CTB_VOL_PATROC_SEG_MES
                                                                      ,REC_CARGA.VLR_CTB_VOL_PATROC_TER_MES
                                                                      ,REC_CARGA.VLR_CTB_VOL_PATROC_PERIODO
                                                                      ,REC_CARGA.DCR_TPO_CTB_OBRIG_PARTIC
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PARTIC_PRM_MES
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PARTIC_SEG_MES
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PARTIC_TER_MES
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PARTIC_PERIODO
                                                                      ,REC_CARGA.DCR_TPO_CTB_OBRIG_PATROC
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PATROC_PRM_MES
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PATROC_SEG_MES
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PATROC_TER_MES
                                                                      ,REC_CARGA.VLR_CTB_OBRIG_PATROC_PERIODO
                                                                      ,REC_CARGA.DCR_TPO_CTB_ESPOR_PATROC
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PATROC_PRM_MES
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PATROC_SEG_MES
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PATROC_TER_MES
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PATROC_PERIODO
                                                                      ,REC_CARGA.DCR_TPO_CTB_ESPOR_PARTIC
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PARTIC_PRM_MES
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PARTIC_SEG_MES
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PARTIC_TER_MES
                                                                      ,REC_CARGA.VLR_CTB_ESPOR_PARTIC_PERIODO
                                                                      ,REC_CARGA.TOT_CTB_PRM_MES
                                                                      ,REC_CARGA.TOT_CTB_SEG_MES
                                                                      ,REC_CARGA.TOT_CTB_TER_MES
                                                                      ,REC_CARGA.TOT_CTB_EXTRATO
                                                                      ,REC_CARGA.PRM_MES_PERIODO_RENT
                                                                      ,REC_CARGA.SEG_MES_PERIODO_RENT
                                                                      ,REC_CARGA.TER_MES_PERIODO_RENT
                                                                      ,REC_CARGA.PCT_RENT_REAL_PRM_MES
                                                                      ,REC_CARGA.PCT_RENT_REAL_SEG_MES
                                                                      ,REC_CARGA.PCT_RENT_REAL_TER_MES
                                                                      ,REC_CARGA.PCT_RENT_REAL_TOT_MES
                                                                      ,REC_CARGA.PCT_RENT_LMTD_PRM_MES
                                                                      ,REC_CARGA.PCT_RENT_LMTD_SEG_MES
                                                                      ,REC_CARGA.PCT_RENT_LMTD_TER_MES
                                                                      ,REC_CARGA.PCT_RENT_LMTD_TOT_MES
                                                                      ,REC_CARGA.PCT_RENT_IGPDI_PRM_MES
                                                                      ,REC_CARGA.PCT_RENT_IGPDI_SEG_MES
                                                                      ,REC_CARGA.PCT_RENT_IGPDI_TER_MES
                                                                      ,REC_CARGA.PCT_RENT_IGPDI_TOT_MES
                                                                      ,REC_CARGA.PCT_RENT_URR_PRM_MES
                                                                      ,REC_CARGA.PCT_RENT_URR_SEG_MES
                                                                      ,REC_CARGA.PCT_RENT_URR_TER_MES
                                                                      ,REC_CARGA.PCT_RENT_URR_TOT_MES
                                                                      ,REC_CARGA.DTA_APOS_PROP
                                                                      ,REC_CARGA.DTA_APOS_INTE
                                                                      ,REC_CARGA.VLR_BENEF_PSAP_PROP
                                                                      ,REC_CARGA.VLR_BENEF_PSAP_INTE
                                                                      ,REC_CARGA.VLR_BENEF_BD_PROP
                                                                      ,REC_CARGA.VLR_BENEF_BD_INTE
                                                                      ,REC_CARGA.VLR_BENEF_CV_PROP
                                                                      ,REC_CARGA.VLR_BENEF_CV_INTE
                                                                      ,REC_CARGA.RENDA_ESTIM_PROP
                                                                      ,REC_CARGA.RENDA_ESTIM_INT
                                                                      ,REC_CARGA.VLR_RESERV_SALD_LQDA
                                                                      ,REC_CARGA.TXT_PRM_MENS
                                                                      ,REC_CARGA.TXT_SEG_MENS
                                                                      ,REC_CARGA.TXT_TER_MENS
                                                                      ,REC_CARGA.TXT_QUA_MENS
                                                                      ,REC_CARGA.IDADE_PROP_BSPS
                                                                      ,REC_CARGA.VLR_CTB_PROP_BSPS
                                                                      ,REC_CARGA.IDADE_INT_BSPS
                                                                      ,REC_CARGA.VLR_CTB_INT_BSPS
                                                                      ,REC_CARGA.IDADE_PROP_BD
                                                                      ,REC_CARGA.VLR_CTB_PROP_BD
                                                                      ,REC_CARGA.IDADE_INT_BD
                                                                      ,REC_CARGA.VLR_CTB_INT_BD
                                                                      ,REC_CARGA.IDADE_PROP_CV
                                                                      ,REC_CARGA.VLR_CTB_PROP_CV
                                                                      ,REC_CARGA.IDADE_INT_CV
                                                                      ,REC_CARGA.VLR_CTB_INT_CV
                                                                      ,REC_CARGA.DCR_COTA_INDEX_PLAN_1
                                                                      ,REC_CARGA.DCR_COTA_INDEX_PLAN_2
                                                                      ,REC_CARGA.DCR_CTA_APOS_INDIV_VOL_PARTIC
                                                                      ,REC_CARGA.SLD_INI_CTA_APO_INDI_VOL_PARTI
                                                                      ,REC_CARGA.VLR_TOT_CTB_APO_INDI_VOL_PARTI
                                                                      ,REC_CARGA.REN_TOT_CTB_APO_INDI_VOL_PARTI
                                                                      ,REC_CARGA.SLD_FIM_CTA_APO_INDI_VOL_PARTI
                                                                      ,REC_CARGA.DCR_CTA_APOS_INDIV_ESPO_PARTIC
                                                                      ,REC_CARGA.SLD_INI_CTA_APO_INDI_ESPOPARTI
                                                                      ,REC_CARGA.VLR_TOT_CTB_APO_INDI_ESPOPARTI
                                                                      ,REC_CARGA.REN_TOT_CTB_APO_INDI_ESPOPARTI
                                                                      ,REC_CARGA.SLD_FIM_CTA_APO_INDI_ESPOPARTI
                                                                      ,REC_CARGA.DCR_CTA_APOS_INDIV_VOL_PATROC
                                                                      ,REC_CARGA.SLD_INI_CTA_APO_INDI_VOL_PATRO
                                                                      ,REC_CARGA.VLR_TOT_CTB_APO_INDI_VOL_PATRO
                                                                      ,REC_CARGA.REN_TOT_CTB_APO_INDI_VOL_PATRO
                                                                      ,REC_CARGA.SLD_FIM_CTA_APO_INDI_VOL_PATRO
                                                                      ,REC_CARGA.DCR_CTA_APOS_INDIV_SUPL_PATROC
                                                                      ,REC_CARGA.SLD_INI_CTA_APO_INDI_SUPLPATRO
                                                                      ,REC_CARGA.VLR_TOT_CTB_APO_INDI_SUPLPATRO
                                                                      ,REC_CARGA.REN_TOT_CTB_APO_INDI_SUPLPATRO
                                                                      ,REC_CARGA.SLD_FIM_CTA_APO_INDI_SUPLPATRO
                                                                      ,REC_CARGA.DCR_PORT_TOTAL
                                                                      ,REC_CARGA.SLD_INIC_CTA_PORT_TOT
                                                                      ,REC_CARGA.VLR_TOT_CTB_PORT_TOT
                                                                      ,REC_CARGA.RENT_TOT_CTB_PORT_TOT
                                                                      ,REC_CARGA.SLD_FIM_CTA_PORT_TOT
                                                                      ,REC_CARGA.DCR_PORT_ABERTA
                                                                      ,REC_CARGA.SLD_INIC_CTA_PORT_ABERTA
                                                                      ,REC_CARGA.VLR_TOT_CTB_PORT_ABERTA
                                                                      ,REC_CARGA.RENT_TOT_CTB_PORT_ABERTA
                                                                      ,REC_CARGA.SLD_FIM_CTA_PORT_ABERTA
                                                                      ,REC_CARGA.DCR_PORT_FECHADA
                                                                      ,REC_CARGA.SLD_INIC_CTA_PORT_FECHADA
                                                                      ,REC_CARGA.VLR_TOT_CTB_PORT_FECHADA
                                                                      ,REC_CARGA.RENT_TOT_CTB_PORT_FECHADA
                                                                      ,REC_CARGA.SLD_FIM_CTA_PORT_FECHADA
                                                                      ,REC_CARGA.DCR_PORT_JOIA_ABERTA
                                                                      ,REC_CARGA.SLD_INIC_CTA_PORT_JOIA_ABERTA
                                                                      ,REC_CARGA.VLR_TOT_CTB_PORT_JOIA_ABERTA
                                                                      ,REC_CARGA.RENT_TOT_CTB_PORT_JOIA_ABERTA
                                                                      ,REC_CARGA.SLD_FIM_CTA_PORT_JOIA_ABERTA
                                                                      ,REC_CARGA.DCR_PORT_JOIA_FECHADA
                                                                      ,REC_CARGA.SLD_INIC_CTA_PORT_JOIA_FECHADA
                                                                      ,REC_CARGA.VLR_TOT_CTB_PORT_JOIA_FECHADA
                                                                      ,REC_CARGA.RENT_TOT_CTB_PORT_JOIA_FECHADA
                                                                      ,REC_CARGA.SLD_FIM_CTA_PORT_JOIA_FECHADA
                                                                      ,REC_CARGA.DCR_DISTR_FUND_PREV_PARTIC
                                                                      ,REC_CARGA.SLD_INI_DIST_FUND_PREV_PARTI
                                                                      ,REC_CARGA.VLR_TOT_DIST_FUND_PREV_PARTI
                                                                      ,REC_CARGA.REN_TOT_DIST_FUND_PREV_PARTI
                                                                      ,REC_CARGA.SLDFIM_CTA_DISTFUNDPREVPARTI
                                                                      ,REC_CARGA.DCR_DISTR_FUND_PREV_PATROC
                                                                      ,REC_CARGA.SLD_INI_DIST_FUND_PREV_PATRO
                                                                      ,REC_CARGA.VLR_TOT_DIST_FUND_PREV_PATRO
                                                                      ,REC_CARGA.REN_TOT_DIST_FUND_PREV_PATRO
                                                                      ,REC_CARGA.SLDFIM_CTA_DISTFUNDPREVPATRO
                                                                      ,REC_CARGA.DCR_PORT_FINAL
                                                                      ,REC_CARGA.SLD_INIC_CTA_PORT_FIM
                                                                      ,REC_CARGA.VLR_TOT_CTB_PORT_FIM
                                                                      ,REC_CARGA.RENT_TOT_CTB_PORT_FIM
                                                                      ,REC_CARGA.SLD_FIM_CTA_PORT_FIM
                                                                      ,REC_CARGA.DCR_SLD_PROJETADO
                                                                      ,REC_CARGA.VLR_SLD_PROJETADO
                                                                      ,REC_CARGA.VLR_SLD_ADICIONAL
                                                                      ,REC_CARGA.VLR_BENEF_ADICIONAL
                                                                      ,REC_CARGA.DTA_ULT_ATUAL
                                                                      ,REC_CARGA.VLR_CONTRIB_RISCO
                                                                      ,REC_CARGA.VLR_CONTRIB_PATRC
                                                                      ,REC_CARGA.VLR_CAPIT_SEGURADO
                                                                      ,REC_CARGA.VLR_CONTRIB_ADM
                                                                      ,REC_CARGA.VLR_CONTRIB_ADM_PATRC
                                                                      ,REC_CARGA.VLR_SIMUL_BENEF_PORCETAGEM
                                                                      ,REC_CARGA.DTA_ELEGIB_BENEF_PORCETAGEM
                                                                      ,REC_CARGA.IDADE_ELEGIB_PORCETAGEM
                                                                      ,REC_CARGA.DTA_EXAURIM_BENEF_PORCETAGEM
                                                                      ,REC_CARGA.VLR_SIMUL_BENEF_PRAZO
                                                                      ,REC_CARGA.DTA_ELEGIB_BENEF_PRAZO
                                                                      ,REC_CARGA.IDADE_ELEGIB_BENEF_PRAZO
                                                                      ,REC_CARGA.DTA_EXAURIM_BENEF_PRAZO
                                                                     );
                                                                COMMIT;*/
                                                                
            END IF;  
            
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
    
   
    END PRC_CARGA_ARQUIVO;
   
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
        FROM FC_PRE_TBL_CARGA_EXTRATO;   
        
      
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