CREATE OR REPLACE PACKAGE OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO IS
  -- VARIAVEIS
 /* G_HOST_NAME    VARCHAR2(64);
  
  G_MODULE       VARCHAR2(255) := '';
  G_OS_USER      VARCHAR2(255) := '';
  G_TERMINAL     VARCHAR2(255) := '';
  G_CURRENT_USER VARCHAR2(255) := '';
  G_IP_ADDRESS   VARCHAR2(255) := '';

  G_ARQ       UTL_FILE.FILE_TYPE;
  G_SEPARADOR CHAR(1)  := ';';
  G_TABLE VARCHAR2(50) := 'FC_PRE_TBL_BASE_EXTRAT_CTB';
  G_PATH VARCHAR2(255);
  G_NAME VARCHAR2(255) := 'GeracaoExtratoCorreio.txt';
  G_DIR VARCHAR2(50)   := 'DIR_WORK';
  G_OWNER VARCHAR2(50) := 'ATT';
  --
  G_DDL_CREATE_TABLE VARCHAR2(5000) := 'CREATE TABLE F02860.EXT_TB_';
  G_DDL_COLUMNN_TABLE CLOB := EMPTY_CLOB(); --VARCHAR2(25000) := '';
  G_SQL VARCHAR2(30000) := '';
  L_LEN NUMBER := 0;
  G_EXT_CONST VARCHAR2(1000) := ' ORGANIZATION EXTERNAL ( '        || CHR(13) ||
                                ' TYPE ORACLE_LOADER '             || CHR(13) ||
                                ' DEFAULT DIRECTORY '              || G_DIR   || CHR(13) ||
                                ' ACCESS PARAMETERS ( '            || CHR(13) ||
                                ' RECORDS DELIMITED BY NEWLINE '   || CHR(13) ||
                                ' FIELDS TERMINATED BY '''''       || G_SEPARADOR || '''''' || CHR(13) ||
                                ' MISSING FIELD VALUES ARE NULL) ' || CHR(13) ||
                                ' LOCATION (<FILENAME>) '          || CHR(13) ||
                                ')'                                || CHR(13) ||
                                ' PARALLEL 5 '                     || CHR(13) ||
                                ' REJECT LIMIT UNLIMITED';

    FUNCTION FN_CARGA_ARQUIVO RETURN BOOLEAN;*/

    PROCEDURE PROC_EXT_PREV_TIETE(P_COD_EMPRESA   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE,
                                  P_DCR_PLANO     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE,
                                  P_DTA_MOV       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE
                                  );


     PROCEDURE PRE_PRC_EXT_PREV_ELETROPAULO(PCOD_EMPRESA ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE,
                                            PDCR_PLANO   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE,
                                            PDTA_MOV     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL
                                            );

   --PROCEDURE PRE_PRC_EXTRATOATUALIZA;
   
  PROCEDURE PRE_INICIA_PROCESSAMENTO( P_PRC_PROCESSO NUMBER DEFAULT NULL
                                     ,P_PRC_DATA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL);

   E_NOT_EXIST           EXCEPTION;
   E_ERROR_CARGA_ARQUIVO EXCEPTION;

 C_DESC_PLANO_ELETROPAULO VARCHAR2(1000) := 'PSAP/ELETROPAULO';
END PKG_EXT_PREVIDENCIARIO;
/
