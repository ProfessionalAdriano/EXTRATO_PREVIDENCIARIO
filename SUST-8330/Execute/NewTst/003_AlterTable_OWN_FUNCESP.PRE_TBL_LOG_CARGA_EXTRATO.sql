----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- SISTEMA.....: AMADEUS CAPITALIZACAO                                                                                
-- DESCRICAO...: SUST-8330 - Correção da rotina do Extrato Previdenciário
-- ANALISTA....: ADRIANO LIMA
-- DATA CRIACAO: 20/10/2021
-- OBJETO......: OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
SET ECHO ON
SET TIME ON
SET TIMING ON
SET SQLBL ON
SET SERVEROUTPUT ON SIZE UNLIMITED
SET DEFINE OFF
SHOW USER
SELECT * FROM GLOBAL_NAME;
SELECT INSTANCE_NAME, HOST_NAME FROM V$INSTANCE;
SELECT TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') DATA FROM DUAL;

ALTER TABLE OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO ADD DCR_PLANO VARCHAR2(40);
