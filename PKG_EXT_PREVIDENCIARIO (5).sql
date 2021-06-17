CREATE OR REPLACE PACKAGE OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO IS

 -- VARIAVEIS
  G_HOST_NAME    VARCHAR2(64);
  
  G_TPO_DADO      ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.TPO_DADO%TYPE;
  G_COD_EMPRS     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE;
  G_DTA_FIM_EXTR  ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE;
  G_DTA_EMISS     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_EMISS%TYPE;  
  G_CONT_TEMP     NUMBER := 0;
  G_COUNT_LOG     NUMBER := 0;
  G_CKECK         CHAR(1):= '';
  G_DCR_PLANO     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE;
  G_OBS           VARCHAR2(100) := 'EXTRATO PREVIDENCIARIO: ';
  G_MODULE        VARCHAR2(255) := '';
  G_OS_USER       VARCHAR2(255) := '';
  G_TERMINAL      VARCHAR2(255) := '';
  G_CURRENT_USER  VARCHAR2(255) := '';
  G_IP_ADDRESS    VARCHAR2(255) := '';
  

  G_ARQ          UTL_FILE.FILE_TYPE;
  G_DIR          VARCHAR2(50)          := '/dados/oracle/NEWDEV/work';
  G_READ         CHAR(1)               := 'R';
  G_SIZE         NUMBER                := 32767;


    FUNCTION FUN_CARGA_STAGE (P_CALCULO NUMBER) RETURN VARCHAR2;
           
    PROCEDURE PRC_CARGA_ARQUIVO (P_NAME_ARQ VARCHAR2 DEFAULT NULL);   
        
    FUNCTION FN_TRATA_ARQUIVO RETURN BOOLEAN;       
    
    PROCEDURE PROC_EXT_PREV_TIETE(  P_COD_EMPRESA   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                                   ,P_DCR_PLANO     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                                   ,P_DTA_MOV       ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL);


    PROCEDURE PROC_EXT_PREV_ELETROPAULO(  PCOD_EMPRESA ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                                         ,PDCR_PLANO   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                                         ,PDTA_MOV     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL);

   
    PROCEDURE PRE_INICIA_PROCESSAMENTO( P_PRC_PROCESSO NUMBER DEFAULT NULL
                                        ,P_PRC_DATA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL 
                                        ,P_NAME_ARQ     VARCHAR2 DEFAULT NULL);

 
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
    
    FUNCTION FUN_CARGA_STAGE (P_CALCULO NUMBER)
    RETURN VARCHAR2 IS
        
    VCAMPO VARCHAR2(2000);    
      
    BEGIN
    
       BEGIN    
          SELECT NVL(DADO, ' ')
            INTO VCAMPO
            FROM OWN_FUNCESP.FC_CARGA_EXTRATO
           WHERE INDX = P_CALCULO;  
           
        EXCEPTION WHEN NO_DATA_FOUND THEN
          VCAMPO := ' ';
        END;        
        RETURN VCAMPO;                        
     
        EXCEPTION
          WHEN OTHERS THEN
           DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
           DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    
    END FUN_CARGA_STAGE;
    
    
    
    PROCEDURE PRC_CARGA_ARQUIVO ( P_NAME_ARQ VARCHAR2 DEFAULT NULL )
    IS
    
       L_REGISTRO      LONG;
       SSQL            CLOB; 
       SSQL2           LONG;       
                            
       -- VARIABLE TYPE TABLE:
       REC_CARGA       OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO%ROWTYPE;
          
   
    BEGIN       
    
          DELETE FROM OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO
          COMMIT;        
         
          G_ARQ := UTL_FILE.FOPEN(G_DIR,P_NAME_ARQ,G_READ,G_SIZE);          
                                   
          LOOP                
                    
            UTL_FILE.GET_LINE(G_ARQ, L_REGISTRO);
            
          
            IF SUBSTR(L_REGISTRO, 1,1 ) = '1' THEN
            --DBMS_OUTPUT.PUT_LINE(L_REGISTRO);  
                        
            DELETE FROM OWN_FUNCESP.FC_CARGA_EXTRATO;
            COMMIT;                
            
            
            --L_REGISTRO := replace(replace(replace (replace(L_REGISTRO, '''', ' '), ';;;' , '; ; ; '), ';;','; ;'), '  ', '') ;
            L_REGISTRO := replace (L_REGISTRO, ';' , '; ') ;
            /*if length (L_REGISTRO) >4000 then
              dbms_output.put_line (L_REGISTRO);
            end if;
            
            insert into OWN_FUNCESP.FC_CARGA_EXTRATO (string)
            values (l_registro);
            commit; */
            
            -- NOVO
                
            INSERT INTO OWN_FUNCESP.FC_CARGA_EXTRATO (DADO, INDX) 
            with temp as
            ( select L_REGISTRO DADOS from dual) 
               
              select distinct 
                  trim(regexp_substr(t.DADOS, '[^;]+', 1, levels.column_value))  as DADOS, levels.column_value Nivel 
              from temp t, 
                table(cast(multiset(select level from dual connect by  level <= length (regexp_replace(t.DADOS, '[^;]+'))  + 1) as sys.OdciNumberList)) levels;
             COMMIT;  
              
                                                                                                    
            REC_CARGA.TPO_DADO								              := FUN_CARGA_STAGE(1);
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


            DELETE FROM OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO
            WHERE  TPO_DADO         = REC_CARGA.TPO_DADO
            AND    COD_EMPRS        = REC_CARGA.COD_EMPRS
            AND    NUM_RGTRO_EMPRG  = REC_CARGA.NUM_RGTRO_EMPRG
            AND    DTA_FIM_EXTR     = REC_CARGA.DTA_FIM_EXTR
            AND    DTA_EMISS        = REC_CARGA.DTA_EMISS
            AND    DCR_PLANO        = REC_CARGA.DCR_PLANO;
            COMMIT;
         
            INSERT INTO OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO VALUES (  REC_CARGA.TPO_DADO
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
                                                                COMMIT;
                                                                
            END IF;  
            
          END LOOP;        
          
          UTL_FILE.FCLOSE(G_ARQ); 
      EXCEPTION
        WHEN UTL_FILE.INVALID_PATH THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Diretório Inválido');
        WHEN UTL_FILE.INVALID_OPERATION THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Operação invalida no arquivo'); 
        WHEN UTL_FILE.WRITE_ERROR THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Erro de gravação no arquivo'); 
        WHEN UTL_FILE.INVALID_MODE THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Modo de acesso inválido');
        WHEN No_data_found THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('Arquivo: '||P_NAME_ARQ);
        WHEN OTHERS THEN
            UTL_FILE.FCLOSE(G_ARQ);
            DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
            DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
    
   
    END PRC_CARGA_ARQUIVO;
   
   
    FUNCTION FN_TRATA_ARQUIVO
      RETURN BOOLEAN
                
    IS
      L_TAB_STAGE VARCHAR2(100):= 'TRUNCATE TABLE'||' '|| 'OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO';
      V_COUNT NUMBER           :=0;
      R_VALIDA BOOLEAN;
      
      CURSOR C_TRATA_DADOS IS
               SELECT DECODE(TO_NUMBER(TPO_DADO),1,2)                                                                     AS TPO_DADO       
                  ,TO_NUMBER(COD_EMPRS)                                                                                   AS COD_EMPRS         
                  ,NUM_RGTRO_EMPRG                                                                                        AS NUM_RGTRO_EMPRG  
                  ,NOM_EMPRG                                                                                              AS NOM_EMPRG                                   
                  ,CASE
                      WHEN UPPER(SUBSTR(DTA_EMISS,4,3)) = 'JAN' THEN
                           TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')                                             
                                                   
                      WHEN UPPER(SUBSTR(DTA_EMISS,3,3)) = 'JAN' THEN
                           TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                           
                      WHEN SUBSTR(DTA_EMISS, 4,2) = '01'  THEN                                                
                           DTA_EMISS     
                       
                      WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'FEV','FEB'),4,3) = 'FEB' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'FEV', 'FEB')),'DD/MM/RRRR')                                                     
                           
                      WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'FEV','FEB'),3,3) = 'FEB' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'FEV', 'FEB')),'DD/MM/RRRR')
                      
                      WHEN SUBSTR(DTA_EMISS, 4,2) = '02'  THEN                                                
                           DTA_EMISS                           
                       
                      WHEN UPPER(SUBSTR(DTA_EMISS,4,3)) = 'MAR' THEN
                           TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                           
                      WHEN UPPER(SUBSTR(DTA_EMISS,3,3)) = 'MAR' THEN
                           TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                      
                      WHEN SUBSTR(DTA_EMISS, 4,2) = '03'  THEN                                                
                           DTA_EMISS     
                       
                      WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'ABR','APR'),4,3) = 'APR' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'ABR', 'APR')),'DD/MM/RRRR')
                           
                      WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'ABR','APR'),3,3) = 'APR' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'ABR', 'APR')),'DD/MM/RRRR') 
                      
                      WHEN SUBSTR(DTA_EMISS, 4,2) = '04'  THEN                                                
                           DTA_EMISS
                        
                       WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'MAI','MAY'),4,3) = 'MAY' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'MAI', 'MAY')),'DD/MM/RRRR')
                            
                       WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'MAI','MAY'),3,3) = 'MAY' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'MAI', 'MAY')),'DD/MM/RRRR')
                            
                       WHEN SUBSTR(DTA_EMISS, 4,2) = '05'  THEN                                                
                           DTA_EMISS     
                       
                       WHEN UPPER(SUBSTR(DTA_EMISS,4,3)) = 'JUN' THEN
                            TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR') 
                            
                       WHEN UPPER(SUBSTR(DTA_EMISS,3,3)) = 'JUN' THEN
                            TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                       
                       WHEN SUBSTR(DTA_EMISS, 4,2) = '06'  THEN                                                
                           DTA_EMISS
                       
                       WHEN UPPER(SUBSTR(DTA_EMISS,4,3)) = 'JUL' THEN
                            TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                        
                       WHEN UPPER(SUBSTR(DTA_EMISS,3,3)) = 'JUL' THEN
                            TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                       
                       WHEN SUBSTR(DTA_EMISS, 4,2) = '07'  THEN                                                
                           DTA_EMISS
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'AGO','AUG'),4,3) = 'AUG' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'AGO', 'AUG')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'AGO','AUG'),3,3) = 'AUG' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'AGO', 'AUG')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(DTA_EMISS, 4,2) = '08'  THEN                                                
                           DTA_EMISS   
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'SET','SEP'),4,3) = 'SEP' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'SET', 'SEP')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'SET','SEP'),3,3) = 'SEP' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'SET', 'SEP')),'DD/MM/RRRR')
                        
                        WHEN SUBSTR(DTA_EMISS, 4,2) = '09'  THEN                                                
                           DTA_EMISS
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'OUT','OCT'),4,3) = 'OCT' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'OUT', 'OCT')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'OUT','OCT'),3,3) = 'OCT' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'OUT', 'OCT')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(DTA_EMISS, 4,2) = '10'  THEN                                                
                           DTA_EMISS
                        
                        WHEN UPPER(SUBSTR(DTA_EMISS,4,3)) = 'NOV' THEN
                            TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')  
                            
                        WHEN UPPER(SUBSTR(DTA_EMISS,3,3)) = 'NOV' THEN
                            TO_CHAR(TO_DATE(DTA_EMISS),'DD/MM/RRRR')
                            
                        WHEN SUBSTR(DTA_EMISS, 4,2) = '11'  THEN                                                
                           DTA_EMISS 
                            
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'DEZ','DEC'),4,3) = 'DEC' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'DEZ', 'DEC')),'DD/MM/RRRR')
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_EMISS),'DEZ','DEC'),3,3) = 'DEC' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_EMISS),'DEZ', 'DEC')),'DD/MM/RRRR')
                            
                        WHEN SUBSTR(DTA_EMISS, 4,2) = '12'  THEN                                                
                           DTA_EMISS
                            
                   END                                                                                                    AS DTA_EMISS                  
                  ,NUM_FOLHA                                                                                              AS NUM_FOLHA                                                                          
                  ,DCR_PLANO                                                                                              AS DCR_PLANO
                  ,PER_INIC_EXTR                                                                                          AS PER_INIC_EXTR
                  ,PER_FIM_EXTR                                                                                           AS PER_FIM_EXTR                  
                  ,CASE
                      WHEN UPPER(SUBSTR(DTA_INIC_EXTR,4,3)) = 'JAN' THEN
                           TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                           
                      WHEN UPPER(SUBSTR(DTA_INIC_EXTR,3,3)) = 'JAN' THEN
                           TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                       
                      WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'FEV','FEB'),4,3) = 'FEB' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'FEV', 'FEB')),'DD/MM/RRRR')
                           
                      WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'FEV','FEB'),3,3) = 'FEB' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'FEV', 'FEB')),'DD/MM/RRRR')
                       
                      WHEN UPPER(SUBSTR(DTA_INIC_EXTR,4,3)) = 'MAR' THEN
                           TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                           
                      WHEN UPPER(SUBSTR(DTA_INIC_EXTR,3,3)) = 'MAR' THEN
                           TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                       
                      WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'ABR','APR'),4,3) = 'APR' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'ABR', 'APR')),'DD/MM/RRRR') 
                           
                      WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'ABR','APR'),3,3) = 'APR' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'ABR', 'APR')),'DD/MM/RRRR') 
                        
                       WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'MAI','MAY'),4,3) = 'MAY' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'MAI', 'MAY')),'DD/MM/RRRR')
                            
                      WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'MAI','MAY'),3,3) = 'MAY' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'MAI', 'MAY')),'DD/MM/RRRR')
                       
                       WHEN UPPER(SUBSTR(DTA_INIC_EXTR,4,3)) = 'JUN' THEN
                            TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR') 
                            
                      WHEN UPPER(SUBSTR(DTA_INIC_EXTR,3,3)) = 'JUN' THEN
                            TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                       
                       WHEN UPPER(SUBSTR(DTA_INIC_EXTR,4,3)) = 'JUL' THEN
                            TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                            
                       WHEN UPPER(SUBSTR(DTA_INIC_EXTR,3,3)) = 'JUL' THEN
                            TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'AGO','AUG'),4,3) = 'AUG' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'AGO', 'AUG')),'DD/MM/RRRR')   
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'AGO','AUG'),3,3) = 'AUG' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'AGO', 'AUG')),'DD/MM/RRRR')   
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'SET','SEP'),4,3) = 'SEP' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'SET', 'SEP')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'SET','SEP'),3,3) = 'SEP' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'SET', 'SEP')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'OUT','OCT'),4,3) = 'OCT' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'OUT', 'OCT')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'OUT','OCT'),3,3) = 'OCT' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'OUT', 'OCT')),'DD/MM/RRRR')
                        
                        WHEN UPPER(SUBSTR(DTA_INIC_EXTR,4,3)) = 'NOV' THEN
                             TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')  
                             
                        WHEN UPPER(SUBSTR(DTA_INIC_EXTR,3,3)) = 'NOV' THEN
                             TO_CHAR(TO_DATE(DTA_INIC_EXTR),'DD/MM/RRRR')
                            
                        WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'DEZ','DEC'),4,3) = 'DEC' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'DEZ', 'DEC')),'DD/MM/RRRR')
                             
                       WHEN SUBSTR(REPLACE(UPPER(DTA_INIC_EXTR),'DEZ','DEC'),3,3) = 'DEC' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_INIC_EXTR),'DEZ', 'DEC')),'DD/MM/RRRR')
                            
                    END                                                                                                   AS DTA_INIC_EXTR                   
                    --
                    --                   
                    ,CASE
                      WHEN UPPER(SUBSTR(DTA_FIM_EXTR,4,3)) = 'JAN' THEN
                           TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')
                      
                      WHEN UPPER(SUBSTR(DTA_FIM_EXTR,3,3)) = 'JAN' THEN
                           TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')
                       
                      WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'FEV','FEB'),4,3) = 'FEB' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'FEV', 'FEB')),'DD/MM/RRRR')
                           
                      WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'FEV','FEB'),3,3) = 'FEB' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'FEV', 'FEB')),'DD/MM/RRRR')
                       
                      WHEN UPPER(SUBSTR(DTA_FIM_EXTR,4,3)) = 'MAR' THEN
                           TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')
                           
                      WHEN UPPER(SUBSTR(DTA_FIM_EXTR,3,3)) = 'MAR' THEN
                           TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')
                       
                      WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'ABR','APR'),4,3) = 'APR' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'ABR', 'APR')),'DD/MM/RRRR') 
                           
                      WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'ABR','APR'),3,3) = 'APR' THEN
                           TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'ABR', 'APR')),'DD/MM/RRRR') 
                        
                       WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'MAI','MAY'),4,3) = 'MAY' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'MAI', 'MAY')),'DD/MM/RRRR')
                            
                       WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'MAI','MAY'),3,3) = 'MAY' THEN
                            TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'MAI', 'MAY')),'DD/MM/RRRR')
                       
                       WHEN UPPER(SUBSTR(DTA_FIM_EXTR,4,3)) = 'JUN' THEN
                            TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR') 
                            
                       WHEN UPPER(SUBSTR(DTA_FIM_EXTR,3,3)) = 'JUN' THEN
                            TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR') 
                       
                       WHEN UPPER(SUBSTR(DTA_FIM_EXTR,4,3)) = 'JUL' THEN
                            TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')
                            
                            
                       WHEN UPPER(SUBSTR(DTA_FIM_EXTR,3,3)) = 'JUL' THEN
                            TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'AGO','AUG'),4,3) = 'AUG' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'AGO', 'AUG')),'DD/MM/RRRR') 
                             
                       WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'AGO','AUG'),3,3) = 'AUG' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'AGO', 'AUG')),'DD/MM/RRRR')     
                        
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'SET','SEP'),4,3) = 'SEP' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'SET', 'SEP')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'SET','SEP'),3,3) = 'SEP' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'SET', 'SEP')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'OUT','OCT'),4,3) = 'OCT' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'OUT', 'OCT')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'OUT','OCT'),3,3) = 'OCT' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'OUT', 'OCT')),'DD/MM/RRRR')
                        
                        WHEN UPPER(SUBSTR(DTA_FIM_EXTR,4,3)) = 'NOV' THEN
                             TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')  
                             
                        WHEN UPPER(SUBSTR(DTA_FIM_EXTR,3,3)) = 'NOV' THEN
                             TO_CHAR(TO_DATE(DTA_FIM_EXTR),'DD/MM/RRRR')  
                            
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'DEZ','DEC'),4,3) = 'DEC' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'DEZ', 'DEC')),'DD/MM/RRRR')
                             
                        WHEN SUBSTR(REPLACE(UPPER(DTA_FIM_EXTR),'DEZ','DEC'),3,3) = 'DEC' THEN
                             TO_CHAR(TO_DATE(REPLACE(UPPER(DTA_FIM_EXTR),'DEZ', 'DEC')),'DD/MM/RRRR')
                                                                                                                    
                    END                                                                                                   AS DTA_FIM_EXTR                                         
                    --
                    -- 
                  ,NVL(TRIM(DCR_SLD_MOV_SALDADO),' ')                                                                     AS DCR_SLD_MOV_SALDADO                                     
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_PL_SALDADO_MOV_INIC,' ',''),'0'),'.',''),',','.'))           AS SLD_PL_SALDADO_MOV_INIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_PL_SALDADO_MOV,' ',''),'0'),'.',''),',','.'))                AS CTB_PL_SALDADO_MOV                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_PL_SALDADO_MOV,' ',''),'0'),'.',''),',','.'))               AS RENT_PL_SALDADO_MOV                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_PL_SALDADO_MOV_FIM,' ',''),'0'),'.',''),',','.'))            AS SLD_PL_SALDADO_MOV_FIM                  
                  ,TRIM(DCR_SLD_MOV_BD)                                                                                   AS DCR_SLD_MOV_BD                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_PL_BD_INIC,' ',''),'0'),'.',''),',','.'))                    AS SLD_PL_BD_INIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_PL_MOV_BD,' ',''),'0'),'.',''),',','.'))                     AS CTB_PL_MOV_BD
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_PL_MOV_BD,' ',''),'0'),'.',''),',','.'))                    AS RENT_PL_MOV_BD
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_PL_BD_MOV_FIM,' ',''),'0'),'.',''),',','.'))                 AS SLD_PL_BD_MOV_FIM
                  ,TRIM(DCR_SLD_MOV_CV)                                                                                   AS DCR_SLD_MOV_CV                 
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_PL_CV_MOV_INIC,' ',''),'0'),'.',''),',','.'))                AS SLD_PL_CV_MOV_INIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_PL_MOV_CV,' ',''),'0'),'.',''),',','.'))                     AS CTB_PL_MOV_CV
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_PL_MOV_CV,' ',''),'0'),'.',''),',','.'))                    AS RENT_PL_MOV_CV
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_PL_CV_MOV_FIM,' ',''),'0'),'.',''),',','.'))                 AS SLD_PL_CV_MOV_FIM
                  ,TRIM(DCR_CTA_OBRIG_PARTIC)                                                                             AS DCR_CTA_OBRIG_PARTIC                                    
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_OBRIG_PARTIC,' ',''),'0'),'.',''),',','.'))              AS SLD_CTA_OBRIG_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_CTA_OBRIG_PARTIC,' ',''),'0'),'.',''),',','.'))              AS CTB_CTA_OBRIG_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_CTA_OBRIG_PARTIC,' ',''),'0'),'.',''),',','.'))             AS RENT_CTA_OBRIG_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_OBRIG_PARTIC_FIM,' ',''),'0'),'.',''),',','.'))          AS SLD_CTA_OBRIG_PARTIC_FIM
                  ,TRIM(DCR_CTA_NORM_PATROC)                                                                              AS DCR_CTA_NORM_PATROC                                                      
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_NORM_PATROC,' ',''),'0'),'.',''),',','.'))               AS SLD_CTA_NORM_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_CTA_NORM_PATROC,' ',''),'0'),'.',''),',','.'))               AS CTB_CTA_NORM_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_NORM_PATROC,' ',''),'0'),'.',''),',','.'))                  AS RENT_NORM_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_NORM_PATROC_INIC,' ',''),'0'),'.',''),',','.'))              AS SLD_NORM_PATROC_INIC
                  ,TRIM(DCR_CTA_ESPEC_PARTIC)                                                                             AS DCR_CTA_ESPEC_PARTIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_ESPEC_PARTIC,' ',''),'0'),'.',''),',','.'))              AS SLD_CTA_ESPEC_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_CTA_ESPEC_PARTIC,' ',''),'0'),'.',''),',','.'))              AS CTB_CTA_ESPEC_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_CTA_ESPEC_PARTIC,' ',''),'0'),'.',''),',','.'))             AS RENT_CTA_ESPEC_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_ESPEC_PARTIC_INIC,' ',''),'0'),'.',''),',','.'))         AS SLD_CTA_ESPEC_PARTIC_INIC
                  ,TRIM(DCR_CTA_ESPEC_PATROC)                                                                             AS DCR_CTA_ESPEC_PATROC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_ESPEC_PATROC,' ',''),'0'),'.',''),',','.'))              AS SLD_CTA_ESPEC_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_CTA_ESPEC_PATROC,' ',''),'0'),'.',''),',','.'))              AS CTB_CTA_ESPEC_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_CTA_ESPEC_PATROC,' ',''),'0'),'.',''),',','.'))             AS RENT_CTA_ESPEC_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_CTA_ESPEC_PATROC_INIC,' ',''),'0'),'.',''),',','.'))         AS SLD_CTA_ESPEC_PATROC_INIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_TOT_INIC,' ',''),'0'),'.',''),',','.'))                      AS SLD_TOT_INIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(CTB_TOT_INIC,' ',''),'0'),'.',''),',','.'))                      AS CTB_TOT_INIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_PERIODO,' ',''),'0'),'.',''),',','.'))                      AS RENT_PERIODO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_TOT_FIM,' ',''),'0'),'.',''),',','.'))                       AS SLD_TOT_FIM
                  ,TRIM(PRM_MES_PERIODO_CTB)                                                                              AS PRM_MES_PERIODO_CTB
                  ,TRIM(SEG_MES_PERIODO_CTB)                                                                              AS SEG_MES_PERIODO_CTB
                  ,TRIM(TER_MES_PERIODO_CTB)                                                                              AS TER_MES_PERIODO_CTB
                  ,TRIM(DCR_TOT_CTB_BD)                                                                                   AS DCR_TOT_CTB_BD                                    
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_BD_PRM_MES,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_BD_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_BD_SEG_MES,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_BD_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_BD_TER_MES,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_BD_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_BD_PERIODO,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_BD_PERIODO
                  ,TRIM(DCR_TOT_CTB_CV)                                                                                   AS DCR_TOT_CTB_CV                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_CV_PRM_MES,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_CV_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_CV_SEG_MES,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_CV_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_CV_TER_MES,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_CV_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_CV_PERIODO,' ',''),'0'),'.',''),',','.'))            AS VLR_TOT_CTB_CV_PERIODO                  
                  ,TRIM(DCR_TPO_CTB_VOL_PARTIC)                                                                           AS DCR_TPO_CTB_VOL_PARTIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PARTIC_PRM_MES,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PARTIC_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PARTIC_SEG_MES,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PARTIC_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PARTIC_TER_MES,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PARTIC_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PARTIC_PERIODO,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PARTIC_PERIODO
                  ,TRIM(DCR_TPO_CTB_VOL_PATROC)                                                                           AS DCR_TPO_CTB_VOL_PATROC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PATROC_PRM_MES,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PATROC_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PATROC_SEG_MES,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PATROC_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PATROC_TER_MES,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PATROC_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_VOL_PATROC_PERIODO,' ',''),'0'),'.',''),',','.'))        AS VLR_CTB_VOL_PATROC_PERIODO
                  ,TRIM(DCR_TPO_CTB_OBRIG_PARTIC)                                                                         AS DCR_TPO_CTB_OBRIG_PARTIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PARTIC_PRM_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PARTIC_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PARTIC_SEG_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PARTIC_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PARTIC_TER_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PARTIC_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PARTIC_PERIODO,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PARTIC_PERIODO
                  ,TRIM(DCR_TPO_CTB_OBRIG_PATROC)                                                                         AS DCR_TPO_CTB_OBRIG_PATROC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PATROC_PRM_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PATROC_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PATROC_SEG_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PATROC_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PATROC_TER_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PATROC_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_OBRIG_PATROC_PERIODO,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_OBRIG_PATROC_PERIODO
                  ,TRIM(DCR_TPO_CTB_ESPOR_PATROC)                                                                         AS DCR_TPO_CTB_ESPOR_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PATROC_PRM_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PATROC_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PATROC_SEG_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PATROC_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PATROC_TER_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PATROC_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PATROC_PERIODO,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PATROC_PERIODO
                  ,TRIM(DCR_TPO_CTB_ESPOR_PARTIC)                                                                         AS DCR_TPO_CTB_ESPOR_PARTIC                                    
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PARTIC_PRM_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PARTIC_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PARTIC_SEG_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PARTIC_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PARTIC_TER_MES,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PARTIC_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_ESPOR_PARTIC_PERIODO,' ',''),'0'),'.',''),',','.'))      AS VLR_CTB_ESPOR_PARTIC_PERIODO                                    
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(TOT_CTB_PRM_MES,' ',''),'0'),'.',''),',','.'))                   AS TOT_CTB_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(TOT_CTB_SEG_MES,' ',''),'0'),'.',''),',','.'))                   AS TOT_CTB_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(TOT_CTB_TER_MES,' ',''),'0'),'.',''),',','.'))                   AS TOT_CTB_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(TOT_CTB_EXTRATO,' ',''),'0'),'.',''),',','.'))                   AS TOT_CTB_EXTRATO
                  ,TRIM(PRM_MES_PERIODO_RENT)                                                                             AS PRM_MES_PERIODO_RENT
                  ,TRIM(SEG_MES_PERIODO_RENT)                                                                             AS SEG_MES_PERIODO_RENT
                  ,TRIM(TER_MES_PERIODO_RENT)                                                                             AS TER_MES_PERIODO_RENT                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_REAL_PRM_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_REAL_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_REAL_SEG_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_REAL_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_REAL_TER_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_REAL_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_REAL_TOT_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_REAL_TOT_MES                                                                                                                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_LMTD_PRM_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_LMTD_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_LMTD_SEG_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_LMTD_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_LMTD_TER_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_LMTD_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_LMTD_TOT_MES,' ',''),'0'),'.',''),',','.'))             AS PCT_RENT_LMTD_TOT_MES                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_IGPDI_PRM_MES,' ',''),'0'),'.',''),',','.'))            AS PCT_RENT_IGPDI_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_IGPDI_SEG_MES,' ',''),'0'),'.',''),',','.'))            AS PCT_RENT_IGPDI_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_IGPDI_TER_MES,' ',''),'0'),'.',''),',','.'))            AS PCT_RENT_IGPDI_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_IGPDI_TOT_MES,' ',''),'0'),'.',''),',','.'))            AS PCT_RENT_IGPDI_TOT_MES                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_URR_PRM_MES,' ',''),'0'),'.',''),',','.'))              AS PCT_RENT_URR_PRM_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_URR_SEG_MES,' ',''),'0'),'.',''),',','.'))              AS PCT_RENT_URR_SEG_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_URR_TER_MES,' ',''),'0'),'.',''),',','.'))              AS PCT_RENT_URR_TER_MES
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(PCT_RENT_URR_TOT_MES,' ',''),'0'),'.',''),',','.'))              AS PCT_RENT_URR_TOT_MES
                  ,TO_DATE(DTA_APOS_PROP,'DD/MM/RRRR')                                                                    AS DTA_APOS_PROP
                  ,TO_DATE(DTA_APOS_INTE,'DD/MM/RRRR')                                                                    AS DTA_APOS_INTE                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_BENEF_PSAP_PROP,' ',''),'0'),'.',''),',','.'))               AS VLR_BENEF_PSAP_PROP
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_BENEF_PSAP_INTE,' ',''),'0'),'.',''),',','.'))               AS VLR_BENEF_PSAP_INTE
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_BENEF_BD_PROP,' ',''),'0'),'.',''),',','.'))                 AS VLR_BENEF_BD_PROP
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_BENEF_BD_INTE,' ',''),'0'),'.',''),',','.'))                 AS VLR_BENEF_BD_INTE
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_BENEF_CV_PROP,' ',''),'0'),'.',''),',','.'))                 AS VLR_BENEF_CV_PROP
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_BENEF_CV_INTE,' ',''),'0'),'.',''),',','.'))                 AS VLR_BENEF_CV_INTE
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENDA_ESTIM_PROP,' ',''),'0'),'.',''),',','.'))                  AS RENDA_ESTIM_PROP
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENDA_ESTIM_INT,' ',''),'0'),'.',''),',','.'))                   AS RENDA_ESTIM_INT
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_RESERV_SALD_LQDA,' ',''),'0'),'.',''),',','.'))              AS VLR_RESERV_SALD_LQDA
                  ,TRIM(TXT_PRM_MENS)                                                                                     AS TXT_PRM_MENS
                  ,TRIM(TXT_SEG_MENS)                                                                                     AS TXT_SEG_MENS
                  ,TRIM(TXT_TER_MENS)                                                                                     AS TXT_TER_MENS
                  ,TRIM(TXT_QUA_MENS)                                                                                     AS TXT_QUA_MENS
                  ,IDADE_PROP_BSPS                                                                                        AS IDADE_PROP_BSPS
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_PROP_BSPS,' ',''),'0'),'.',''),',','.'))                 AS VLR_CTB_PROP_BSPS
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(IDADE_INT_BSPS,' ',''),'0'),'.',''),',','.'))                    AS IDADE_INT_BSPS
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_INT_BSPS,' ',''),'0'),'.',''),',','.'))                  AS VLR_CTB_INT_BSPS
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(IDADE_PROP_BD,' ',''),'0'),'.',''),',','.'))                     AS IDADE_PROP_BD                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_PROP_BD,' ',''),'0'),'.',''),',','.'))                   AS VLR_CTB_PROP_BD
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(IDADE_INT_BD,' ',''),'0'),'.',''),',','.'))                      AS IDADE_INT_BD
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_INT_BD,' ',''),'0'),'.',''),',','.'))                    AS VLR_CTB_INT_BD
                  ,IDADE_PROP_CV                                                                                          AS IDADE_PROP_CV                    
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_PROP_CV,' ',''),'0'),'.',''),',','.'))                   AS VLR_CTB_PROP_CV
                  ,IDADE_INT_CV                                                                                           AS IDADE_INT_CV
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_CTB_INT_CV,' ',''),'0'),'.',''),',','.'))                    AS VLR_CTB_INT_CV                                    
                  ,TRIM(DCR_COTA_INDEX_PLAN_1)                                                                            AS DCR_COTA_INDEX_PLAN_1  
                  ,TRIM(DCR_COTA_INDEX_PLAN_2)                                                                            AS DCR_COTA_INDEX_PLAN_2
                  ,TRIM(DCR_CTA_APOS_INDIV_VOL_PARTIC)                                                                    AS DCR_CTA_APOS_INDIV_VOL_PARTIC                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INI_CTA_APO_INDI_VOL_PARTI,' ',''),'0'),'.',''),',','.'))    AS SLD_INI_CTA_APO_INDI_VOL_PARTI
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_APO_INDI_VOL_PARTI,' ',''),'0'),'.',''),',','.'))    AS VLR_TOT_CTB_APO_INDI_VOL_PARTI                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(REN_TOT_CTB_APO_INDI_VOL_PARTI,' ',''),'0'),'.',''),',','.'))    AS REN_TOT_CTB_APO_INDI_VOL_PARTI                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_APO_INDI_VOL_PARTI,' ',''),'0'),'.',''),',','.'))    AS SLD_FIM_CTA_APO_INDI_VOL_PARTI
                  ,TRIM(DCR_CTA_APOS_INDIV_ESPO_PARTIC)                                                                   AS DCR_CTA_APOS_INDIV_ESPO_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INI_CTA_APO_INDI_ESPOPARTI,' ',''),'0'),'.',''),',','.'))    AS SLD_INI_CTA_APO_INDI_ESPOPARTI
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_APO_INDI_ESPOPARTI,' ',''),'0'),'.',''),',','.'))    AS VLR_TOT_CTB_APO_INDI_ESPOPARTI
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(REN_TOT_CTB_APO_INDI_ESPOPARTI,' ',''),'0'),'.',''),',','.'))    AS REN_TOT_CTB_APO_INDI_ESPOPARTI
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_APO_INDI_ESPOPARTI,' ',''),'0'),'.',''),',','.'))    AS SLD_FIM_CTA_APO_INDI_ESPOPARTI
                  ,TRIM(DCR_CTA_APOS_INDIV_VOL_PATROC)                                                                    AS DCR_CTA_APOS_INDIV_VOL_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INI_CTA_APO_INDI_VOL_PATRO,' ',''),'0'),'.',''),',','.'))    AS SLD_INI_CTA_APO_INDI_VOL_PATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_APO_INDI_VOL_PATRO,' ',''),'0'),'.',''),',','.'))    AS VLR_TOT_CTB_APO_INDI_VOL_PATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(REN_TOT_CTB_APO_INDI_VOL_PATRO,' ',''),'0'),'.',''),',','.'))    AS REN_TOT_CTB_APO_INDI_VOL_PATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_APO_INDI_VOL_PATRO,' ',''),'0'),'.',''),',','.'))    AS SLD_FIM_CTA_APO_INDI_VOL_PATRO                  
                  ,TRIM(DCR_CTA_APOS_INDIV_SUPL_PATROC)                                                                   AS DCR_CTA_APOS_INDIV_SUPL_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INI_CTA_APO_INDI_SUPLPATRO,' ',''),'0'),'.',''),',','.'))    AS SLD_INI_CTA_APO_INDI_SUPLPATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_APO_INDI_SUPLPATRO,' ',''),'0'),'.',''),',','.'))    AS VLR_TOT_CTB_APO_INDI_SUPLPATRO                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(REN_TOT_CTB_APO_INDI_SUPLPATRO,' ',''),'0'),'.',''),',','.'))    AS REN_TOT_CTB_APO_INDI_SUPLPATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_APO_INDI_SUPLPATRO,' ',''),'0'),'.',''),',','.'))    AS SLD_FIM_CTA_APO_INDI_SUPLPATRO
                  ,TRIM(DCR_PORT_TOTAL)                                                                                   AS DCR_PORT_TOTAL
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INIC_CTA_PORT_TOT,' ',''),'0'),'.',''),',','.'))             AS SLD_INIC_CTA_PORT_TOT                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_PORT_TOT,' ',''),'0'),'.',''),',','.'))              AS VLR_TOT_CTB_PORT_TOT
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_TOT_CTB_PORT_TOT,' ',''),'0'),'.',''),',','.'))             AS RENT_TOT_CTB_PORT_TOT
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_PORT_TOT,' ',''),'0'),'.',''),',','.'))              AS SLD_FIM_CTA_PORT_TOT
                  ,TRIM(DCR_PORT_ABERTA)                                                                                  AS DCR_PORT_ABERTA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INIC_CTA_PORT_ABERTA,' ',''),'0'),'.',''),',','.'))          AS SLD_INIC_CTA_PORT_ABERTA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_PORT_ABERTA,' ',''),'0'),'.',''),',','.'))           AS VLR_TOT_CTB_PORT_ABERTA                                
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_TOT_CTB_PORT_ABERTA,' ',''),'0'),'.',''),',','.'))          AS RENT_TOT_CTB_PORT_ABERTA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_PORT_ABERTA,' ',''),'0'),'.',''),',','.'))           AS SLD_FIM_CTA_PORT_ABERTA
                  ,TRIM(DCR_PORT_FECHADA)                                                                                 AS DCR_PORT_FECHADA                 
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INIC_CTA_PORT_FECHADA,' ',''),'0'),'.',''),',','.'))         AS SLD_INIC_CTA_PORT_FECHADA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_PORT_FECHADA,' ',''),'0'),'.',''),',','.'))          AS VLR_TOT_CTB_PORT_FECHADA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_TOT_CTB_PORT_FECHADA,' ',''),'0'),'.',''),',','.'))         AS RENT_TOT_CTB_PORT_FECHADA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_PORT_FECHADA,' ',''),'0'),'.',''),',','.'))          AS SLD_FIM_CTA_PORT_FECHADA
                  ,TRIM(DCR_PORT_JOIA_ABERTA)                                                                             AS DCR_PORT_JOIA_ABERTA                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INIC_CTA_PORT_JOIA_ABERTA,' ',''),'0'),'.',''),',','.'))     AS SLD_INIC_CTA_PORT_JOIA_ABERTA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_PORT_JOIA_ABERTA,' ',''),'0'),'.',''),',','.'))      AS VLR_TOT_CTB_PORT_JOIA_ABERTA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_TOT_CTB_PORT_JOIA_ABERTA,' ',''),'0'),'.',''),',','.'))     AS RENT_TOT_CTB_PORT_JOIA_ABERTA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_PORT_JOIA_ABERTA,' ',''),'0'),'.',''),',','.'))      AS SLD_FIM_CTA_PORT_JOIA_ABERTA                  
                  ,TRIM(DCR_PORT_JOIA_FECHADA)                                                                            AS DCR_PORT_JOIA_FECHADA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INIC_CTA_PORT_JOIA_FECHADA,' ',''),'0'),'.',''),',','.'))    AS SLD_INIC_CTA_PORT_JOIA_FECHADA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_PORT_JOIA_FECHADA,' ',''),'0'),'.',''),',','.'))     AS VLR_TOT_CTB_PORT_JOIA_FECHADA                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_TOT_CTB_PORT_JOIA_FECHADA,' ',''),'0'),'.',''),',','.'))    AS RENT_TOT_CTB_PORT_JOIA_FECHADA
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_PORT_JOIA_FECHADA,' ',''),'0'),'.',''),',','.'))     AS SLD_FIM_CTA_PORT_JOIA_FECHADA
                  ,TRIM(DCR_DISTR_FUND_PREV_PARTIC)                                                                       AS DCR_DISTR_FUND_PREV_PARTIC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INI_DIST_FUND_PREV_PARTI,' ',''),'0'),'.',''),',','.'))      AS SLD_INI_DIST_FUND_PREV_PARTI                                    
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_DIST_FUND_PREV_PARTI,' ',''),'0'),'.',''),',','.'))      AS VLR_TOT_DIST_FUND_PREV_PARTI
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(REN_TOT_DIST_FUND_PREV_PARTI,' ',''),'0'),'.',''),',','.'))      AS REN_TOT_DIST_FUND_PREV_PARTI
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLDFIM_CTA_DISTFUNDPREVPARTI,' ',''),'0'),'.',''),',','.'))      AS SLDFIM_CTA_DISTFUNDPREVPARTI
                  ,TRIM(DCR_DISTR_FUND_PREV_PATROC)                                                                       AS DCR_DISTR_FUND_PREV_PATROC
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INI_DIST_FUND_PREV_PATRO,' ',''),'0'),'.',''),',','.'))      AS SLD_INI_DIST_FUND_PREV_PATRO                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_DIST_FUND_PREV_PATRO,' ',''),'0'),'.',''),',','.'))      AS VLR_TOT_DIST_FUND_PREV_PATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(REN_TOT_DIST_FUND_PREV_PATRO,' ',''),'0'),'.',''),',','.'))      AS REN_TOT_DIST_FUND_PREV_PATRO
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLDFIM_CTA_DISTFUNDPREVPATRO,' ',''),'0'),'.',''),',','.'))      AS SLDFIM_CTA_DISTFUNDPREVPATRO
                  ,TRIM(DCR_PORT_FINAL)                                                                                   AS DCR_PORT_FINAL                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_INIC_CTA_PORT_FIM,' ',''),'0'),'.',''),',','.'))             AS SLD_INIC_CTA_PORT_FIM                  
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(VLR_TOT_CTB_PORT_FIM,' ',''),'0'),'.',''),',','.'))              AS VLR_TOT_CTB_PORT_FIM
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(RENT_TOT_CTB_PORT_FIM,' ',''),'0'),'.',''),',','.'))             AS RENT_TOT_CTB_PORT_FIM
                  ,TO_NUMBER(REPLACE(REPLACE(NVL(REPLACE(SLD_FIM_CTA_PORT_FIM,' ',''),'0'),'.',''),',','.'))              AS SLD_FIM_CTA_PORT_FIM
                  ,TRIM(DCR_SLD_PROJETADO)                                                                                AS DCR_SLD_PROJETADO                  
                  ,TO_NUMBER(REPLACE(REPLACE(REPLACE(VLR_SLD_PROJETADO, '.',''), CHR(13), ''), ',','.'))                  AS VLR_SLD_PROJETADO


---     ###       ATUALMENTE O CORPORATIVO NAO GERA DADOS PARA OS BENEFICIOS ADICIONAIS.
---               OS DADOS PARA ESSES CAMPOS SAO GERADOS NO ROTINA DO EXTRATO PREVIDENCIARIO 
                  --,VLR_SLD_ADICIONAL                                                                      AS VLR_SLD_ADICIONAL
                  --,NVL(REPLACE(REPLACE(VLR_BENEF_ADICIONAL,'.',''),',','.'),' ')                          AS VLR_BENEF_ADICIONAL                                                       
                  
---     ###        AGUARDANDO DEFINICAO DA CNPC32, NAO EXISTEM DADOS ATUALMENTE NO COPORATIVO P/ OS ATRIBUTOS ABAIXO:
                                    
/*                ,NVL(DTA_ULT_ATUAL,' ')                                                                 AS DTA_ULT_ATUAL
                  ,NVL(REPLACE(REPLACE(VLR_CONTRIB_RISCO,'.',''),',','.'),' ')                            AS VLR_CONTRIB_RISCO
                  ,NVL(REPLACE(REPLACE(VLR_CONTRIB_PATRC,'.',''),',','.'),' ')                            AS VLR_CONTRIB_PATRC
                  ,NVL(REPLACE(REPLACE(VLR_CAPIT_SEGURADO,'.',''),',','.'),' ')                           AS VLR_CAPIT_SEGURADO
                  ,NVL(REPLACE(REPLACE(VLR_CONTRIB_ADM,'.',''),',','.'),' ')                              AS VLR_CONTRIB_ADM
                  ,NVL(REPLACE(REPLACE(VLR_CONTRIB_ADM_PATRC,'.',''),',','.'),' ')                        AS VLR_CONTRIB_ADM_PATRC
                  ,NVL(REPLACE(REPLACE(VLR_SIMUL_BENEF_PORCETAGEM,'.',''),',','.'),' ')                   AS VLR_SIMUL_BENEF_PORCETAGEM
                  ,NVL(DTA_ELEGIB_BENEF_PORCETAGEM,' ')                                                   AS DTA_ELEGIB_BENEF_PORCETAGEM
                  ,NVL(IDADE_ELEGIB_PORCETAGEM,' ')                                                       AS IDADE_ELEGIB_PORCETAGEM
                  ,NVL(DTA_EXAURIM_BENEF_PORCETAGEM,' ')                                                  AS DTA_EXAURIM_BENEF_PORCETAGEM
                  ,NVL(REPLACE(REPLACE(VLR_SIMUL_BENEF_PRAZO,'.',''),',','.'),' ')                        AS VLR_SIMUL_BENEF_PRAZO
                  ,NVL(DTA_ELEGIB_BENEF_PRAZO,' ')                                                        AS DTA_ELEGIB_BENEF_PRAZO
                  ,NVL(IDADE_ELEGIB_BENEF_PRAZO,' ')                                                      AS IDADE_ELEGIB_BENEF_PRAZO
                  ,NVL(DTA_EXAURIM_BENEF_PRAZO,' ')    
*/

                  
            FROM OWN_FUNCESP.FC_PRE_TBL_CARGA_EXTRATO; 
                               
    BEGIN                      
        
        BEGIN             
             
             SELECT COUNT(*) INTO G_CONT_TEMP FROM FC_PRE_TBL_CARGA_EXTRATO  WHERE TPO_DADO = 1;
              
           EXCEPTION
             WHEN OTHERS THEN 
                DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO: ' || SQLCODE || ' MSG: ' ||SQLERRM);
                DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
             
        END;              
      --
      --

         
         
      FOR RG_TRATA_DADOS IN C_TRATA_DADOS 
         LOOP
         
/*            DBMS_OUTPUT.PUT_LINE(RG_TRATA_DADOS.TPO_DADO                                             ||CHR(13)||
                                 RG_TRATA_DADOS.COD_EMPRS                                            ||CHR(13)||
                                 RG_TRATA_DADOS.NUM_RGTRO_EMPRG                                      ||CHR(13)||
                                 RG_TRATA_DADOS.NOM_EMPRG                                            ||CHR(13)||
                                 RG_TRATA_DADOS.DTA_EMISS
                                 --RG_TRATA_DADOS.NUM_FOLHA                                            ||CHR(13)||
                                 --RG_TRATA_DADOS.DCR_PLANO                                            ||CHR(13)||
                                 --RG_TRATA_DADOS.PER_INIC_EXTR                                        ||CHR(13)||
                                 --RG_TRATA_DADOS.PER_FIM_EXTR                                         ||CHR(13)||
                                 --RG_TRATA_DADOS.DTA_INIC_EXTR                                        ||CHR(13)||                                                               
                                 --TO_CHAR(RG_TRATA_DADOS.DTA_FIM_EXTR,'DD/MM/RRRR')                   ||CHR(13)
                               );  */ 
                               

           
         DELETE FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB
         WHERE  TPO_DADO         = RG_TRATA_DADOS.TPO_DADO
           AND  COD_EMPRS        = RG_TRATA_DADOS.COD_EMPRS
           AND  NUM_RGTRO_EMPRG  = RG_TRATA_DADOS.NUM_RGTRO_EMPRG           
           AND  DTA_FIM_EXTR     = TO_DATE(RG_TRATA_DADOS.DTA_FIM_EXTR,'DD/MM/RRRR')
           AND  DCR_PLANO        = RG_TRATA_DADOS.DCR_PLANO;
         COMMIT;     
     
         INSERT INTO ATT.FC_PRE_TBL_BASE_EXTRAT_CTB (  TPO_DADO
                                                      ,COD_EMPRS
                                                      ,NUM_RGTRO_EMPRG
                                                      ,NOM_EMPRG
                                                      ,DTA_EMISS
                                                      ,NUM_FOLHA
                                                      ,DCR_PLANO
                                                      ,PER_INIC_EXTR
                                                      ,PER_FIM_EXTR
                                                      ,DTA_INIC_EXTR
                                                      ,DTA_FIM_EXTR
                                                      ,DCR_SLD_MOV_SALDADO
                                                      ,SLD_PL_SALDADO_MOV_INIC
                                                      ,CTB_PL_SALDADO_MOV
                                                      ,RENT_PL_SALDADO_MOV
                                                      ,SLD_PL_SALDADO_MOV_FIM
                                                      ,DCR_SLD_MOV_BD
                                                      ,SLD_PL_BD_INIC
                                                      ,CTB_PL_MOV_BD
                                                      ,RENT_PL_MOV_BD
                                                      ,SLD_PL_BD_MOV_FIM 
                                                      ,DCR_SLD_MOV_CV 
                                                      ,SLD_PL_CV_MOV_INIC
                                                      ,CTB_PL_MOV_CV
                                                      ,RENT_PL_MOV_CV
                                                      ,SLD_PL_CV_MOV_FIM
                                                      ,DCR_CTA_OBRIG_PARTIC
                                                      ,SLD_CTA_OBRIG_PARTIC
                                                      ,CTB_CTA_OBRIG_PARTIC
                                                      ,RENT_CTA_OBRIG_PARTIC
                                                      ,SLD_CTA_OBRIG_PARTIC_FIM
                                                      ,DCR_CTA_NORM_PATROC 
                                                      ,SLD_CTA_NORM_PATROC  
                                                      ,CTB_CTA_NORM_PATROC
                                                      ,RENT_NORM_PATROC
                                                      ,SLD_NORM_PATROC_INIC                                                 
                                                      ,DCR_CTA_ESPEC_PARTIC
                                                      ,SLD_CTA_ESPEC_PARTIC
                                                      ,CTB_CTA_ESPEC_PARTIC
                                                      ,RENT_CTA_ESPEC_PARTIC
                                                      ,SLD_CTA_ESPEC_PARTIC_INIC
                                                      ,DCR_CTA_ESPEC_PATROC
                                                      ,SLD_CTA_ESPEC_PATROC
                                                      ,CTB_CTA_ESPEC_PATROC
                                                      ,RENT_CTA_ESPEC_PATROC
                                                      ,SLD_CTA_ESPEC_PATROC_INIC
                                                      ,SLD_TOT_INIC
                                                      ,CTB_TOT_INIC
                                                      ,RENT_PERIODO
                                                      ,SLD_TOT_FIM
                                                      ,PRM_MES_PERIODO_CTB
                                                      ,SEG_MES_PERIODO_CTB
                                                      ,TER_MES_PERIODO_CTB
                                                      ,DCR_TOT_CTB_BD
                                                      ,VLR_TOT_CTB_BD_PRM_MES
                                                      ,VLR_TOT_CTB_BD_SEG_MES
                                                      ,VLR_TOT_CTB_BD_TER_MES
                                                      ,VLR_TOT_CTB_BD_PERIODO
                                                      ,DCR_TOT_CTB_CV
                                                      ,VLR_TOT_CTB_CV_PRM_MES
                                                      ,VLR_TOT_CTB_CV_SEG_MES
                                                      ,VLR_TOT_CTB_CV_TER_MES
                                                      ,VLR_TOT_CTB_CV_PERIODO
                                                      ,DCR_TPO_CTB_VOL_PARTIC
                                                      ,VLR_CTB_VOL_PARTIC_PRM_MES
                                                      ,VLR_CTB_VOL_PARTIC_SEG_MES
                                                      ,VLR_CTB_VOL_PARTIC_TER_MES
                                                      ,VLR_CTB_VOL_PARTIC_PERIODO
                                                      ,DCR_TPO_CTB_VOL_PATROC
                                                      ,VLR_CTB_VOL_PATROC_PRM_MES
                                                      ,VLR_CTB_VOL_PATROC_SEG_MES
                                                      ,VLR_CTB_VOL_PATROC_TER_MES
                                                      ,VLR_CTB_VOL_PATROC_PERIODO
                                                      ,DCR_TPO_CTB_OBRIG_PARTIC
                                                      ,VLR_CTB_OBRIG_PARTIC_PRM_MES
                                                      ,VLR_CTB_OBRIG_PARTIC_SEG_MES
                                                      ,VLR_CTB_OBRIG_PARTIC_TER_MES
                                                      ,VLR_CTB_OBRIG_PARTIC_PERIODO
                                                      ,DCR_TPO_CTB_OBRIG_PATROC
                                                      ,VLR_CTB_OBRIG_PATROC_PRM_MES
                                                      ,VLR_CTB_OBRIG_PATROC_SEG_MES
                                                      ,VLR_CTB_OBRIG_PATROC_TER_MES
                                                      ,VLR_CTB_OBRIG_PATROC_PERIODO
                                                      ,DCR_TPO_CTB_ESPOR_PATROC
                                                      ,VLR_CTB_ESPOR_PATROC_PRM_MES
                                                      ,VLR_CTB_ESPOR_PATROC_SEG_MES
                                                      ,VLR_CTB_ESPOR_PATROC_TER_MES
                                                      ,VLR_CTB_ESPOR_PATROC_PERIODO
                                                      ,DCR_TPO_CTB_ESPOR_PARTIC
                                                      ,VLR_CTB_ESPOR_PARTIC_PRM_MES
                                                      ,VLR_CTB_ESPOR_PARTIC_SEG_MES
                                                      ,VLR_CTB_ESPOR_PARTIC_TER_MES
                                                      ,VLR_CTB_ESPOR_PARTIC_PERIODO
                                                      ,TOT_CTB_PRM_MES
                                                      ,TOT_CTB_SEG_MES
                                                      ,TOT_CTB_TER_MES
                                                      ,TOT_CTB_EXTRATO
                                                      ,PRM_MES_PERIODO_RENT
                                                      ,SEG_MES_PERIODO_RENT
                                                      ,TER_MES_PERIODO_RENT
                                                      ,PCT_RENT_REAL_PRM_MES
                                                      ,PCT_RENT_REAL_SEG_MES
                                                      ,PCT_RENT_REAL_TER_MES
                                                      ,PCT_RENT_REAL_TOT_MES
                                                      ,PCT_RENT_LMTD_PRM_MES
                                                      ,PCT_RENT_LMTD_SEG_MES
                                                      ,PCT_RENT_LMTD_TER_MES
                                                      ,PCT_RENT_LMTD_TOT_MES
                                                      ,PCT_RENT_IGPDI_PRM_MES
                                                      ,PCT_RENT_IGPDI_SEG_MES
                                                      ,PCT_RENT_IGPDI_TER_MES   
                                                      ,PCT_RENT_IGPDI_TOT_MES
                                                      ,PCT_RENT_URR_PRM_MES
                                                      ,PCT_RENT_URR_SEG_MES
                                                      ,PCT_RENT_URR_TER_MES
                                                      ,PCT_RENT_URR_TOT_MES
                                                      ,DTA_APOS_PROP
                                                      ,DTA_APOS_INTE
                                                      ,VLR_BENEF_PSAP_PROP
                                                      ,VLR_BENEF_PSAP_INTE
                                                      ,VLR_BENEF_BD_PROP
                                                      ,VLR_BENEF_BD_INTE
                                                      ,VLR_BENEF_CV_PROP
                                                      ,VLR_BENEF_CV_INTE
                                                      ,RENDA_ESTIM_PROP
                                                      ,RENDA_ESTIM_INT
                                                      ,VLR_RESERV_SALD_LQDA
                                                      ,TXT_PRM_MENS
                                                      ,TXT_SEG_MENS
                                                      ,TXT_TER_MENS
                                                      ,TXT_QUA_MENS
                                                      ,IDADE_PROP_BSPS
                                                      ,VLR_CTB_PROP_BSPS
                                                      ,IDADE_INT_BSPS
                                                      ,VLR_CTB_INT_BSPS
                                                      ,IDADE_PROP_BD
                                                      ,VLR_CTB_PROP_BD
                                                      ,IDADE_INT_BD
                                                      ,VLR_CTB_INT_BD
                                                      ,IDADE_PROP_CV
                                                      ,VLR_CTB_PROP_CV 
                                                      ,IDADE_INT_CV
                                                      ,VLR_CTB_INT_CV
                                                      ,DCR_COTA_INDEX_PLAN_1
                                                      ,DCR_COTA_INDEX_PLAN_2
                                                      ,DCR_CTA_APOS_INDIV_VOL_PARTIC
                                                      ,SLD_INI_CTA_APO_INDI_VOL_PARTI
                                                      ,VLR_TOT_CTB_APO_INDI_VOL_PARTI
                                                      ,REN_TOT_CTB_APO_INDI_VOL_PARTI
                                                      ,SLD_FIM_CTA_APO_INDI_VOL_PARTI
                                                      ,DCR_CTA_APOS_INDIV_ESPO_PARTIC
                                                      ,SLD_INI_CTA_APO_INDI_ESPOPARTI
                                                      ,VLR_TOT_CTB_APO_INDI_ESPOPARTI
                                                      ,REN_TOT_CTB_APO_INDI_ESPOPARTI
                                                      ,SLD_FIM_CTA_APO_INDI_ESPOPARTI
                                                      ,DCR_CTA_APOS_INDIV_VOL_PATROC
                                                      ,SLD_INI_CTA_APO_INDI_VOL_PATRO
                                                      ,VLR_TOT_CTB_APO_INDI_VOL_PATRO
                                                      ,REN_TOT_CTB_APO_INDI_VOL_PATRO
                                                      ,SLD_FIM_CTA_APO_INDI_VOL_PATRO
                                                      ,DCR_CTA_APOS_INDIV_SUPL_PATROC
                                                      ,SLD_INI_CTA_APO_INDI_SUPLPATRO
                                                      ,VLR_TOT_CTB_APO_INDI_SUPLPATRO
                                                      ,REN_TOT_CTB_APO_INDI_SUPLPATRO
                                                      ,SLD_FIM_CTA_APO_INDI_SUPLPATRO
                                                      ,DCR_PORT_TOTAL
                                                      ,SLD_INIC_CTA_PORT_TOT
                                                      ,VLR_TOT_CTB_PORT_TOT
                                                      ,RENT_TOT_CTB_PORT_TOT
                                                      ,SLD_FIM_CTA_PORT_TOT
                                                      ,DCR_PORT_ABERTA
                                                      ,SLD_INIC_CTA_PORT_ABERTA
                                                      ,VLR_TOT_CTB_PORT_ABERTA
                                                      ,RENT_TOT_CTB_PORT_ABERTA
                                                      ,SLD_FIM_CTA_PORT_ABERTA
                                                      ,DCR_PORT_FECHADA
                                                      ,SLD_INIC_CTA_PORT_FECHADA
                                                      ,VLR_TOT_CTB_PORT_FECHADA      
                                                      ,RENT_TOT_CTB_PORT_FECHADA
                                                      ,SLD_FIM_CTA_PORT_FECHADA
                                                      ,DCR_PORT_JOIA_ABERTA
                                                      ,SLD_INIC_CTA_PORT_JOIA_ABERTA
                                                      ,VLR_TOT_CTB_PORT_JOIA_ABERTA
                                                      ,RENT_TOT_CTB_PORT_JOIA_ABERTA
                                                      ,SLD_FIM_CTA_PORT_JOIA_ABERTA
                                                      ,DCR_PORT_JOIA_FECHADA
                                                      ,SLD_INIC_CTA_PORT_JOIA_FECHADA
                                                      ,VLR_TOT_CTB_PORT_JOIA_FECHADA
                                                      ,RENT_TOT_CTB_PORT_JOIA_FECHADA
                                                      ,SLD_FIM_CTA_PORT_JOIA_FECHADA
                                                      ,DCR_DISTR_FUND_PREV_PARTIC
                                                      ,SLD_INI_DIST_FUND_PREV_PARTI
                                                      ,VLR_TOT_DIST_FUND_PREV_PARTI
                                                      ,REN_TOT_DIST_FUND_PREV_PARTI
                                                      ,SLDFIM_CTA_DISTFUNDPREVPARTI
                                                      ,DCR_DISTR_FUND_PREV_PATROC
                                                      ,SLD_INI_DIST_FUND_PREV_PATRO
                                                      ,VLR_TOT_DIST_FUND_PREV_PATRO           
                                                      ,REN_TOT_DIST_FUND_PREV_PATRO
                                                      ,SLDFIM_CTA_DISTFUNDPREVPATRO
                                                      ,DCR_PORT_FINAL
                                                      ,SLD_INIC_CTA_PORT_FIM
                                                      ,VLR_TOT_CTB_PORT_FIM
                                                      ,RENT_TOT_CTB_PORT_FIM
                                                      ,SLD_FIM_CTA_PORT_FIM
                                                      ,DCR_SLD_PROJETADO
                                                      ,VLR_SLD_PROJETADO													  
                                                      --,VLR_SLD_ADICIONAL
                                                      --,VLR_BENEF_ADICIONAL 
                                                      --,DTA_ULT_ATUAL
                                                      --,VLR_CONTRIB_RISCO
                                                      --,VLR_CONTRIB_PATRC
                                                      --,VLR_CAPIT_SEGURADO
                                                      --,VLR_CONTRIB_ADM
                                                      --,VLR_CONTRIB_ADM_PATRC
                                                      --,VLR_SIMUL_BENEF_PORCETAGEM
                                                      --,DTA_ELEGIB_BENEF_PORCETAGEM
                                                      --,IDADE_ELEGIB_PORCETAGEM
                                                      --,DTA_EXAURIM_BENEF_PORCETAGEM
                                                      --,VLR_SIMUL_BENEF_PRAZO
                                                      --,DTA_ELEGIB_BENEF_PRAZO
                                                      --,IDADE_ELEGIB_BENEF_PRAZO
                                                      --,DTA_EXAURIM_BENEF_PRAZO
                                                     )
                                              VALUES
                                                     (  RG_TRATA_DADOS.TPO_DADO
                                                       ,RG_TRATA_DADOS.COD_EMPRS
                                                       ,RG_TRATA_DADOS.NUM_RGTRO_EMPRG
                                                       ,RG_TRATA_DADOS.NOM_EMPRG
                                                       ,TO_DATE(RG_TRATA_DADOS.DTA_EMISS,'DD/MM/RRRR')
                                                       ,RG_TRATA_DADOS.NUM_FOLHA
                                                       ,RG_TRATA_DADOS.DCR_PLANO
                                                       ,RG_TRATA_DADOS.PER_INIC_EXTR
                                                       ,RG_TRATA_DADOS.PER_FIM_EXTR
                                                       ,TO_DATE(RG_TRATA_DADOS.DTA_INIC_EXTR,'DD/MM/RRRR')
                                                       ,TO_DATE(RG_TRATA_DADOS.DTA_FIM_EXTR,'DD/MM/RRRR')                                                       
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
                                                       -----------------------------------    
                                                       -- SEM DADOS DO CORPORATIVO:     --
                                                       -----------------------------------
                                                       --,RG_TRATA_DADOS.VLR_SLD_ADICIONAL
                                                       --,RG_TRATA_DADOS.VLR_BENEF_ADICIONAL 
                                                       --,RG_TRATA_DADOS.DTA_ULT_ATUAL
                                                       --,RG_TRATA_DADOS.VLR_CONTRIB_RISCO
                                                       --,RG_TRATA_DADOS.VLR_CONTRIB_PATRC
                                                       --,RG_TRATA_DADOS.VLR_CAPIT_SEGURADO
                                                       --,RG_TRATA_DADOS.VLR_CONTRIB_ADM
                                                       --,RG_TRATA_DADOS.VLR_CONTRIB_ADM_PATRC
                                                       --,RG_TRATA_DADOS.VLR_SIMUL_BENEF_PORCETAGEM
                                                       --,RG_TRATA_DADOS.DTA_ELEGIB_BENEF_PORCETAGEM
                                                       --,RG_TRATA_DADOS.IDADE_ELEGIB_PORCETAGEM
                                                       --,RG_TRATA_DADOS.DTA_EXAURIM_BENEF_PORCETAGEM
                                                       --,RG_TRATA_DADOS.VLR_SIMUL_BENEF_PRAZO                                                        
                                                      );                                                                                                      
                                                 
         G_TPO_DADO     := RG_TRATA_DADOS.TPO_DADO;
         G_COD_EMPRS    := RG_TRATA_DADOS.COD_EMPRS;
         G_DTA_FIM_EXTR := TO_DATE(RG_TRATA_DADOS.DTA_FIM_EXTR,'DD/MM/RRRR');
         G_DTA_EMISS    := TO_DATE(RG_TRATA_DADOS.DTA_EMISS,'DD/MM/RRRR');
         G_DCR_PLANO    := RG_TRATA_DADOS.DCR_PLANO;                                                  
                               
         --DBMS_OUTPUT.PUT_LINE(G_DTA_EMISS);                   
         --DBMS_OUTPUT.PUT_LINE(RG_TRATA_DADOS.DTA_EMISS);
                                             
         IF SQL%ROWCOUNT > 0 THEN                                                                                                                                
              V_COUNT := V_COUNT + 1;                                                                                                                                                         
         END IF;
                   
      END LOOP;
      COMMIT;
      DBMS_OUTPUT.PUT_LINE('Total de Registros Carregado no Portal: '||G_CONT_TEMP);
      --
      --            
      
      -- GERA LOG DO PROCESSAMENTO:
      BEGIN
         SELECT COUNT(*) 
           INTO G_COUNT_LOG
         FROM ATT.FC_PRE_TBL_BASE_EXTRAT_CTB
          WHERE TPO_DADO     = G_TPO_DADO
            AND COD_EMPRS    = G_COD_EMPRS
            AND DTA_FIM_EXTR = G_DTA_FIM_EXTR            
            AND DCR_PLANO    = G_DCR_PLANO
            AND DTA_EMISS    = G_DTA_EMISS;
      
        IF (G_COUNT_LOG > 0) THEN
           
           G_CKECK := 'I'; -- INSERIDO
        ELSE
           G_CKECK := 'E'; -- ERRO       
        END IF;  
        
       EXCEPTION
         WHEN OTHERS THEN         
           G_CKECK := 'A'; -- ABEND
      END;   
      
      
      BEGIN
         
          SELECT  SYS_CONTEXT('USERENV', 'MODULE')       AS MODULE
                 ,SYS_CONTEXT('USERENV', 'OS_USER')      AS OS_USER
                 ,SYS_CONTEXT('USERENV', 'TERMINAL')     AS TERMINAL
                 ,SYS_CONTEXT('USERENV', 'CURRENT_USER') AS "CURRENT_USER" 
                 ,SYS_CONTEXT('USERENV', 'IP_ADDRESS')   AS IP_ADDRESS  
           INTO   G_MODULE
                 ,G_OS_USER
                 ,G_TERMINAL
                 ,G_CURRENT_USER
                 ,G_IP_ADDRESS                 
         FROM DUAL;  
         
         DELETE FROM OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO
         WHERE  TPO_DADO     = G_TPO_DADO
         AND    COD_EMPRS    = G_COD_EMPRS
         AND    DTA_FIM_EXTR = G_DTA_FIM_EXTR
         AND    DTA_EMISS    = G_DTA_EMISS
         AND    DCR_PLANO    = G_DCR_PLANO;
         COMMIT;
    
         
         
         INSERT INTO OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO (   COD_LOG_CARGA_EXTRATO
                                                              ,TPO_DADO
                                                              ,COD_EMPRS
                                                              ,DTA_FIM_EXTR
                                                              ,DTA_EMISS
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
                                                              ,DCR_PLANO
                                                            )
                                                    VALUES ( OWN_FUNCESP.PRE_TBL_LOG_CARGA_EXTRATO_SEQ.NEXTVAL
                                                              ,G_TPO_DADO            
                                                              ,G_COD_EMPRS                
                                                              ,G_DTA_FIM_EXTR  
                                                              ,G_DTA_EMISS     
                                                              ,G_CONT_TEMP           
                                                              ,SYSDATE          
                                                              ,G_CKECK               
                                                              ,NULL
                                                               --           
                                                              ,G_MODULE               
                                                              ,G_OS_USER             
                                                              ,G_TERMINAL            
                                                              ,G_CURRENT_USER       
                                                              ,G_IP_ADDRESS
                                                              ,G_DCR_PLANO
                                                           );
                                                      COMMIT;
         
      
      END;
                            
      
      IF V_COUNT = G_CONT_TEMP THEN 
           R_VALIDA:= TRUE;    
        RETURN R_VALIDA;        
         COMMIT;         
          
      ELSE
         RETURN R_VALIDA;       
       ROLLBACK;
       
      END IF;               
                                     
      --
      EXCEPTION    
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('CODIGO DO ERRO: ' || SQLCODE || ' MSG: ' ||SQLERRM);
            DBMS_OUTPUT.PUT_LINE('LINHA: ' || DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
         RETURN FALSE;
         NULL;
    END FN_TRATA_ARQUIVO;
   
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
          R_VLR_BENEF_BD_PROP   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_PROP%TYPE DEFAULT NULL;
          R_VLR_BENEF_BD_INTE   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.VLR_BENEF_BD_INTE%TYPE DEFAULT NULL;        
          

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
                  ELSIF (P_CALC = 4) THEN                
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
                ELSE
                  DBMS_OUTPUT.PUT_LINE('-----------------------');                                                      
                  
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
                    --DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: '||TO_CHAR(L_COUNT));
                    DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: '||TO_CHAR(''));
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
  PROCEDURE PROC_EXT_PREV_ELETROPAULO (  PCOD_EMPRESA ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.COD_EMPRS%TYPE
                                        ,PDCR_PLANO   ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DCR_PLANO%TYPE
                                        ,PDTA_MOV     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL)
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
              --DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: ' || TO_CHAR(L_C_UPD));
              DBMS_OUTPUT.PUT_LINE('LINHAS AFETADAS: ' || TO_CHAR(''));
            END IF;
          END IF;
      END LOOP;
      --
    END;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.PUT_LINE(SQLCODE || ' - ' || SQLERRM);

  END PROC_EXT_PREV_ELETROPAULO;


  PROCEDURE PRE_INICIA_PROCESSAMENTO( P_PRC_PROCESSO NUMBER DEFAULT NULL  -- 1: ELETROPAULO / 2: TIM / 3: TIETE 
                                     ,P_PRC_DATA     ATT.FC_PRE_TBL_BASE_EXTRAT_CTB.DTA_FIM_EXTR%TYPE DEFAULT NULL
                                     ,P_NAME_ARQ     VARCHAR2 DEFAULT NULL)
  IS   
  
  VAR_FUNC   BOOLEAN;
  
  BEGIN
      
     IF (P_PRC_PROCESSO = 1) THEN
        --
        PRC_CARGA_ARQUIVO(P_NAME_ARQ);     
        VAR_FUNC := OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.FN_TRATA_ARQUIVO; 
        PROC_EXT_PREV_ELETROPAULO(40,'PSAP/ELETROPAULO', P_PRC_DATA); -- ELETROPAULO
        COMMIT;
        --
        ELSIF (P_PRC_PROCESSO = 2) THEN
        --
        
        PRC_CARGA_ARQUIVO(P_NAME_ARQ);     
        VAR_FUNC := OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.FN_TRATA_ARQUIVO;         
        PROC_EXT_PREV_ELETROPAULO(60,'PSAP/ELETROPAULO', P_PRC_DATA); -- TIM
        COMMIT;
        --
        ELSIF (P_PRC_PROCESSO = 3) THEN
        --
        
        PRC_CARGA_ARQUIVO(P_NAME_ARQ);     
        VAR_FUNC := OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.FN_TRATA_ARQUIVO;         
        PROC_EXT_PREV_TIETE(44,'PSAP/TIETE', P_PRC_DATA); -- TIETE
        COMMIT;
        --
        ELSIF (P_PRC_PROCESSO = 4) THEN -- SALDADO: APENAS PLANO CD
        --
        
        PRC_CARGA_ARQUIVO(P_NAME_ARQ);     
        VAR_FUNC := OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.FN_TRATA_ARQUIVO;
        COMMIT;   
        --        
        ELSE
        --
        
        DBMS_OUTPUT.PUT_LINE(' - ');
        DBMS_OUTPUT.PUT_LINE('CODIGO ERRO: '||SQLCODE|| ' - '||'MSG: '||SQLERRM);
        DBMS_OUTPUT.PUT_LINE('LINHA: '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE);
        --
      END IF;
      
  END PRE_INICIA_PROCESSAMENTO;
  
END PKG_EXT_PREVIDENCIARIO;
