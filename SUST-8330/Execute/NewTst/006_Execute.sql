BEGIN
  -- TIETE:
  OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.PRE_INICIA_PROCESSAMENTO(  P_PRC_PROCESSO => 3
                                                               ,P_PRC_DATA    => TO_DATE('31/08/2021','DD/MM/RRRR')
                                                               ,P_NAME_ARQ    => 'GeracaoExtratoCorreio_044_031_11092021.txt'
                                                               );
END;     
--
--
BEGIN
  -- TIM:
  OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.PRE_INICIA_PROCESSAMENTO(  P_PRC_PROCESSO => 2
                                                               ,P_PRC_DATA    => TO_DATE('31/08/2021','DD/MM/RRRR')
                                                               ,P_NAME_ARQ    => 'GeracaoExtratoCorreio_060_019_11092021.txt'
                                                               );
END;                                              
--
--
BEGIN
  -- ELETROPAULO/ENEL:
  OWN_FUNCESP.PKG_EXT_PREVIDENCIARIO.PRE_INICIA_PROCESSAMENTO(  P_PRC_PROCESSO => 1
                                                               ,P_PRC_DATA    => TO_DATE('31/08/2021','DD/MM/RRRR')
                                                               ,P_NAME_ARQ    => 'GeracaoExtratoCorreio_040_019_11092021_alterado.txt'
                                                               );
END;                                              
