from connect_eemovel import importacao_eemovel as iee
from conector_sheet_web import web_sheets_import as wsi
from connect_dw_protheus import dw_protheus as dwp
import logging


def main():
# Configuração do log
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('integra_eemovel.log'),
                                logging.StreamHandler()])
    
    ## BUSCA DADOS DO EEMOVEL
    # Mensagens de log
    logging.info(":... Aplicação iniciada ...:")
    df_dadosE = iee()

    if len(df_dadosE.df_result)>0:
        ## BUSCA DADOS DO PROTHEUS
        # Mensagens de log
        logging.info(":... Cruzando dados com o Banco ...:")        
        df_dadosP = dwp().input_codlj(df_dadosE.df_result)

        ## ENVIA PARA O GOOGLE SHEETS 
        # Mensagens de log
        logging.info(":... Exportando para planilha Web ...:")                
        con_sheets = wsi()
        _cRet=con_sheets.import_df(df_dadosP)

        ## SINALIZA COMO EXPORTADO NA API
        if not _cRet:    
            df_dadosE.sinc_campanha(_cRet)
        else: 
            logging.error(":... Erro ao acessar planilha Web ...:")
        
        logging.info(":... Aplicação Encerrada ...:") 
    else:
        # Mensagens de log
        logging.info(":... Aplicação encerrada, sem dados para processar ...:")
    

if __name__ == '__main__':
    main()



