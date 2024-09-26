from connect_eemovel import importacao_eemovel as iee
from conector_sheet_web import web_sheets_import as wsi
from connect_dw_protheus import dw_protheus as dwp


def main():
    ## BUSCA DADOS DO EEMOVEL
    df_dadosE = iee()

    if len(df_dadosE.df_result)>0:
        ## BUSCA DADOS DO PROTHEUS
        df_dadosP = dwp().input_codlj(df_dadosE.df_result)

        ## ENVIA PARA O GOOGLE SHEETS 
        con_sheets = wsi()
        _cRet=con_sheets.import_df(df_dadosP)

        ## SINALIZA COMO EXPORTADO NA API
        if not _cRet:    
            df_dadosE.sinc_campanha(_cRet)
    

if __name__ == '__main__':
    main()



