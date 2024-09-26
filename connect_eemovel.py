import requests
import pandas as pd
from send_mail import notifica as ntf
import inspect
from dotenv import load_dotenv
import os


class importacao_eemovel:
    load_dotenv()
    def __init__(self) -> None:
        KEY_EEMOVEL = os.getenv("KEY_EEMOVEL")
        self.api_url = 'https://api.prd.agro.eemovel.com.br/agro/agro-hub/delivery/internal/campaing'
        self.headers = {'x-api-key': f'{KEY_EEMOVEL}', 'Accept':'*'}
        self.payload = {'geo':'true'}
        self.api_urlC = 'https://api.prd.agro.eemovel.com.br/agro/agro-hub/delivery/internal/campaing/XXXXX/detail?geo=true'
        self.api_urlO = 'https://api.prd.agro.eemovel.com.br/agro/agro-hub/delivery/internal/campaing/XXXXX/owner'
        self.api_urlS = 'https://api.prd.agro.eemovel.com.br/agro/agro-hub/delivery/internal/campaing/XXXXX'
        self.lista_campanha_ativa = []
        self.df_result = self.busca_dados()


    def busca_dados(self):

        r = requests.get(self.api_url, headers=self.headers)

        if r.status_code == 200:
            jContente = r.json()
        else:
            ntf().envia(f'''Erro de execução  
                        \nClasse : {self.__class__.__name__} 
                        \nFunção : {inspect.currentframe().f_code.co_name}  
                        \nErro   : {r.text}''')
            return ''

        df = pd.json_normalize(jContente,'data')

        ## FILTRA AS CAMPANHAS PARA IMPORTAÇÃO 
        ## #########################################################################
        df_filtrada = df.query("name.str.contains('IMP')")
        self.lista_campanha_ativa = df_filtrada['id'].to_list()

        ## COLETA OS DADOS DAS CAMPANHAS
        ## #########################################################################

        retJoson = list()
        df_owners = pd.DataFrame()
        for id_campanha in self.lista_campanha_ativa:
            api_url2 = self.api_urlC.replace('XXXXX',str(id_campanha))
            r2 = requests.get(api_url2,params=self.payload, headers=self.headers)

            if r2.status_code == 200:
                retJoson.append(r2.json()['data'])
            else:
                ntf().envia(f'''Erro de execução  
                            \nClasse : {self.__class__.__name__} 
                            \nFunção : {inspect.currentframe().f_code.co_name}  
                            \nErro   : {r2.text}''')

        ## COLETA OS DADOS DOS CONTATOS 
        ## #########################################################################
        for  propriedade in  retJoson:
            for realty in propriedade['realty']:
                for owners in realty['owners'] :
                    df_owner = pd.DataFrame([owners])
                    df_owner['uf'] = realty['uf']
                    df_owner['cidade'] = realty['city_name']
                    df_owner['fazenda'] = realty['realty_name']
                    df_owner['area_total'] = realty['total_area']
                    df_owner['area_platavel'] = realty['arable_area']
                    latlog = [str(realty['centroid']['coordinates'][0]).strip(), str(realty['centroid']['coordinates'][1]).strip()]
                    df_owner['coordenadas'] = f'https://www.google.com/maps?q={latlog[1]},{latlog[0]}'
                    df_owner['vendedor'] = propriedade['name'].split('-')[1].strip()
                    df_owner['id_propriedade'] =propriedade['id']

                    ## MONTA O DATA FRAME PARA EXPORTAÇÃO 
                    ## #########################################################################
                    df_owners = pd.concat([df_owners,df_owner], ignore_index=True)
        
        ## BUSCA TELEFONES DOS PROPRIETÁRIOS 
        ## #########################################################################
        for idx ,row in df_owners.iterrows():
            list_telefones = []
            api_url3 = self.api_urlO.replace('XXXXX',str(row['id_propriedade']))
            payload = {'document': row['document_number'].strip() }
            r3 = requests.post(api_url3, headers=self.headers, json=payload)

            if r3.status_code == 200:
                dados_proprietario = r3.json()['data'] 
                telefones = dados_proprietario['phones']
                
                for telefone in telefones:
                    list_telefones.append(telefone['phone_number'])
                
                col_telefones = ' / '.join(list_telefones)
                df_owners.loc[idx,'TELEFONES'] = col_telefones
            else:
                ntf().envia(f'''Erro de execução  
                            \nClasse : {self.__class__.__name__} 
                            \nFunção : {inspect.currentframe().f_code.co_name}  
                            \nErro   : {r3.text}''')                

        if len(self.lista_campanha_ativa) > 0:
            df_owners = df_owners.drop(columns=['id_propriedade'])
            df_owners = df_owners.drop(columns=['realty_id'])
            df_owners = df_owners.drop(columns=['is_customer'])

        return df_owners
    
    ## MARCA A CAMPANHA COMO CONCLUIDA
    ## #########################################################################
    def sinc_campanha(self,xRet):

       
        for campanha in self.lista_campanha_ativa:
            api_url4 = self.api_urlS.replace('XXXXX',str(campanha))
            # r4 = requests.put(api_url4, headers=self.headers)

            # if r4.status_code != 200:
            #     ntf().envia(f'''Erro de execução  
            #                 \nClasse : {self.__class__.__name__} 
            #                 \nFunção : {inspect.currentframe().f_code.co_name}  
            #                 \nErro   : {r4.text}''')                



