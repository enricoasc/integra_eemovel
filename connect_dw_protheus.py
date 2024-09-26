from datetime import datetime
import pymssql
from send_mail import notifica as ntf
import inspect
from dotenv import load_dotenv
import os

class dw_protheus:
    load_dotenv()    
    def __init__(self):
        # Configurações de conexão
        try:
            self.server = os.getenv("SERVER_DW")
            self.database = os.getenv("DATABASE_DW")
            self.username = os.getenv("USER_DW")
            self.password = os.getenv("PASS_DW")
            self.conn = pymssql.connect(server=self.server, user=self.username, password=self.password, database=self.database, as_dict=True)
            self.SQL_QUERY = """
                    SELECT A1_COD+' / '+A1_LOJA AS CODIGO, A1_ULTCOM AS ULT_COMPRA
                    FROM SA1010
                    WHERE A1_CGC = %s
                    AND D_E_L_E_T_ = ''
                """
        except pymssql.Error as err:
            ntf().envia(f'''Erro de execução  
                            \nClasse : {self.__class__.__name__} 
                            \nFunção : {inspect.currentframe().f_code.co_name}  
                            \nErro   : {err}''')
        

    def input_codlj(self,df_eemovel):
        try: 
            cursor = self.conn.cursor()
            for idx, linha in df_eemovel.iterrows():
                cgcrow = linha['document_number'].strip()
                cursor.execute(self.SQL_QUERY,(cgcrow) )
                record = cursor.fetchone()
                if cursor.rowcount != 0:
                    df_eemovel.loc[idx,'COD_PROTHEUS'] = record['CODIGO']

                    data_ultcompra = datetime.strptime(record['ULT_COMPRA'], '%Y%m%d')
                    data_ultcompra = data_ultcompra.strftime('%d/%m/%Y')
                    df_eemovel.loc[idx,'ULT_COMPRA'] = data_ultcompra

            cursor.close()
            self.conn.close()
            df_eemovel = df_eemovel.fillna('')
            return df_eemovel
        except:
            ntf().envia(f'''Erro de execução  
                            \nClasse : {self.__class__.__name__} 
                            \nFunção : {inspect.currentframe().f_code.co_name}  
                            \nErro   : Erro de inserção de cliente protheus em DataFrame ...: ''')        
    
