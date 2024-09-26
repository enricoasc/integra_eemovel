import yagmail
import os 
from dotenv import load_dotenv

class notifica:
    load_dotenv() 
    def __init__(self) -> None:
        USER_MAIL=os.getenv("USER_MAIL")
        PASS_MAIL=os.getenv("PASS_MAIL")
        self.yag = yagmail.SMTP(USER_MAIL, PASS_MAIL)

    def envia(self,err):
        body = [err]
        subject = ['| ROBÔ EEMOVEL | ERRO DE EXECUÇÃO ']
        self.yag.send(to='enrico@riobrancopetroleo.com.br', subject=subject, contents=body)