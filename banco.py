from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import os
import sqlalchemy
import smtplib
import email.message

# teste = requests.delete(url='https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings/73361?session_token=fd5d8958073e6d5a51c83cd92df32b8b07d22b92')
# print(teste)

class Banco_de_dados():  
    def __init__(self):
        usuario =  os.environ['usuario']
        senha =  os.environ['senha']
        servidor =  os.environ['servidor']
        banco =  os.environ['banco']
        sp_usuario =  os.environ['sp_usuario']
        sp_senha =  os.environ['sp_senha']
        sp_servidor =  os.environ['sp_servidor']
        sp_banco =  os.environ['sp_banco']
        self.bi = create_engine(f"""postgresql://{usuario}:{senha}@{servidor}/{banco}""", pool_pre_ping=True)
        self.tech = create_engine(f"""postgresql://{sp_usuario}:{sp_senha}@{sp_servidor}/{sp_banco}""", pool_pre_ping=True)
        
        
    def consulta(self, servidor, consulta):
        if servidor == 'tech':
            servidor = self.tech
        if servidor == 'bi':
            servidor = self.bi
        df = pd.read_sql_query(consulta, con=servidor)
        return df

    def inserirdados(self, tabela_slots_da_agenda, servidor):
        if servidor == 'tech':
            servidor = self.tech
        if servidor == 'bi':
            servidor = self.bi
        df = pd.DataFrame(tabela_slots_da_agenda, columns=['id_slot', 'data', 'horario', 'area', 'hub', 'regime', 'produto', 'id_tecnica', 'tecnica'])
        df.to_sql(con=servidor, name='slots_abertos', schema='workstation', if_exists='append', method='multi', index=False)
        consulta = f"select * from workstation.slots_abertos"
        tabela = pd.read_sql_query(consulta, con=servidor)
        print('Adicionando dados na tabela...')
        return tabela

    def remover_dados(self, id_tecnica, hub, data):
        servidor = self.bi
        con = servidor.connect() 
        sql = f"""
        DELETE FROM workstation.slots_abertos
        WHERE hub = '{hub}' and data = '{data}' and id_tecnica = {id_tecnica}
        """
        try:
            trans = con.begin()
            con.execute(sql)
            trans.commit()
        except:
            trans.rollback()
        return print('Dados da tabela workstation deletados com sucesso.')

    def atualizar_dados_tecnica(self, id_tecnica, nome_tecnica, id_tecnica_substituida, nome_tecnica_substituida, data, hub):
        servidor = self.bi
        con = servidor.connect() 
        sql = f"""
        UPDATE workstation.slots_abertos  
        SET "id_tecnica" ='{id_tecnica}',"tecnica" ='{nome_tecnica}'
        WHERE "id_tecnica" ='{id_tecnica_substituida}' 
        AND "data"='{data}'
        AND "hub"='{hub}'
        """
        try:
            trans = con.begin()
            con.execute(sql)
            trans.commit()
        except:
            trans.rollback()
        return print('Tecnica ' + nome_tecnica + ' foi colocada no lugar da tecnica ' + nome_tecnica_substituida + ' com sucesso.')

    def enviar_email(self, corpo_email, assunto):
        msg = email.message.Message()
        msg['Subject'] = assunto
        msg['From'] = 'operacoes@beepsaude.com.br'
        password = '123mudar!'
        msg.add_header('Content-Type', 'text/html')
        msg.set_payload(corpo_email)
        s = smtplib.SMTP('smtp.gmail.com: 587')
        s.starttls()
        s.login(msg['From'], password)
        s.sendmail(msg['From'], ['juan.melo@beepsaude.com.br','contato@beepsaude.com.br','planejamento.demanda@beepsaude.com.br'], msg.as_string().encode('utf-8'))
        print('Email enviado com sucesso.')
