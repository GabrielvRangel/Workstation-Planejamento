from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import os

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