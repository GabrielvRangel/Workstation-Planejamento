from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import datetime
import requests
import os
class Agenda():
    def __init__(self, hub, nome, regime, horárioinicial, horáriofinal, bu, alocação_disponível, data):
        self.hub = hub
        self.nome = nome
        self.regime = regime
        self.horárioinicial = horárioinicial
        self.horáriofinal = horáriofinal
        self.bu = bu
        self.alocação_disponível = alocação_disponível
        self.data = data
        
    def filtraragenda():
        print("filtra uma agenda")
    def verificardisponibilidade():
        print("verificando se a alocação está disponível")
    def escolherparceiro():
        print("Selecionar parceiro para abrir os slots")
    def alocaragenda():
        print("abrindo slots para agenda x filtrada")

class Dashboard():    
    def __init__(self):
        usuario =  os.environ['usuario']
        senha =  os.environ['senha']
        servidor =  os.environ['servidor']
        banco =  os.environ['banco']
        self.conexão = create_engine(f'postgresql://{usuario}:{senha}@{servidor}/{banco}')
    
    def tratarcards():
        print("Tratando os cards")
    
    def tratarfiltrarprioridade(self, data, região, bu):
        consulta = f"""
        select macro_região as região, a.hub, a.área, max(a.score) as score, a.data from ( 
        select "HUB" as hub, 
        case when "BU da Agenda" = 'Imunizações' then 'vaccines' else 'laboratories' end as bu, 
        SUBSTRING("Score"::text from 1 for 5)::numeric as score, "Área" as área, "Taxa de Ocupação Simulada" as ocupação, "Data da Agenda" as data 
        from last_mile.sugestoes_alocacao
        where "Turno da Agenda" = 'Manhã') a
        left join dim_parceiros
        on "HUB" = a.hub
        where data >= '{data}' and data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        and macro_região = '{região}'
        and a.bu = '{bu}'
        group by macro_região, a.hub, a.bu, a.área, a.data
        order by a.data, a.hub, a.área
        """
        self.prioridadetratada = pd.read_sql_query(consulta, con=self.conexão)
        self.prioridadetratada = pd.pivot_table(self.prioridadetratada, index=["região", "hub", "área"], columns=["data"], values=["score"])
        self.prioridadetratada = self.prioridadetratada.set_axis(self.prioridadetratada.columns.tolist(), axis=1).reset_index()
        self.prioridadetratada.columns = ['região', 'hub', 'área', str(self.somardata(data, 0)), str(self.somardata(data, 1)), str(self.somardata(data, 2)), str(self.somardata(data, 3)), str(self.somardata(data, 4)), str(self.somardata(data, 5)), str(self.somardata(data, 6)), str(self.somardata(data, 7)), str(self.somardata(data, 8)), str(self.somardata(data, 9))] 
        return self.prioridadetratada

    def tratarcapacidade():
        print("Tratando tabela com capacidade, alocado e saldo.")
    
    def tratarescala(self):
        consulta = f"""
        select macro_região as região, hub, escala, data, id_colaborador, colaborador, id_cargo, data_inicio_previsto::time as data_inicio, data_fim_previsto::time as data_fim, ausente_lancamento,
        (case when escala LIKE '%VAC%' then 'vaccines' when escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu
        from jornadas_escala.escala_operacional
        left join dim_parceiros
        on "HUB" = hub
        where escala LIKE '%Técnica%'
        and previsto = 'Trabalho'
        and (lancamento = 'Licença médica' or lancamento = 'Folga hora' or lancamento is null)
        and data > current_date
        group by  macro_região, hub, escala, data::date, id_colaborador, colaborador, id_cargo, data_inicio_previsto, data_fim_previsto, ausente_lancamento
        order by data::date, hub, colaborador
        """
        self.escalatratada = pd.read_sql_query(consulta, con=self.conexão)
        return self.escalatratada

    def filtrarcards():
        print("filtrando cards")

    def filtrarcapacidade():
        print("Filtrando tabela com capacidade, alocado e saldo.")
    
    def filtrarescala(self, data, região, bu): 
        self.escalatratada['data'] = pd.to_datetime(self.escalatratada['data'])
        filtrarescala = self.escalatratada[(self.escalatratada['data'] >= f'{data}') & (self.escalatratada['data'] <= f'{str(self.somardata(data, 9))}') & (self.escalatratada['região'] == f'{região}') & (self.escalatratada['bu'] == f'{bu}')] 
        return filtrarescala

    def receberopçãodefiltro(self):
        print('filtrando tudo')
    
    def opçãodefiltroregião(self):
        consulta = f"select macro_região from dim_parceiros where macro_região is not null group by macro_região"
        opçãoregião = pd.read_sql_query(consulta, con=self.conexão)
        regiões = opçãoregião['macro_região'].values
        return regiões

    def opçãodefiltrobu(self):
        consulta = f"""select a.bu from (
        select (case when escala LIKE '%VAC%' then 'vaccines' when escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu
        from jornadas_escala.escala_operacional
        left join dim_parceiros
        on "HUB" = hub
        where escala LIKE '%Técnica%'
        and previsto = 'Trabalho'
        and (lancamento = 'Licença médica' or lancamento = 'Folga' or lancamento = 'Folga extra' or lancamento = 'Folga hora' or lancamento is null)
        and data > current_date
        order by data ) a group by a.bu
        """
        opçãobu = pd.read_sql_query(consulta, con=self.conexão)
        bus = opçãobu['bu'].values
        return bus

    def opçãodefiltrostatus(self):
        print("Mostrar se a capacidade está disponível ou alocada")

    def somardata(self, data, dias):
        soma = datetime.datetime.strptime(data, '%Y-%m-%d') + datetime.timedelta(days=dias)
        return soma.strftime("%Y-%m-%d")


# url = f'https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings?session_token={token}'
# payload = {
#     "bookings": [
#         {
#             "date": "2022-12-27", # Data da agenda
#             "work_shift": "diarist", # tipo de regime (diarist = Diarista | rotating = Plantonista)
#             "product_type": "vaccines", # Tipo de produto da agenda (vaccines = Vacinas | laboratories = Exames) 
#             "supplier_id": 634, # ID da região onde a agenda será alocada
#             "slots": [
#                 { "time": "08:00", "supplier_id": 634, "duration": 40 }
#             ] # lista de slots seguindo a estrutura, Opcional
#         }
#         ]
#     }

# response = requests.post(url=url, json=payload)
# print(response.text)    