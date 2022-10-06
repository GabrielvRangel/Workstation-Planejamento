from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd

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
    
    def tratarcards():
        print("Tratando os cards")
    
    def tratarprioridade():
        print("Tratando os dados prioridade")
    
    def tratarcapacidade():
        print("Tratando tabela com capacidade, alocado e saldo.")
    
    def tratarescala(self):
        consulta = f"""
        select macro_região as região, hub, escala, data::date, id_colaborador, colaborador, id_cargo, data_inicio_previsto, data_fim_previsto, ausente_lancamento,
        (case when escala LIKE '%VAC%' then 'vaccines' when escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu
        from jornadas_escala.escala_operacional
        left join dim_parceiros
        on "HUB" = hub
        where escala LIKE '%Técnica%'
        and previsto = 'Trabalho'
        and (lancamento = 'Licença médica' or lancamento = 'Folga' or lancamento = 'Folga extra' or lancamento = 'Folga hora' or lancamento is null)
        and data > current_date
        group by  macro_região, hub, escala, data, id_colaborador, colaborador, id_cargo, data_inicio_previsto, data_fim_previsto, ausente_lancamento
        order by data, hub, colaborador
        """
        self.escalatratada = pd.read_sql_query(consulta, con=self.conexão)
        return self.escalatratada

    def filtrarcards():
        print("filtrando cards")
    
    def filtrarprioridade():
        print("filtrando prioridade de abertura")
    
    def filtrarcapacidade():
        print("Filtrando tabela com capacidade, alocado e saldo.")
    
    def filtrarescala(self, data, região, bu): 
        self.escalatratada['data'] = pd.to_datetime(self.escalatratada['data'], format='%Y-%m-%d')
        filtrarescala = self.escalatratada[(self.escalatratada['data'] == f'{data}') & (self.escalatratada['região'] == f'{região}') & (self.escalatratada['bu'] == f'{bu}')] 
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