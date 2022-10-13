from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import datetime
import requests
import os
import json
class Slots():
    def abrirslots(self, data, regime, bu, idparceiro, slot_hora, urltoken):    
        payload = {
            "bookings": [
                {
                    "date": f"{data}", # Data da agenda
                    "work_shift": f"{regime}", # tipo de regime (diarist = Diarista | rotating = Plantonista)
                    "product_type": f"{bu}", # Tipo de produto da agenda (vaccines = Vacinas | laboratories = Exames) 
                    "supplier_id": idparceiro, # ID da região onde a agenda será alocada
                    "slots": [
                        { "time": f"{slot_hora}", "supplier_id": idparceiro, "duration": 40 }
                    ] # lista de slots seguindo a estrutura, Opcional
                }
            ]
        }
        
        response = requests.post(url=urltoken, json=payload)
        self.dados = json.loads(response.text)
        self.dados2 = self.dados[0]
        self.dados3 = self.dados2['slots']
        self.dados4 = self.dados3[0]
        self.slotid = self.dados4['id']
    
    def iddoslot(self):
        return self.slotid

       
class Dashboard():    
    def __init__(self):
        usuario =  os.environ['usuario']
        senha =  os.environ['senha']
        servidor =  os.environ['servidor']
        banco =  os.environ['banco']
        sp_usuario =  os.environ['sp_usuario']
        sp_senha =  os.environ['sp_senha']
        sp_servidor =  os.environ['sp_servidor']
        sp_banco =  os.environ['sp_banco']
        self.conexão = create_engine(f"""postgresql://{usuario}:{senha}@{servidor}/{banco}""")
        self.serverproduction = create_engine(f"""postgresql://{sp_usuario}:{sp_senha}@{sp_servidor}/{sp_banco}""")

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

    def tratarfiltrarcapacidade(self, data, região, bu):
        consulta = f"""    
        select a.status, count(a.status) as quant, a.data from (
        select macro_região as região, jeeo.hub, jeeo.escala, jeeo.data, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica, jeeo.data_inicio_previsto::time as hr_entrada, jeeo.data_fim_previsto::time as hr_saída, 
        wsa."area" as área,
        (case when jeeo.escala LIKE '%VAC%' then 'vaccines' when jeeo.escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu,
        (case when wsa.tecnica is not null then 'Ocupado' else 'Disponível' end) as status
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and macro_região = '{região}'
        and previsto = 'Trabalho'
        and (lancamento <> 'Afastamento INSS' and lancamento <> 'Treinamento' and lancamento <> 'Licença maternidade' and lancamento <> 'Curso/Evento' and lancamento <> 'Férias' and lancamento <> 'Recesso' and lancamento <> 'Licença nojo/óbito' and lancamento <> 'Atividade administrativa' and lancamento <> 'Folga' and lancamento <> 'Folga extra' and lancamento <> 'Licença gala' or lancamento is null)
        and jeeo.data > current_date
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area"
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador ) a 
        where a.bu = '{bu}'
        and a.data >= '{data}' and a.data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        group by a.status, a.data
        """
        self.capacidadetratada = pd.read_sql_query(consulta, con=self.conexão)
        self.capacidadetratada = pd.pivot_table(self.capacidadetratada, index=["status"], columns=["data"], values=["quant"])
        self.capacidadetratada = self.capacidadetratada.set_axis(self.capacidadetratada.columns.tolist(), axis=1).reset_index()
        self.capacidadetratada.columns = ['status', str(self.somardata(data, 0)), str(self.somardata(data, 1)), str(self.somardata(data, 2)), str(self.somardata(data, 3)), str(self.somardata(data, 4)), str(self.somardata(data, 5)), str(self.somardata(data, 6)), str(self.somardata(data, 7)), str(self.somardata(data, 8)), str(self.somardata(data, 9))] 
        return self.capacidadetratada

    def tratarescala(self):
        consulta = f"""
        select macro_região as região, jeeo.hub, jeeo.escala, jeeo.data, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica, jeeo.data_inicio_previsto::time as hr_entrada, jeeo.data_fim_previsto::time as hr_saída, 
        wsa."area" as área,
        (case when jeeo.escala LIKE '%VAC%' then 'vaccines' when jeeo.escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu,
        (case when wsa.tecnica is not null then 'Ocupado' else 'Disponível' end) as status
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and previsto = 'Trabalho'
        and (lancamento <> 'Afastamento INSS' and lancamento <> 'Treinamento' and lancamento <> 'Licença maternidade' and lancamento <> 'Curso/Evento' and lancamento <> 'Férias' and lancamento <> 'Recesso' and lancamento <> 'Licença nojo/óbito' and lancamento <> 'Atividade administrativa' and lancamento <> 'Folga' and lancamento <> 'Folga extra' and lancamento <> 'Licença gala' or lancamento is null)
        and jeeo.data > current_date
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area"
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador
        """
        self.escalatratada = pd.read_sql_query(consulta, con=self.conexão)
        return self.escalatratada

    def filtrarescala(self, data, região, bu): 
        self.escalatratada['data'] = pd.to_datetime(self.escalatratada['data'])
        filtrarescala = self.escalatratada[(self.escalatratada['data'] >= f'{data}') & (self.escalatratada['data'] <= f'{str(self.somardata(data, 9))}') & (self.escalatratada['região'] == f'{região}') & (self.escalatratada['bu'] == f'{bu}')] 
        return filtrarescala
    
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

    def somardata(self, data, dias):
        soma = datetime.datetime.strptime(data, '%Y-%m-%d') + datetime.timedelta(days=dias)
        return soma.strftime("%Y-%m-%d")

    def idparceiro(self, parceiro):
        consulta = f"select parceiro_nome, id_parceiro from dim_parceiros"
        idparceiro = pd.read_sql_query(consulta, con=self.conexão)
        idparceiro = idparceiro[(idparceiro['parceiro_nome'] == f'{parceiro}')]
        idparceiro = int(idparceiro.iloc[0]['id_parceiro'])
        idparceiro = json.dumps(idparceiro)
        return idparceiro

    def duraçãoslot(self, hub, bu):
        if (hub == 'Brasília' and bu == 'vaccines') or (hub == 'São Cristóvão' and bu == 'vaccines') or (hub == 'Barra' and bu == 'vaccines') or (hub == 'Cabo Frio' and bu == 'vaccines') or (hub == 'Alphaville' and bu == 'vaccines') or (hub == 'Vila Olímpia' and bu == 'vaccines') or (hub == 'São Bernardo do Campo' and bu == 'vaccines') or (hub == 'Tatuapé' and bu == 'vaccines')  or (hub == 'Curitiba' and bu == 'vaccines') or (hub == 'Campinas' and bu == 'vaccines') or (hub == 'Recife' and bu == 'vaccines'):
            duração = 40
        elif  (hub == 'São Cristóvão' and bu == 'laboratories') or (hub == 'Barra' and bu == 'laboratories') or (hub == 'Alphaville' and bu == 'laboratories') or (hub == 'Vila Olímpia' and bu == 'laboratories') or (hub == 'São Bernardo do Campo' and bu == 'laboratories') or (hub == 'Tatuapé' and bu == 'laboratories'):
            duração = 30
        elif (hub == 'Vitória' and bu == 'vaccines') or (hub == 'Brasília' and bu == 'laboratories') or (hub == 'Vitória' and bu == 'laboratories') or (hub == 'Curitiba' and bu == 'laboratories') or (hub == 'Recife' and bu == 'laboratories') or (hub == 'Campinas' and bu == 'laboratories'):
            duração = 60
        return duração    

    def regime(self, escala):
        if (escala == 'VAC Técnica P1') or (escala == 'VAC Técnica P2') or (escala == 'LAB Técnica P1') or (escala == 'LAB Técnica P2'):
            regime = 'rotating'
        elif (escala == 'VAC Técnica D') or (escala == 'LAB Técnica D'):
            regime = 'diarist'
        return regime

    def inserirdados(self, idslot, data, horario, parceiro, hub, regime, produto, id_tecnica, tecnica):
        df = pd.DataFrame([[idslot, data, horario, parceiro, hub, regime, produto, id_tecnica, tecnica]], columns=['id_slot', 'data', 'horario', 'area', 'hub', 'regime', 'produto', 'id_tecnica', 'tecnica'])
        df.to_sql(con=self.conexão, name='slots_abertos', schema='workstation', if_exists='append', method='multi', index=False)
        consulta = f"select * from workstation.slots_abertos"
        tabela = pd.read_sql_query(consulta, con=self.conexão)
        print('Adicionando dados na tabela...')
        return tabela

    def token(self):
        consulta = f"""
        select remember_token from users where username = 'gabriel.rangel@beepsaude.com.br'
        """
        self.tkn = pd.read_sql_query(consulta, con=self.serverproduction)
        self.tkn = self.tkn.iloc[0]['remember_token']
        self.url = f'https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings?session_token={self.tkn}'
        return self.url

# dash = Dashboard()
# dash.tratarcapacidade('2022-10-10', 'Rio de Janeiro', 'vaccines')