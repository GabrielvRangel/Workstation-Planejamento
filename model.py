from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import datetime
import requests
import os
import json
import sqlalchemy
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
        self.conexão = sqlalchemy.create_engine(f"""postgresql://{usuario}:{senha}@{servidor}/{banco}""", pool_pre_ping=True)
        self.serverproduction = sqlalchemy.create_engine(f"""postgresql://{sp_usuario}:{sp_senha}@{sp_servidor}/{sp_banco}""", pool_pre_ping=True)
       

    def tratarfiltrarprioridade(self, data, região, bu):
        if bu == 'vaccines': bu = 'Imunizações' 
        else: bu = 'Lab'
        consulta = f"""
        select macro_região as região, lmsa."HUB" as hub,"Área" as área, max(SUBSTRING("Score"::text from 1 for 5)::numeric) as score, "Data da Agenda" as data
        from last_mile.sugestoes_alocacao lmsa
        left join dim_parceiros dp
        on dp."HUB" = lmsa."HUB"
        where "Turno da Agenda" = 'Manhã'
        and "BU da Agenda" = '{bu}'
        and "Data da Agenda" >= '{data}' and "Data da Agenda" <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '4 days'
        and macro_região = '{região}'
        group by macro_região, lmsa."HUB", "Área", "Data da Agenda"
        order by "Data da Agenda", lmsa."HUB", "Área"
        """
        self.prioridadetratada = pd.read_sql_query(consulta, con=self.conexão)
        self.prioridadetratada = pd.pivot_table(self.prioridadetratada, index=["região", "hub", "área"], columns=["data"], values=["score"])
        self.prioridadetratada = self.prioridadetratada.set_axis(self.prioridadetratada.columns.tolist(), axis=1).reset_index()
        self.prioridadetratada.columns = ['região', 'hub', 'área', str(self.somardata(data, 0)), str(self.somardata(data, 1)), str(self.somardata(data, 2)), str(self.somardata(data, 3)), str(self.somardata(data, 4))] 
        return self.prioridadetratada

    def tratarfiltrarcapacidade(self, data, região, bu):
        if bu == 'vaccines': bu = '%VAC%' 
        else: bu = '%LAB%'
        consulta = f"""    
        select a.status, count(a.status) as quant, a.data from (
        select macro_região as região, jeeo.data, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica,
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
        and jeeo.data >= '{data}' and jeeo.data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '4 days'
        and jeeo.escala LIKE '{bu}'
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area"
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador ) a 
        group by a.status, a.data
        """
        self.capacidadetratada = pd.read_sql_query(consulta, con=self.conexão)
        self.capacidadetratada = pd.pivot_table(self.capacidadetratada, index=["status"], columns=["data"], values=["quant"])
        self.capacidadetratada = self.capacidadetratada.set_axis(self.capacidadetratada.columns.tolist(), axis=1).reset_index()
        self.capacidadetratada.columns = ['status', str(self.somardata(data, 0)), str(self.somardata(data, 1)), str(self.somardata(data, 2)), str(self.somardata(data, 3)), str(self.somardata(data, 4))] 
        return self.capacidadetratada

    def tratarfiltrarescala(self, data, região, bu):
        if bu == 'vaccines': bu = '%VAC%' 
        else: bu = '%LAB%'
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
        and jeeo.escala LIKE '{bu}'
        and macro_região = '{região}'
        and jeeo.data >= '{data}' and jeeo.data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '4 days'
        and (lancamento <> 'Afastamento INSS' and lancamento <> 'Treinamento' and lancamento <> 'Licença maternidade' and lancamento <> 'Curso/Evento' and lancamento <> 'Férias' and lancamento <> 'Recesso' and lancamento <> 'Licença nojo/óbito' and lancamento <> 'Atividade administrativa' and lancamento <> 'Folga' and lancamento <> 'Folga extra' and lancamento <> 'Licença gala' or lancamento is null)
        and jeeo.data > current_date
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area"
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador
        """
        self.escalatratada = pd.read_sql_query(consulta, con=self.conexão)
        return self.escalatratada
            
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

