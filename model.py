from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import datetime
import requests
import os
import json
import sqlalchemy
class Slots():
    def abrirslots(self, data, regime, bu, idparceiro, urltoken, slotsdaagenda, área, hub, id_tecnica, tecnica):    
        payload = {
            "bookings": [
                {
                    "date": f"{data}", # Data da agenda
                    "work_shift": f"{regime}", # tipo de regime (diarist = Diarista | rotating = Plantonista)
                    "product_type": f"{bu}", # Tipo de produto da agenda (vaccines = Vacinas | laboratories = Exames) 
                    "supplier_id": idparceiro, # ID da região onde a agenda será alocada
                    "slots": slotsdaagenda # lista de slots seguindo a estrutura, Opcional
                }
            ]
        }

        response = requests.post(url=urltoken, json=payload)
        self.dados = json.loads(response.text)
        self.dados2 = self.dados[0]
        self.ids_dos_slots_da_agenda = self.dados2['slots']
        quantidade_slots_matriz = len(self.ids_dos_slots_da_agenda)
        tabela_slots_da_agenda = []
        while quantidade_slots_matriz > 0:
            self.selecionar_agenda = self.ids_dos_slots_da_agenda[quantidade_slots_matriz - 1]
            self.slotid = self.selecionar_agenda['id'] 
            self.horario = self.selecionar_agenda['date'] 
            tabela_slots_da_agenda.append({'id_slot':self.slotid,'data': data,'horario': str(self.horario),'area': área,'hub': hub,'regime': regime,'produto': bu,'id_tecnica': id_tecnica,'tecnica': tecnica})
            quantidade_slots_matriz = quantidade_slots_matriz - 1
        dash = Dashboard()
        dash.inserirdados(tabela_slots_da_agenda)
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
        # self.conexão = create_engine(f"""postgresql://Logistica:beep%40saude@tableau-bi.coxxaz1blvi6.us-east-1.rds.amazonaws.com/beepsaude""")
        # self.serverproduction = create_engine(f"""postgresql://awsuser:72Fk2m1Jx08i@beep-server-production-replica-02.coxxaz1blvi6.us-east-1.rds.amazonaws.com/beep_server_production""")

    def áreasabertura(self, hub, bu, classificaçãoinicial, classificaçãofinal):
        if (bu == 'vaccines'):
            bu_nome_sinergia = 'VAC'
        else:
            bu_nome_sinergia = 'LAB'

        consulta = f"""
        select b.id_parceiro, b.hub, b.bu, b.área, b.classificação, b."nome_sinergia", b.id_sinergia from (
        select a.id_parceiro, a.hub, a.bu, a.área, case when a.área like '%Domingo%' then 7 when "nome_sinergia" like '%Domingo%' then 7 else a.classificação end as classificação, 
        case when ssa."nome_sinergia" is null then área when ssa."nome_sinergia" like '%desativado%' then área else ssa."nome_sinergia" end as "nome_sinergia" ,
        case when ssa.id_sinergia is null then 0 when ssa."nome_sinergia" like '%desativado%' then id_area else ssa.id_sinergia end as id_sinergia from (
        select id_sinergia::text as id_parceiro, "HUB" as hub, parceiro_tipo as bu, nome_sinergia as área, SUBSTRING("Categoria Sinergia" from 1 for 1)::int  as classificação from last_mile.analise_sinergias
        union
        select id_parceiro::text as id_parceiro, "HUB" as hub, parceiro_tipo as bu, parceiro_nome as área, SUBSTRING("Classificação" from 1 for 1)::int as classificação from last_mile.analise_areas ) a
        left join staging.sinergia_e_areas ssa
        on ssa.nome_area = a.área
        where a.hub = '{hub}' and a.bu = '{bu}' 
        and a.classificação >= 0 and a.classificação <= 6
        and a.área not like '%Global%' and a.área not like '%[%' and a.área not like '%]%' and a.área not like '%teste1%' and a.área not like '%[desativado]%' and a.área not like '%Beep%'
        group by a.id_parceiro, a.hub, a.bu, a.área, a.classificação, ssa.id_sinergia, "nome_sinergia", ssa.id_area
        order by a.área desc ) b
        where b."nome_sinergia" like '%{bu_nome_sinergia}%' and b."nome_sinergia" not like '%Global%' and b."nome_sinergia" not like '%[%' and b."nome_sinergia" not like '%]%' and b."nome_sinergia" not like '%teste1%' and b."nome_sinergia" not like '%[desativado]%' and b."nome_sinergia" not like '%Beep%'
        group by nome_sinergia, id_parceiro, hub, bu, área, classificação, id_sinergia
        """
        df = pd.read_sql_query(consulta, con=self.conexão)
        linhasdf = len(df)
        eixodf = 0
        df['status abertura'] = 0
        while linhasdf > eixodf:
            id_sinergia = str(df.iloc[eixodf]['id_sinergia'])
            quantidadeárea = len(df[df['id_parceiro'] == id_sinergia])
            valor_id_sinergia = str(df.iat[eixodf, 6])
            if (quantidadeárea == 0):
                df.iat[eixodf, 7] = 1
            elif quantidadeárea <= 1 and str(df.iat[eixodf, 0]) == valor_id_sinergia:
                df.iat[eixodf, 7] = 1
            else:
                df.iat[eixodf, 7] = 0
            eixodf = eixodf + 1
        df = df[df['status abertura'] == 1]
        df = df[(df['classificação'] >= classificaçãoinicial) &( df['classificação'] <= classificaçãofinal)]
        return df


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
        and "Data da Agenda" >= '{data}' and "Data da Agenda" <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        and macro_região = '{região}'
        group by macro_região, lmsa."HUB", "Área", "Data da Agenda"
        order by "Data da Agenda", lmsa."HUB", "Área"
        """
        self.prioridadetratada = pd.read_sql_query(consulta, con=self.conexão)
        self.prioridadetratada = pd.pivot_table(self.prioridadetratada, index=["região", "hub", "área"], columns=["data"], values=["score"])
        self.prioridadetratada = self.prioridadetratada.set_axis(self.prioridadetratada.columns.tolist(), axis=1).reset_index()
        self.prioridadetratada.columns = ['região', 'hub', 'área', str(self.somardata(data, 0)), str(self.somardata(data, 1)), str(self.somardata(data, 2)), str(self.somardata(data, 3)), str(self.somardata(data, 4)), str(self.somardata(data, 5)), str(self.somardata(data, 6)), str(self.somardata(data, 7)), str(self.somardata(data, 8)), str(self.somardata(data, 9))]
        return self.prioridadetratada

    def tratarfiltrarcapacidade(self, data, região, bu):
        if bu == 'vaccines': bu = '%VAC%' 
        else: bu = '%LAB%'
        consulta = f"""    
        select a.status, sum(case a.dataincompleta = data::date when true then 1 else 0 end) as quant, a.data::date from (
        select generate_series(DATE'{data}', to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days',INTERVAL'1 day') as data, macro_região as região, jeeo.data as dataincompleta, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica,
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
        and jeeo.data >= '{data}' and jeeo.data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        and jeeo.escala LIKE '{bu}'
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area") a 
        group by a.status, a.data
        order by status, a.data
        """
        self.capacidadetratada = pd.read_sql_query(consulta, con=self.conexão)
        self.capacidadetratada = pd.pivot_table(self.capacidadetratada, index=["status"], columns=["data"], values=["quant"])
        self.capacidadetratada = self.capacidadetratada.set_axis(self.capacidadetratada.columns.tolist(), axis=1).reset_index()
        self.capacidadetratada.columns = ['status', str(self.somardata(data, 0)), str(self.somardata(data, 1)), str(self.somardata(data, 2)), str(self.somardata(data, 3)), str(self.somardata(data, 4)), str(self.somardata(data, 5)), str(self.somardata(data, 6)), str(self.somardata(data, 7)), str(self.somardata(data, 8)), str(self.somardata(data, 9))] 
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
        and jeeo.data >= '{data}' and jeeo.data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        and (lancamento <> 'Afastamento INSS' and lancamento <> 'Treinamento' and lancamento <> 'Licença maternidade' and lancamento <> 'Curso/Evento' and lancamento <> 'Férias' and lancamento <> 'Recesso' and lancamento <> 'Licença nojo/óbito' and lancamento <> 'Atividade administrativa' and lancamento <> 'Folga' and lancamento <> 'Folga extra' and lancamento <> 'Licença gala' or lancamento is null)
        and jeeo.data > current_date
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area"
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador
        """
        self.escalatratada = pd.read_sql_query(consulta, con=self.conexão)
        return self.escalatratada
    
    def escalaautomatica(self, data, hub, bu, rangedias):
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
        and jeeo.hub = '{hub}'
        and jeeo.data >= '{data}' and jeeo.data <= '{rangedias}' 
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

    def inserirdados(self, tabela_slots_da_agenda):
        df = pd.DataFrame(tabela_slots_da_agenda, columns=['id_slot', 'data', 'horario', 'area', 'hub', 'regime', 'produto', 'id_tecnica', 'tecnica'])
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

    
    def filtrartaxaocupacao(self, hub, bu, data, taxaocupacao):
        if bu == 'vaccines': bu = '%VAC%' 
        else: bu = '%LAB%'
        consulta = f"""
        select a."HUB", a."slot_date", a."parceiro_nome", a."taxa_de_ocupacao", "Prioridade", "ID Área", "Taxa de Ocupação Simulada" from ( 
        select 
        dp."HUB"
        ,(spss.slot_date-interval '3 hours')::date as slot_date
        ,count(id) as total_slots
        ,sum(case when call_product_id is null then 0 else 1 end) as slots_ocupados
        ,sum(case when call_product_id is null then 0 else 1 end)/count(id)::numeric as taxa_de_ocupacao
        ,dp.parceiro_nome 
        ,dp.parceiro_ativo
        from sp_product_schedule_slots spss
        left join dim_parceiros dp 
        on dp.id_parceiro = spss.supplier_id 
        where slot_date::date = '{data}'
        and parceiro_nome not like '%desativado%'
        and parceiro_nome like '{bu}'
        and "HUB" = '{hub}'
        and spss.id not in (select * from stg_slots_bloqueados ssb)
        and dp.parceiro_ativo = true
        group by dp."HUB",(spss.slot_date-interval '3 hours')::date, dp.parceiro_tipo,dp.parceiro_nome,dp.parceiro_ativo) a
        left join last_mile.sugestoes_alocacao lmsa
        on concat(a."slot_date", a."parceiro_nome") = concat(lmsa."Data da Agenda",lmsa."Área")
        where lmsa."Turno da Agenda" = 'Manhã'
        order by "Prioridade" 
        """
        filtrartaxaocupacao = pd.read_sql_query(consulta, con=self.conexão)
        filtrartaxaocupacao = filtrartaxaocupacao[filtrartaxaocupacao['taxa_de_ocupacao'] >= taxaocupacao]
        return filtrartaxaocupacao

aberturaautomatica = Dashboard()
print(aberturaautomatica.áreasabertura('Vila Olímpia', 'vaccines', 0, 6))