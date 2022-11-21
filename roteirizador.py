from sqlite3 import Timestamp
from telnetlib import theNULL
import pandas as pd
import psycopg2 as pg
from sqlalchemy import Time, create_engine
import googlemaps
import json
import datetime
from branca.element import Figure
import os

class Banco_de_dados:
    def __init__(self):
        # usuario =  os.environ['usuario']
        # senha =  os.environ['senha']
        # servidor =  os.environ['servidor']
        # banco =  os.environ['banco']
        # sp_usuario =  os.environ['sp_usuario']
        # sp_senha =  os.environ['sp_senha']
        # sp_servidor =  os.environ['sp_servidor']
        # sp_banco =  os.environ['sp_banco']
        # self.conexão = sqlalchemy.create_engine(f"""postgresql://{usuario}:{senha}@{servidor}/{banco}""", pool_pre_ping=True)
        # self.serverproduction = sqlalchemy.create_engine(f"""postgresql://{sp_usuario}:{sp_senha}@{sp_servidor}/{sp_banco}""", pool_pre_ping=True)
        self.conexão = create_engine(f"""postgresql://Logistica:beep%40saude@tableau-bi.coxxaz1blvi6.us-east-1.rds.amazonaws.com/beepsaude""")
     
    def consultar_tabela_slot(self, data, hub, produto):
        if produto == 'vaccines':
            produto = 'VAC'
        if produto == 'laboratories':
            produto = 'LAB'

        sql = f"""
        select vouchers as voucher, data_agendamento, hr_agendamento::time, hub, parceiro_nome, tipo_produto as product_type, nome_comprador, tel_comprador, endereco as endereço, latitude, longitude, latitude + longitude as chave, num_slots
        from last_mile.fct_agendamentos_para_roteirizacao_2 
        where data_agendamento = '{data}'
        and hub = '{hub}' 
        and ( SUBSTRING(parceiro_nome FROM 1 FOR 3) = '{produto}' or SUBSTRING(parceiro_nome FROM 10 FOR 3) = '{produto}' )
        order by data_agendamento, parceiro_nome, hr_agendamento
        """
        road = pd.read_sql_query(sql, con=self.conexão)
        #Substituindo o array de vouchers por número 0 para ficar em formato numerico
        road['voucher'] = road['voucher'].astype(str).str.replace('[','0').str.replace(']','0').str.replace(',','0').str.replace(' ','0') 
        return road
    
    def consulta_tabela_tecnica(self, data, hub, produto):
        sql = f"""
        select a.tecnica, a."Regime_Técnica", a.bu, a.area, a.data, jeeo.hub as "hub_origem", a.hub as "hub_destino", data_inicio_previsto::time as "inicio", data_fim_previsto::time as "fim", lancamento as "observação" from ( select tecnica, hub,
        CASE
            WHEN regime = 'rotating' then 'Plantonista'
            WHEN regime = 'diarist' then 'Diarista'
            END AS "Regime_Técnica",
            
        CASE
            WHEN LOWER("area") like '%vac%' then 'Vacinas'
            WHEN LOWER("area") like '%lab%' then 'Laboratório'
            END AS "bu",
        CASE
            WHEN LOWER("area") like '%vac%' then 'vaccines,vacina,vacinas,vac,vaccine'
            WHEN LOWER("area") like '%lab%' then 'laboratories,laboratorio,laboratório,lab,laboratóries,laboratory'
            END AS "bufiltro",
        "area", data
        from workstation.slots_abertos ) a
        left join jornadas_escala.escala_operacional jeeo
        on concat(a.data::text,a.tecnica) = concat(jeeo.data::text, jeeo.colaborador)
        where 1=1 
        and LOWER(a.hub) = LOWER('{hub}')
        and a.data = '{data}'
        and LOWER(a."bufiltro") like concat('%',LOWER('{produto}'),'%')
        group by a.tecnica, a."Regime_Técnica", a.bu, a.area, a.data, a.hub, jeeo.hub, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, jeeo.lancamento
        order by a.data, a.hub, a.bu, a.tecnica, a."Regime_Técnica", a.area
        """
        escala = pd.read_sql_query(sql, con=self.conexão)
        return escala

class Roteirizar:
    def __init__(self,data, hub, produto, tempo_atendimento):
        self.data = data
        self.hub = hub
        self.produto = produto
        self.tempo_atendimento = tempo_atendimento
        self.api_google_maps = googlemaps.Client(key='AIzaSyC8qP_rQDA-yo7TgbW5_ZbWfwxgu8yZ1JE')
        self.bloqueio = 2
        self.horario_minimo_bloqueio_slots = datetime.timedelta(minutes=1)
        self.horario_maximo_bloqueio_slots = datetime.timedelta(minutes=int(self.tempo_atendimento))
        self.inicio_plantonista_6h = 'Jan 1 0001 06:30AM' #Primeiro slot plantonista 6 - 18
        self.fim_plantonista_6h = 'Jan 1 0001 05:00PM' #Ultimo slot plantonista 6 - 18
        self.inicio_plantonista_7h = 'Jan 1 0001 07:50AM' #Primeiro slot plantonista 7 - 19
        self.fim_plantonista_7h = 'Jan 1 0001 06:20PM' #Ultimo slot plantonista 7 - 19
        self.inicio_diarista_8h = 'Jan 1 0001 08:30AM' #Primeiro slot diarista 8 - 14
        self.fim_diarista_8h = 'Jan 1 0001 01:10PM' #Ultimo slot diarista 8 - 14
        self.inicio_diarista_6h = 'Jan 1 0001 06:30AM' #Primeiro slot diarista 6 - 12
        self.fim_diarista_6h  = 'Jan 1 0001 11:10AM' #Ultimo slot diarista 6 - 12
        self.inicio_diarista_7h = 'Jan 1 0001 07:50AM' #Primeiro slot diarista 7 - 13
        self.fim_diarista_7h = 'Jan 1 0001 12:10PM' #Ultimo slot diarista 7 - 13

banco = Banco_de_dados()
print(banco.consulta_tabela_tecnica('2022-11-18','São Cristóvão','vaccines'))
# print(banco.consultar_tabela_slot('2022-11-18','São Cristóvão','vaccines'))