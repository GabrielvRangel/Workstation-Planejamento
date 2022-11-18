from sqlite3 import Timestamp
from sqlalchemy import Time, create_engine
import pandas as pd
import datetime
import requests
import os
import json
import sqlalchemy
from datetime import date, datetime, timedelta

class Dashboard():    
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
        self.serverproduction = create_engine(f"""postgresql://awsuser:72Fk2m1Jx08i@beep-server-production-replica-02.coxxaz1blvi6.us-east-1.rds.amazonaws.com/beep_server_production""")




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

dash = Dashboard()
agenda = Slots()


        

