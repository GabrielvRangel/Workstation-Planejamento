import requests
import pandas as pd
import io
import json
token = '8b03c0e78612ff675b14551d9e5c3c765740ffbc'
url = f'https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings?session_token={token}'

payload = {
    "bookings": [
        {
            "date": "2022-12-28", # Data da agenda
            "work_shift": "diarist", # tipo de regime (diarist = Diarista | rotating = Plantonista)
            "product_type": "vaccines", # Tipo de produto da agenda (vaccines = Vacinas | laboratories = Exames) 
            "supplier_id": 600, # ID da região onde a agenda será alocada
            "slots": [
                { "time": "08:00", "supplier_id": 600, "duration": 40 }
            ] # lista de slots seguindo a estrutura, Opcional
        }
        ]
    }

response = requests.post(url=url, json=payload)  

dados = json.loads(response.text)
dados = dados[0]
dados = dados['slots']
dados = dados[0]
dados = dados['id']

print(dados)