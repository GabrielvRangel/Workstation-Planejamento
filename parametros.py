import datetime

class Parametros_internos():
    def retornar_bu(self, bu, base):
        if (bu == 'vaccines' and base == 'escala app'): bu = '%VAC%'
        if (bu == 'laboratories' and base == 'escala app'): bu = '%LAB%'
        if (bu == 'vaccines' and base == 'beep'): bu = 'Imunizações'
        if (bu == 'laboratories' and base == 'beep'): bu = 'Lab'
        return bu

    def retornar_regime(self, escala):
        if (escala == 'VAC Técnica P1') or (escala == 'VAC Técnica P2') or (escala == 'LAB Técnica P1') or (escala == 'LAB Técnica P2'):
            regime = 'rotating'
        elif (escala == 'VAC Técnica D') or (escala == 'LAB Técnica D'):
            regime = 'diarist'
        return regime

    def retornar_data_somada(self, data, dias):
        soma = datetime.datetime.strptime(data, '%Y-%m-%d') + datetime.timedelta(days=dias)
        return soma.strftime("%Y-%m-%d")

    def retornar_hubs_permitidos_transferir_agendas(self, hub):
        if hub == 'São Cristóvão':
            hubs = ['São Cristóvão', 'Barra'] 
        if hub == 'Barra':
            hubs = ['Barra', 'São Cristóvão']
        if hub == 'Alphaville':
            hubs = ['Alphaville', 'Vila Olímpia']
        if hub == 'Tatuapé':
            hubs = ['Tatuapé', 'São Bernardo do Campo', 'Vila Olímpia']
        if hub == 'São Bernardo do Campo':
            hubs = ['São Bernardo do Campo', 'Tatuapé', 'Vila Olímpia']
        if hub == 'Vila Olímpia':
            hubs = ['Vila Olímpia', 'Alphaville', 'Tatuapé', 'São Bernardo do Campo']
        if hub == 'Campinas':
            hubs = ['Campinas']
        if hub == 'Cabo Frio':
            hubs = ['Cabo Frio']
        if hub == 'Brasília':
            hubs = ['Brasília']
        if hub == 'Curitiba':
            hubs = ['Curitiba']
        if hub == 'Recife':
            hubs = ['Recife']
        return hubs



# Parametros_internos().retornar_hub_regiao('Alphaville')



