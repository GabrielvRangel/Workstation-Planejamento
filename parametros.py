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
