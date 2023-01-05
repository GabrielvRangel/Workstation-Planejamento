import os
from flask import Flask, render_template, request, redirect
from sqlalchemy import Time, create_engine
import pandas as pd
import model
import parametros
from datetime import date, timedelta

app = Flask(__name__)

Agenda = model.Agenda()
Area = model.Area()
Dashboard = model.Dashboard()
Parametros = parametros.Parametros_internos()
Slots = model.Slots()
lista_macro_regiao = Dashboard.retorna_lista_macro_regiao()
lista_bu = Dashboard.retorna_lista_bu()

@app.route("/")
def index():
    return render_template("index.html", regiões = lista_macro_regiao, bus = lista_bu)

@app.route("/filtrar", methods=["GET","POST"])
def filtrar():
    date = request.args.get('date')
    regiao = request.args.get('região')
    bu = request.args.get('bu')
    if not date or regiao == "Escolha a região" or bu == "Escolha o produto":
        return render_template("index.html", regiões= lista_macro_regiao, bus= lista_bu)
    capacidade = Agenda.retornar_tabela_quantidade_agenda_disponivel_ocupado(date, regiao, bu)
    capacidade_heading = list(capacidade)
    capacidade_status = list(capacidade['status'])
    capacidade_data_d1 = list(capacidade[str(Parametros.retornar_data_somada(date, 0))])
    capacidade_data_d2 = list(capacidade[str(Parametros.retornar_data_somada(date, 1))])
    capacidade_data_d3 = list(capacidade[str(Parametros.retornar_data_somada(date, 2))])
    capacidade_data_d4 = list(capacidade[str(Parametros.retornar_data_somada(date, 3))])
    capacidade_data_d5 = list(capacidade[str(Parametros.retornar_data_somada(date, 4))])
    capacidade_data_d6 = list(capacidade[str(Parametros.retornar_data_somada(date, 5))])
    capacidade_data_d7 = list(capacidade[str(Parametros.retornar_data_somada(date, 6))])
    capacidade_data_d8 = list(capacidade[str(Parametros.retornar_data_somada(date, 7))])
    capacidade_data_d9 = list(capacidade[str(Parametros.retornar_data_somada(date, 8))])
    capacidade_data_d10 = list(capacidade[str(Parametros.retornar_data_somada(date, 9))])
    prioridade = Area.retorna_tabela_prioridade_score_areas(date, regiao, bu)
    prioridade_heading = list(prioridade)
    prioridade_regiao = list(prioridade['região'])
    prioridade_hub = list(prioridade['hub'])
    prioridade_area = list(prioridade['área'])
    prioridade_data_d1 = list(prioridade[str(Parametros.retornar_data_somada(date, 0))])
    prioridade_data_d2 = list(prioridade[str(Parametros.retornar_data_somada(date, 1))])
    prioridade_data_d3 = list(prioridade[str(Parametros.retornar_data_somada(date, 2))])
    prioridade_data_d4 = list(prioridade[str(Parametros.retornar_data_somada(date, 3))])
    prioridade_data_d5 = list(prioridade[str(Parametros.retornar_data_somada(date, 4))])
    prioridade_data_d6 = list(prioridade[str(Parametros.retornar_data_somada(date, 5))])
    prioridade_data_d7 = list(prioridade[str(Parametros.retornar_data_somada(date, 6))])
    prioridade_data_d8 = list(prioridade[str(Parametros.retornar_data_somada(date, 7))])
    prioridade_data_d9 = list(prioridade[str(Parametros.retornar_data_somada(date, 8))])
    prioridade_data_d10 = list(prioridade[str(Parametros.retornar_data_somada(date, 9))])
    escala = Agenda.retornar_tabela_agendas_regiao(date, regiao, bu)
    escala_heading = list(escala)
    escala_regiao = list(escala['região']) 
    escala_hub = list(escala['hub'])
    escala_escala = list(escala['escala'])
    escala_data = list(escala['data'].astype(str))
    escala_id_tecnica = list(escala['id_técnica'])
    escala_tecnica = list(escala['técnica'])
    escala_hora_entrada = list(escala['hr_entrada'])
    escala_hora_saida = list(escala['hr_saída'])
    escala_area = list(escala['área'])
    escala_bu = list(escala['bu'])
    escala_status = list(escala['status'])
    return  render_template("index.html", prioridadeheading=prioridade_heading, prioridaderegião=prioridade_regiao, prioridadeárea=prioridade_area, prioridadehub=prioridade_hub, 
    prioridadedate1=prioridade_data_d1, prioridadedate2=prioridade_data_d2, prioridadedate3=prioridade_data_d3, prioridadedate4=prioridade_data_d4, prioridadedate5=prioridade_data_d5,
    prioridadedate6=prioridade_data_d6, prioridadedate7=prioridade_data_d7, prioridadedate8=prioridade_data_d8, prioridadedate9=prioridade_data_d9, prioridadedate10=prioridade_data_d10,
    escalaheading=escala_heading, escalaregião=escala_regiao, escalahub=escala_hub, escalaescala=escala_escala, escaladata=escala_data, escalaid_técnica=escala_id_tecnica, 
    escalatécnica=escala_tecnica, escalahrentrada=escala_hora_entrada, escalahrsaída=escala_hora_saida, escalaárea=escala_area, escalabu=escala_bu, escalastatus=escala_status, 
    regiões = lista_macro_regiao, bus = lista_bu, capacidadeheading=capacidade_heading, capacidadestatus=capacidade_status, capacidadedate1=capacidade_data_d1, capacidadedate2=capacidade_data_d2, 
    capacidadedate3=capacidade_data_d3, capacidadedate4=capacidade_data_d4, capacidadedate5=capacidade_data_d5, capacidadedate6=capacidade_data_d6, capacidadedate7=capacidade_data_d7, 
    capacidadedate8=capacidade_data_d8, capacidadedate9=capacidade_data_d9, capacidadedate10=capacidade_data_d10)

@app.route("/abrirslots")
def abrirslots():   
    data = request.args.get('col')
    area = request.args.get('lin')
    id_tecnica = request.args.get('id_técnica')
    tecnica = request.args.get('técnica')
    regime = request.args.get('regime')
    inicio_regime = request.args.get('inicioregime')
    fim_regime = request.args.get('fimregime')
    hub = request.args.get('hub')
    hub_origem = request.args.get('hub_origem')
    duracao = int(request.args.get('duração'))
    regime = Parametros.retornar_regime(regime)
    id_parceiro = Area.retorna_id_parceiro(area)
    if area.find("LAB") != -1:
        produto = 'laboratories'
    if area.find("VAC") != -1:
        produto = 'vaccines'
    print('próximo de abrir agenda...')
    Agenda.registrar_agenda(data, produto, id_parceiro, area, hub, hub_origem, duracao, id_tecnica, tecnica, regime, inicio_regime, fim_regime)
    return redirect("https://workstation-planejamento.herokuapp.com/", code=302)


@app.route("/fechar_agenda")
def fechar_agenda():   
    token = Slots.retornar_token()
    id_agenda = int(request.args.get('id_agenda'))
    id_tecnica = int(request.args.get('id_tecnica'))
    nome_tecnica = request.args.get('nome_tecnica') 
    hub = request.args.get('hub')
    data = request.args.get('data')
    Slots.fechar_slots(id_agenda, token, id_tecnica, nome_tecnica, hub, data)
    return "Slots da técnica " + nome_tecnica + " do hub " + hub + " na data " + data + " foram deletados com sucesso!"

# @app.route("/abrirslotsminimos", methods=["GET","POST"])
# def abrirslotsminimos():
#     hub = request.args.get('hub') 
#     bu = request.args.get('bu')
#     duração = int(request.args.get('duração'))
#     classificação_min = int(request.args.get('classificação_min'))
#     classificação_max = int(request.args.get('classificação_max'))
#     verificação_range_dias = int(request.args.get('verificação_range_dias'))
#     aberturaautomatica(f'{hub}', f'{bu}', 60, verificação_range_dias, classificação_min, classificação_max, duração)
#     return 'Agendas abertas com sucesso.'

# @app.route("/abrirslotssobdemanda", methods=["GET","POST"])
# def abrirslotssobdemanda():
#     hub = request.args.get('hub')
#     bu = request.args.get('bu')
#     duração = int(request.args.get('duração'))
#     dias = int(request.args.get('dias'))
#     ocupação = float(request.args.get('ocupação'))
#     removerduplicado = int(request.args.get('removerduplicado'))
#     aberturaautomaticasobdemanda(f'{hub}', f'{bu}', dias, ocupação, duração, removerduplicado)
#     return 'Agendas abertas com sucesso.'

if __name__ == "__main__":
    app.run(debug= True)