import os
from flask import Flask, render_template, request
from sqlalchemy import Time, create_engine
import pandas as pd
import model

app = Flask(__name__)
dash = model.Dashboard()
regiões = dash.opçãodefiltroregião()
bus = dash.opçãodefiltrobu()

@app.route("/")
def index():    
    regiões = dash.opçãodefiltroregião()
    bus = dash.opçãodefiltrobu()
    return render_template("index.html", regiões = regiões, bus = bus)

@app.route("/filtrar", methods=["GET","POST"])
def filtrar():
    date = request.args.get('date')
    região = request.args.get('região')
    bu = request.args.get('bu')
    dash.tratarescala()
    #ação depois de filtrar
    if not date or região == "Escolha a região" or bu == "Escolha o produto":
        return render_template("index.html", regiões= regiões, bus= bus)
    else:
        prioridade = dash.tratarfiltrarprioridade(date, região, bu)
        prioridadeheading = list(prioridade)
        prioridaderegião = list(prioridade['região'])
        prioridadehub = list(prioridade['hub'])
        prioridadeárea = list(prioridade['área'])
        prioridadedate1 = list(prioridade[str(dash.somardata(date, 0))])
        prioridadedate2 = list(prioridade[str(dash.somardata(date, 1))])
        prioridadedate3 = list(prioridade[str(dash.somardata(date, 2))])
        prioridadedate4 = list(prioridade[str(dash.somardata(date, 3))])
        prioridadedate5 = list(prioridade[str(dash.somardata(date, 4))])
        prioridadedate6 = list(prioridade[str(dash.somardata(date, 5))])
        prioridadedate7 = list(prioridade[str(dash.somardata(date, 6))])
        prioridadedate8 = list(prioridade[str(dash.somardata(date, 7))])
        prioridadedate9 = list(prioridade[str(dash.somardata(date, 8))])
        prioridadedate10 = list(prioridade[str(dash.somardata(date, 9))])

        escala = dash.filtrarescala(date, região, bu)
        escalaheading = list(escala)
        escalaregião = list(escala['região']) 
        escalahub = list(escala['hub'])
        escalaescala = list(escala['escala'])
        escaladata = list(escala['data'].astype(str))
        escalaid_colaborador = list(escala['id_colaborador'])
        escalacolaborador = list(escala['colaborador'])
        escalaid_cargo = list(escala['id_cargo'])
        escaladatainicio = list(escala['data_inicio'])
        escaladatafim = list(escala['data_fim'])
        escalaausente = list(escala['ausente_lancamento'])
        escalabu = list(escala['bu'])
        return  render_template("index.html", prioridadeheading=prioridadeheading, prioridaderegião=prioridaderegião, prioridadeárea=prioridadeárea, prioridadehub=prioridadehub, prioridadedate1=prioridadedate1, prioridadedate2=prioridadedate2, prioridadedate3=prioridadedate3,
        prioridadedate4=prioridadedate4, prioridadedate5=prioridadedate5, prioridadedate6=prioridadedate6, prioridadedate7=prioridadedate7, 
        prioridadedate8=prioridadedate8, prioridadedate9=prioridadedate9, prioridadedate10=prioridadedate10,
        escalaheading=escalaheading, escalaregião=escalaregião, escalahub=escalahub, escalaescala=escalaescala, 
        escaladata=escaladata, escalaid_colaborador=escalaid_colaborador, escalacolaborador=escalacolaborador, escalaid_cargo=escalaid_cargo, 
        escaladatainicio=escaladatainicio, escaladatafim=escaladatafim, escalaausente=escalaausente, escalabu=escalabu, regiões = regiões, bus = bus)

@app.route("/abrirslots")
def abrirslots():    
    data = request.args.get('col')
    área = request.args.get('lin')
    tabela = request.args.get('cel')
    print('////')
    print(data)
    print('////')
    print(área)
    print('////')
    print(tabela)
    print('////')
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug= True)