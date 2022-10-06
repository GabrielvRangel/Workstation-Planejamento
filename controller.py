import os
from flask import Flask, render_template, request
from sqlalchemy import Time, create_engine
import pandas as pd
import model

app = Flask(__name__)

@app.route("/")
def index():    
    dash = model.Dashboard()
    regiões = dash.opçãodefiltroregião()
    bus = dash.opçãodefiltrobu()
    return render_template("index.html", regiões = regiões, bus = bus)

@app.route("/filtrar", methods=["GET","POST"])
def filtrar():
    date = request.args.get('date')
    região = request.args.get('região')
    bu = request.args.get('bu')
    dash = model.Dashboard()
    regiões = dash.opçãodefiltroregião()
    bus = dash.opçãodefiltrobu()    
    dash.tratarescala()
    if not date or região == "Escolha a região" or bu == "Escolha o produto":
        escala = dash.filtrarescala(date, região, bu)
        escalaheading = list(escala)
        escalaregião = list(escala['região']) 
        escalahub = list(escala['hub'])
        escalaescala = list(escala['escala'])
        escaladata = list(escala['data'])
        escalaid_colaborador = list(escala['id_colaborador'])
        escalacolaborador = list(escala['colaborador'])
        escalaid_cargo = list(escala['id_cargo'])
        escaladatainicio = list(escala['data_inicio_previsto'])
        escaladatafim = list(escala['data_fim_previsto'])
        escalaausente = list(escala['ausente_lancamento'])
        escalabu = list(escala['bu'])
    else:
        escala = dash.filtrarescala(date, região, bu)
        escalaheading = list(escala)
        escalaregião = list(escala['região']) 
        escalahub = list(escala['hub'])
        escalaescala = list(escala['escala'])
        escaladata = list(escala['data'])
        escalaid_colaborador = list(escala['id_colaborador'])
        escalacolaborador = list(escala['colaborador'])
        escalaid_cargo = list(escala['id_cargo'])
        escaladatainicio = list(escala['data_inicio_previsto'])
        escaladatafim = list(escala['data_fim_previsto'])
        escalaausente = list(escala['ausente_lancamento'])
        escalabu = list(escala['bu'])
    return  render_template("index.html", escalaheading=escalaheading, escalaregião=escalaregião, escalahub=escalahub, escalaescala=escalaescala, escaladata=escaladata, escalaid_colaborador=escalaid_colaborador, escalacolaborador=escalacolaborador, escalaid_cargo=escalaid_cargo, escaladatainicio=escaladatainicio, escaladatafim=escaladatafim, escalaausente=escalaausente, escalabu=escalabu, regiões = regiões, bus = bus)
        
if __name__ == "__main__":
    app.run(debug= True)