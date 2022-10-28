import os
from flask import Flask, render_template, request, redirect
from sqlalchemy import Time, create_engine
import pandas as pd
import model
from datetime import date, datetime, timedelta

app = Flask(__name__)
dash = model.Dashboard()
agenda = model.Slots()
regiões = dash.opçãodefiltroregião()
bus = dash.opçãodefiltrobu()

@app.route("/")
def index():
    return render_template("index.html", regiões = regiões, bus = bus)

@app.route("/agendasautomaticas")
def agendasautomaticas():
    
    return 'Agendas automaticas verificadas.'

@app.route("/filtrar", methods=["GET","POST"])
def filtrar():
    date = request.args.get('date')
    região = request.args.get('região')
    bu = request.args.get('bu')
    #ação depois de filtrar
    if not date or região == "Escolha a região" or bu == "Escolha o produto":
        return render_template("index.html", regiões= regiões, bus= bus)
    else:
        capacidade = dash.tratarfiltrarcapacidade(date, região, bu)
        capacidadeheading = list(capacidade)
        capacidadestatus = list(capacidade['status'])
        capacidadedate1 = list(capacidade[str(dash.somardata(date, 0))])
        capacidadedate2 = list(capacidade[str(dash.somardata(date, 1))])
        capacidadedate3 = list(capacidade[str(dash.somardata(date, 2))])
        capacidadedate4 = list(capacidade[str(dash.somardata(date, 3))])
        capacidadedate5 = list(capacidade[str(dash.somardata(date, 4))])
        capacidadedate6 = list(capacidade[str(dash.somardata(date, 5))])
        capacidadedate7 = list(capacidade[str(dash.somardata(date, 6))])
        capacidadedate8 = list(capacidade[str(dash.somardata(date, 7))])
        capacidadedate9 = list(capacidade[str(dash.somardata(date, 8))])
        capacidadedate10 = list(capacidade[str(dash.somardata(date, 9))])
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
        escala = dash.tratarfiltrarescala(date, região, bu)
        escalaheading = list(escala)
        escalaregião = list(escala['região']) 
        escalahub = list(escala['hub'])
        escalaescala = list(escala['escala'])
        escaladata = list(escala['data'].astype(str))
        escalaid_técnica = list(escala['id_técnica'])
        escalatécnica = list(escala['técnica'])
        escalahrentrada = list(escala['hr_entrada'])
        escalahrsaída = list(escala['hr_saída'])
        escalaárea = list(escala['área'])
        escalabu = list(escala['bu'])
        escalastatus = list(escala['status'])
        return  render_template("index.html", prioridadeheading=prioridadeheading, prioridaderegião=prioridaderegião, prioridadeárea=prioridadeárea, prioridadehub=prioridadehub, 
        prioridadedate1=prioridadedate1, prioridadedate2=prioridadedate2, prioridadedate3=prioridadedate3, prioridadedate4=prioridadedate4, prioridadedate5=prioridadedate5,
        prioridadedate6=prioridadedate6, prioridadedate7=prioridadedate7, prioridadedate8=prioridadedate8, prioridadedate9=prioridadedate9, prioridadedate10=prioridadedate10,
        escalaheading=escalaheading, escalaregião=escalaregião, escalahub=escalahub, escalaescala=escalaescala, escaladata=escaladata, escalaid_técnica=escalaid_técnica, 
        escalatécnica=escalatécnica, escalahrentrada=escalahrentrada, escalahrsaída=escalahrsaída, escalaárea=escalaárea, escalabu=escalabu, escalastatus=escalastatus, 
        regiões = regiões, bus = bus, capacidadeheading=capacidadeheading, capacidadestatus=capacidadestatus, capacidadedate1=capacidadedate1, capacidadedate2=capacidadedate2, 
        capacidadedate3=capacidadedate3, capacidadedate4=capacidadedate4, capacidadedate5=capacidadedate5, capacidadedate6=capacidadedate6, capacidadedate7=capacidadedate7, 
        capacidadedate8=capacidadedate8, capacidadedate9=capacidadedate9, capacidadedate10=capacidadedate10)

@app.route("/abrirslots")
def abrirslots():   
    data = request.args.get('col')
    área = request.args.get('lin')
    id_técnica = request.args.get('id_técnica')
    técnica = request.args.get('técnica')
    produto = request.args.get('produto')
    regime = request.args.get('regime')
    inicioregime = request.args.get('inicioregime')
    fimregime = request.args.get('fimregime')
    hub = request.args.get('hub')
    duração = int(request.args.get('duração'))
    regime = dash.regime(regime)
    idparceiro = dash.idparceiro(área)
    slotatual = datetime.strptime(inicioregime, "%H:%M:%S")
    if (regime == 'rotating') or (regime == 'diarist' and datetime.strptime(fimregime, "%H:%M:%S") > datetime.strptime("14:00:00", "%H:%M:%S")): 
        while(slotatual < datetime.strptime(fimregime, "%H:%M:%S") - timedelta(hours=0, minutes=duração, seconds=0)):
            slotatual = slotatual + timedelta(hours=0, minutes=duração, seconds=0)
            if slotatual >= datetime.strptime('12:00:00', "%H:%M:%S") and slotatual < datetime.strptime('13:00:00', "%H:%M:%S"):
                slotatual = datetime.strptime('13:00:00', "%H:%M:%S")
            slotatualtexto = slotatual.strftime('%H:%M:%S')
            if slotatualtexto != "19:00:00":    
                print('Abrindo slot ' + slotatualtexto + '...')
                token = dash.token()
                agenda.abrirslots(f'{data}', f'{regime}', f'{produto}', idparceiro, slotatualtexto, token)
                id = agenda.iddoslot()
                dash.inserirdados( id, data, slotatual, área, hub, regime, produto, id_técnica, técnica)

    elif regime == 'diarist':
        while(slotatual < datetime.strptime(fimregime, "%H:%M:%S") - timedelta(hours=0, minutes=duração, seconds=0)):
            slotatual = slotatual + timedelta(hours=0, minutes=duração, seconds=0)
            slotatualtexto = slotatual.strftime('%H:%M:%S')
            print('Abrindo slot ' + slotatualtexto + '...')
            token = dash.token()
            agenda.abrirslots(f'{data}', f'{regime}', f'{produto}', idparceiro, slotatualtexto, token)
            id = agenda.iddoslot()
            dash.inserirdados( id, data, slotatual, área, hub, regime, produto, id_técnica, técnica)
    
    print('Todos os slots abertos com sucesso!')
    return redirect("https://workstation-planejamento.herokuapp.com/", code=302)

def abriragenda(data, produto, idparceiro, área, hub, duração, id_técnica, técnica, regime, slotatual, fimregime):
    print('Você está abrindo slot na área: ' + área + '.')
    print(' O ID do parceiro da área é: ' + idparceiro + '.')
    slotatual = str(slotatual)
    slotatual = datetime.strptime(slotatual, "%H:%M:%S")
    fimregime = str(fimregime)
    if (regime == 'rotating') or (regime == 'diarist' and datetime.strptime(fimregime, "%H:%M:%S") > datetime.strptime("14:00:00", "%H:%M:%S")): 
        while(slotatual < datetime.strptime(fimregime, "%H:%M:%S") - timedelta(hours=0, minutes=duração, seconds=0)):
            slotatual = slotatual + timedelta(hours=0, minutes=duração, seconds=0)
            if slotatual >= datetime.strptime('12:00:00', "%H:%M:%S") and slotatual < datetime.strptime('13:00:00', "%H:%M:%S"):
                slotatual = datetime.strptime('13:00:00', "%H:%M:%S")
            slotatualtexto = slotatual.strftime('%H:%M:%S')
            if slotatualtexto != "19:00:00":    
                print('Abrindo slot ' + slotatualtexto + '...')
                token = dash.token()
                agenda.abrirslots(f'{data}', f'{regime}', f'{produto}', idparceiro, slotatualtexto, token)
                id = agenda.iddoslot()
                dash.inserirdados( id, data, slotatual, área, hub, regime, produto, id_técnica, técnica)

    elif regime == 'diarist':
        while(slotatual < datetime.strptime(fimregime, "%H:%M:%S") - timedelta(hours=0, minutes=duração, seconds=0)):
            slotatual = slotatual + timedelta(hours=0, minutes=duração, seconds=0)
            slotatualtexto = slotatual.strftime('%H:%M:%S')
            print('Abrindo slot ' + slotatualtexto + '...')
            token = dash.token()
            agenda.abrirslots(f'{data}', f'{regime}', f'{produto}', idparceiro, slotatualtexto, token)
            id = agenda.iddoslot()
            dash.inserirdados( id, data, slotatual, área, hub, regime, produto, id_técnica, técnica)

def aberturaautomatica(hub, bu, dias, estrelasmin, estrelasmax, duração):
    current_date = date.today()
    eixoárea = 0
    diaabertura = current_date + timedelta(dias)
    área = dash.áreasabertura(hub, bu, estrelasmin, estrelasmax)
    linhasárea = len(área)
    escala = dash.escalaautomatica(diaabertura, hub, bu)
    linhasescala = len(escala)
    if linhasescala == 0: 
        linhasárea = 0 
        print('Não temos técnicas para trabalhar nesse dia.')
    else: 
        while linhasárea > eixoárea:
            escala = dash.escalaautomatica(diaabertura, hub, bu)
            disponibilidadeescala = escala[(escala['área'] == área.iloc[eixoárea]['área'])]
            técnicadisponível = len(escala[escala['status'] == 'Disponível'])
            if len(disponibilidadeescala) == 1:
                print('Área ' + área.iloc[eixoárea]['área'] + ' já está aberta.')
                eixoárea = eixoárea + 1
            elif técnicadisponível == 0:
                print('Nenhuma técnica disponível para abrir slots.')
                linhasárea = 0
            else:
                escalafiltro = escala[(escala['status'] == 'Disponível')]
                abriragenda(escalafiltro.iloc[0]['data'], bu, área.iloc[eixoárea]['id_parceiro'], área.iloc[eixoárea]['área'], hub, duração, escalafiltro.iloc[0]['id_técnica'], escalafiltro.iloc[0]['técnica'], dash.regime(escalafiltro.iloc[0]['escala']), escalafiltro.iloc[0]['hr_entrada'], escalafiltro.iloc[0]['hr_saída'])
                print('Área ' + área.iloc[eixoárea]['área'] + ' foi aberta com sucesso!')
                eixoárea = eixoárea + 1
    return(print('Dia ' + str(diaabertura) + ' verificado com sucesso.'))

if __name__ == "__main__":
    app.run(debug= True)