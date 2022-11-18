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

@app.route("/filtrar", methods=["GET","POST"])
def filtrar():
    date = request.args.get('date')
    região = request.args.get('região')
    bu = request.args.get('bu')
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
    slotatual = inicioregime
    print('próximo de abrir agenda...')
    abriragenda(data, produto, idparceiro, área, hub, duração, id_técnica, técnica, regime, slotatual, fimregime)
    return redirect("https://workstation-planejamento.herokuapp.com/", code=302)

def abriragenda(data, produto, idparceiro, área, hub, duração, id_técnica, técnica, regime, slotatual, fimregime):
    print('Você está abrindo slot na área: ' + área + '.')
    print(' O ID do parceiro da área é: ' + idparceiro + '.')
    slotatual = str(slotatual)
    slotatual = datetime.strptime(slotatual, "%H:%M:%S")
    fimregime = str(fimregime)
    quantidadeslots = 0
    slotsdaagenda = []
    if (regime == 'rotating') or (regime == 'diarist' and datetime.strptime(fimregime, "%H:%M:%S") > datetime.strptime("14:00:00", "%H:%M:%S")): 
        while(slotatual < datetime.strptime(fimregime, "%H:%M:%S") - timedelta(hours=0, minutes=duração+20, seconds=0)):
            slotatual = slotatual + timedelta(hours=0, minutes=duração, seconds=0)
            if slotatual >= datetime.strptime('11:30:00', "%H:%M:%S") and slotatual < datetime.strptime('13:00:00', "%H:%M:%S"):
                slotatual = datetime.strptime('13:00:00', "%H:%M:%S")
            slotatualtexto = slotatual.strftime('%H:%M:%S')
            if slotatualtexto != "19:00:00":    
                print('Registrando slot ' + slotatualtexto + '...')
                slotsdaagenda.append({"time": slotatualtexto, "supplier_id": idparceiro, "duration": duração})
                quantidadeslots = quantidadeslots + 1

    elif regime == 'diarist':
        while(slotatual < datetime.strptime(fimregime, "%H:%M:%S") - timedelta(hours=0, minutes=duração+30, seconds=0)):
            slotatual = slotatual + timedelta(hours=0, minutes=duração, seconds=0)
            slotatualtexto = slotatual.strftime('%H:%M:%S')
            print('Registrando slot ' + slotatualtexto + '...') 
            slotsdaagenda.append({"time": slotatualtexto, "supplier_id": idparceiro, "duration": duração})
            quantidadeslots = quantidadeslots + 1
    print('Consultando token...')
    token = dash.token()
    agenda.abrirslots(f'{data}', f'{regime}', f'{produto}', idparceiro, token, slotsdaagenda, f'{área}', f'{hub}', id_técnica, técnica)
    return print('Todos os slots abertos com sucesso!')

@app.route("/abrirslotsminimos", methods=["GET","POST"])
def abrirslotsminimos():
    hub = request.args.get('hub') 
    dias = 60
    # SLOTS A CADA 15 DIAS
    aberturaautomatica(f'{hub}', 'laboratories', dias, 15, 0, 2, 40, 0)
    aberturaautomatica(f'{hub}', 'vaccines', dias, 15, 0, 2, 40, 0)

    # SLOTS A CADA 7 DIAS     
    aberturaautomatica(f'{hub}', 'laboratories', dias, 7, 3, 4, 30, 0)
    aberturaautomatica(f'{hub}', 'vaccines', dias, 7, 3, 4, 40, 0)

    # SLOTS A CADA 1 DIA
    aberturaautomatica(f'{hub}', 'laboratories', dias, 1, 5, 6, 30, 0)
    aberturaautomatica(f'{hub}', 'vaccines', dias, 1, 5, 6, 40, 0)
    return 'Agendas abertas com sucesso.'

@app.route("/abrirslotsminimossmallops", methods=["GET","POST"])
def abrirslotsminimossmallops():
    hub = request.args.get('hub')
    dias = 46 
    # SLOTS A CADA 15 DIAS
    aberturaautomatica(f'{hub}', 'laboratories', dias, 15, 0, 2, 50, 0)
    aberturaautomatica(f'{hub}', 'vaccines', dias, 15, 0, 2, 50, 0)
    
    # SLOTS A CADA 7 DIAS     
    aberturaautomatica(f'{hub}', 'laboratories', dias, 7, 3, 4, 50, 0)
    aberturaautomatica(f'{hub}', 'vaccines', dias, 7, 3, 4, 50, 0)
    
    # SLOTS A CADA 1 DIA
    aberturaautomatica(f'{hub}', 'laboratories', dias, 1, 5, 6, 50, 0)
    aberturaautomatica(f'{hub}', 'vaccines', dias, 1, 5, 6, 50, 0)
    return 'Agendas abertas com sucesso.'

@app.route("/abrirslotssobdemanda", methods=["GET","POST"])
def abrirslotssobdemanda():
    hub = request.args.get('hub')
    dias = 5
    # SLOTS A CADA 15 DIAS
    aberturaautomaticasobdemanda('São Cristóvão', 'vaccines', dias, 0.80, 1)
    
    return 'Agendas abertas com sucesso.'


def aberturaautomatica(hub, bu, dias, rangedias, estrelasmin, estrelasmax, duração, domingo):
    while dias > 15:
        current_date = date.today()
        eixoárea = 0
        diaabertura = current_date + timedelta(dias)
        rangediasfinal = diaabertura + timedelta(rangedias)
        área = dash.áreasabertura(hub, bu, 0, 7, domingo)
        área = área[(área['classificação'] >= estrelasmin) &( área['classificação'] <= estrelasmax)]
        linhasárea = len(área)
        escala = dash.escalaautomatica(diaabertura, hub, bu, diaabertura)
        linhasescala = len(escala)
        print('Verificando o dia ' + str(diaabertura) + ' ...')
        if linhasescala == 0: 
            linhasárea = 0 
            print('Não temos técnica disponível para trabalhar nesse dia.')
        else: 
            while linhasárea > eixoárea:
                escala = dash.escalaautomatica(diaabertura, hub, bu, rangediasfinal)
                disponibilidadeescala = escala[(escala['área'] == área.iloc[eixoárea]['área'])]
                escala = dash.escalaautomatica(diaabertura, hub, bu, diaabertura)
                técnicadisponível = len(escala[escala['status'] == 'Disponível'])
                if len(disponibilidadeescala) >= 1:
                    print('A área ' + área.iloc[eixoárea]['área'] + ' já está aberta no range de dias solicitado.')
                    eixoárea = eixoárea + 1
                elif técnicadisponível == 0:
                    print('Não temos técnica disponível para trabalhar nesse dia na ' + área.iloc[eixoárea]['área'] + '.')
                    linhasárea = 0
                else:
                    escalafiltro = escala[(escala['status'] == 'Disponível')]
                    abriragenda(escalafiltro.iloc[0]['data'], bu, área.iloc[eixoárea]['id_parceiro'], área.iloc[eixoárea]['área'], hub, duração, escalafiltro.iloc[0]['id_técnica'], escalafiltro.iloc[0]['técnica'], dash.regime(escalafiltro.iloc[0]['escala']), escalafiltro.iloc[0]['hr_entrada'], escalafiltro.iloc[0]['hr_saída'])
                    print('Slots na ' + área.iloc[eixoárea]['área'] + ' abertos com sucesso!')
                    eixoárea = eixoárea + 1
        dias = dias - rangedias
    return(print('Dia ' + str(diaabertura) + ' verificado.'))

def aberturaautomaticasobdemanda(hub, bu, dias, taxaocupacao, removerduplicado):
    current_date = date.today()
    diaabertura = current_date + timedelta(dias)
    tabelaareastaxadeocupacao = dash.filtrartaxaocupacao(hub, bu, diaabertura, taxaocupacao)
    if removerduplicado == 1:
        tabelaareastaxadeocupacao = tabelaareastaxadeocupacao.drop_duplicates(subset='parceiro_nome', keep='first')
    escala = dash.escalaautomatica(diaabertura, hub, bu, diaabertura)
    linhastabelataxadeocupacao = len(tabelaareastaxadeocupacao)
    print('Verificando dia ' + str(diaabertura) + '...')
    while linhastabelataxadeocupacao > 0:
        quantidadetécnicadisponível = len(escala[escala['status'] == 'Disponível'])
        if linhastabelataxadeocupacao == 0:
            print('Não temos nenhuma área com a taxa de ocupação maior que ' + str(taxaocupacao))
        elif quantidadetécnicadisponível == 0:
            print('Não temos técnica disponível para trabalhar nesse dia.')
            linhastabelataxadeocupacao = 0
        else:
            abriragenda(tabelaareastaxadeocupacao.iloc[linhastabelataxadeocupacao - 1]['slot_date'], bu, área.iloc[eixoárea]['id_parceiro'], área.iloc[eixoárea]['área'], hub, duração, escalafiltro.iloc[0]['id_técnica'], escalafiltro.iloc[0]['técnica'], dash.regime(escalafiltro.iloc[0]['escala']), escalafiltro.iloc[0]['hr_entrada'], escalafiltro.iloc[0]['hr_saída'])
            print('Slots na ' + tabelaareastaxadeocupacao.iloc[linhastabelataxadeocupacao - 1]['parceiro_nome'] + ' abertos com sucesso!')
            linhastabelataxadeocupacao = linhastabelataxadeocupacao - 1
        return(print('Dia ' + str(diaabertura) + ' verificado.'))
        


if __name__ == "__main__":
    app.run(debug= True)