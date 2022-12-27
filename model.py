import pandas as pd
import requests
import json
from datetime import datetime, timedelta, date
import banco
import parametros
import math

Parametros = parametros.Parametros_internos()
Banco_de_dados = banco.Banco_de_dados()

# teste = requests.delete(url='https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings/73361?session_token=fd5d8958073e6d5a51c83cd92df32b8b07d22b92')
# print(teste)

#abrir slots: https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings?session_token={token}

class Slots():
    def abrirslots(self, data, regime, bu, id_parceiro, url_token, slots_agenda, area, hub, id_tecnica, tecnica):    
        payload = { "bookings": [{ "date": f"{data}", "work_shift": f"{regime}", "product_type": f"{bu}", "supplier_id": id_parceiro, "slots": slots_agenda}]}
        slots_abertos_json = requests.post(url=url_token, json=payload)
        print('Slots abertos com sucesso!')
        slots_abertos = json.loads(slots_abertos_json.text)
        slots_abertos = slots_abertos[0]
        tabela_slots_abertos = slots_abertos['slots']
        quantidade_slots_abertos = len(tabela_slots_abertos)
        tabela_slots_da_agenda = []
        while quantidade_slots_abertos > 0:
            slot_da_agenda_selecionada = tabela_slots_abertos[quantidade_slots_abertos - 1]
            slot_id = slot_da_agenda_selecionada['id'] 
            horario = slot_da_agenda_selecionada['date'] 
            tabela_slots_da_agenda.append({'id_slot':slot_id,'data': data,'horario': str(horario),'area': area,'hub': hub,'regime': regime,'produto': bu,'id_tecnica': id_tecnica,'tecnica': tecnica})
            quantidade_slots_abertos = quantidade_slots_abertos - 1
        subiu_slots_na_tabela = Slots().verificar_se_id_slots_subiu_na_tabela(slot_id)
        while subiu_slots_na_tabela == 'Não':
            Banco_de_dados.inserirdados(tabela_slots_da_agenda, 'bi')
            subiu_slots_na_tabela = Slots().verificar_se_id_slots_subiu_na_tabela(slot_id)
        return print('Slots registrados no banco com sucesso!')

    def abertura_minima_automatica(self, hub, bu, dias, range_dias, classificacao_minima, classificacao_maxima, duracao):
        while dias > 12:
            dia_hoje = date.today()
            eixo_tabela_area = 0
            dia_abertura_slot = dia_hoje + timedelta(dias)
            range_dia_inicial_abertura_slot = dia_abertura_slot - timedelta(range_dias)
            tabela_classificacao_areas = Area().retornar_tabela_classificação_areas(hub, bu)
            tabela_classificacao_areas = Area().remover_areas_nao_utilizadas(tabela_classificacao_areas, classificacao_minima, classificacao_maxima)
            quantidade_linhas_tabela_classificacao_areas = len(tabela_classificacao_areas)
            print('Verificando o dia ' + str(dia_abertura_slot) + ' ...')
            maximo_areas_permitido_abrir_no_mesmo_dia = 1000
            if classificacao_maxima <= 2:
                maximo_areas_permitido_abrir_no_mesmo_dia = 2
            while quantidade_linhas_tabela_classificacao_areas > eixo_tabela_area:
                permissao_abrir_slots = 1
                tabela_agendas_hub = Agenda().retornar_tabela_agendas_hub(hub, bu, range_dia_inicial_abertura_slot, dia_abertura_slot)
                tabela_tecnicas_disponiveis_dia_abertura = Agenda().retornar_tabela_agendas_hub(hub, bu, dia_abertura_slot, dia_abertura_slot)
                tecnicas_disponiveis_dia_abertura = tabela_tecnicas_disponiveis_dia_abertura[tabela_tecnicas_disponiveis_dia_abertura['status'] == 'Disponível']
                filtrando_area_atual_da_tabela_agendas_hub = tabela_agendas_hub[(tabela_agendas_hub['área'] == tabela_classificacao_areas.iloc[eixo_tabela_area]['área'])]
                if maximo_areas_permitido_abrir_no_mesmo_dia == 0:
                    quantidade_linhas_tabela_classificacao_areas = 0
                    permissao_abrir_slots = 0
                    print('Limite de abertura na quantidade de áreas classificação até 2 atingido.')
                if len(tecnicas_disponiveis_dia_abertura) == 0: 
                    quantidade_linhas_tabela_classificacao_areas = 0 
                    permissao_abrir_slots = 0
                    print('Não temos técnica disponível para trabalhar nesse dia.')
                if len(filtrando_area_atual_da_tabela_agendas_hub) >= 1:
                    print('A área ' + tabela_classificacao_areas.iloc[eixo_tabela_area]['área'] + ' já está aberta no range de dias solicitado.')
                    eixo_tabela_area = eixo_tabela_area + 1
                    permissao_abrir_slots = 0
                if permissao_abrir_slots == 1:
                    Agenda().registrar_agenda(tecnicas_disponiveis_dia_abertura.iloc[0]['data'], bu, tabela_classificacao_areas.iloc[eixo_tabela_area]['id_parceiro'], tabela_classificacao_areas.iloc[eixo_tabela_area]['área'], hub, duracao, tecnicas_disponiveis_dia_abertura.iloc[0]['id_técnica'], tecnicas_disponiveis_dia_abertura.iloc[0]['técnica'], Parametros.retornar_regime(tecnicas_disponiveis_dia_abertura.iloc[0]['escala']), tecnicas_disponiveis_dia_abertura.iloc[0]['hr_entrada'], tecnicas_disponiveis_dia_abertura.iloc[0]['hr_saída'])
                    print('Slots na ' + tabela_classificacao_areas.iloc[eixo_tabela_area]['área'] + ' abertos com sucesso!')
                eixo_tabela_area = eixo_tabela_area + 1
            dias = dias - range_dias
            print('Dia ' + str(dia_abertura_slot) + ' verificado.')

    def abertura_sob_demanda_automatica(self, hub, bu, dias, taxa_ocupacao, duracao, remover_duplicado):
        dia_hoje = date.today()
        dia_abertura_slot = dia_hoje + timedelta(dias)
        if remover_duplicado == 0:
            tabela_taxa_ocupacao_score = Area().retorna_tabela_taxa_ocupacao_score_filtrada_sem_duplicada(hub, bu, dia_abertura_slot, taxa_ocupacao)    
        if remover_duplicado == 1:
            tabela_taxa_ocupacao_score = Area().retorna_tabela_taxa_ocupacao_score_filtrada(hub, bu, dia_abertura_slot, taxa_ocupacao)
            tabela_taxa_ocupacao_score = tabela_taxa_ocupacao_score.drop_duplicates(subset='parceiro_nome', keep='first')
        quantidade_linhas_tabela_taxa_ocupacao_score = len(tabela_taxa_ocupacao_score)
        print('Verificando dia ' + str(dia_abertura_slot) + '...')
        if quantidade_linhas_tabela_taxa_ocupacao_score == 0:
                print('Não temos nenhuma área com a taxa de ocupação maior que ' + str(taxa_ocupacao))
        while quantidade_linhas_tabela_taxa_ocupacao_score > 0:
            permissao_abrir_slots = 1
            tabela_agendas_hub = Agenda().retornar_tabela_agendas_hub(hub, bu, dia_abertura_slot, dia_abertura_slot)
            tabela_agendas_hub_tecnicas_disponiveis = tabela_agendas_hub[tabela_agendas_hub['status'] == 'Disponível']
            quantidade_tecnica_disponivel_tabela_agendas_hub = len(tabela_agendas_hub_tecnicas_disponiveis)
            if quantidade_tecnica_disponivel_tabela_agendas_hub == 0:
                print('Não temos técnica disponível para trabalhar nesse dia.')
                quantidade_linhas_tabela_taxa_ocupacao_score = 0
                permissao_abrir_slots = 0
            if permissao_abrir_slots == 1:
                Agenda().registrar_agenda(tabela_taxa_ocupacao_score.iloc[quantidade_linhas_tabela_taxa_ocupacao_score - 1]['slot_date'], bu, str(tabela_taxa_ocupacao_score.iloc[quantidade_linhas_tabela_taxa_ocupacao_score - 1]['ID Área']), tabela_taxa_ocupacao_score.iloc[quantidade_linhas_tabela_taxa_ocupacao_score - 1]['parceiro_nome'], hub, duracao, tabela_agendas_hub_tecnicas_disponiveis.iloc[0]['id_técnica'], tabela_agendas_hub_tecnicas_disponiveis.iloc[0]['técnica'], Parametros.retornar_regime(tabela_agendas_hub_tecnicas_disponiveis.iloc[0]['escala']), tabela_agendas_hub_tecnicas_disponiveis.iloc[0]['hr_entrada'], tabela_agendas_hub_tecnicas_disponiveis.iloc[0]['hr_saída'])
                print('Slots na ' + tabela_taxa_ocupacao_score.iloc[quantidade_linhas_tabela_taxa_ocupacao_score - 1]['parceiro_nome'] + ' abertos com sucesso!')
                quantidade_linhas_tabela_taxa_ocupacao_score = quantidade_linhas_tabela_taxa_ocupacao_score - 1
            return(print('Dia ' + str(dia_abertura_slot) + ' verificado.'))

    def fechar_slots(self, id_agenda, token, id_tecnica, nome_tecnica, hub, data):
        print('Fechando slots...')
        url = f'https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings/{id_agenda}?session_token={token}'
        requests.delete(url)
        Banco_de_dados.remover_dados(id_tecnica, hub, data)
        return print('Todos os slots da tecnica ' + str(nome_tecnica) + ' foram fechados com sucesso!')

    def retornar_token(self):
        consulta_tabela_token = f"""
        select remember_token from users where username = 'gabriel.rangel@beepsaude.com.br'
        """
        tabela_token = Banco_de_dados.consulta('tech', consulta_tabela_token)
        token = tabela_token.iloc[0]['remember_token']
        token = f'https://api.beepapp.com.br/api/v8/booking_management/schedule_bookings?session_token={token}'
        return token

    def verificar_se_id_slots_subiu_na_tabela(self, id_slot):
        consultar_tabela_workstation_id_slot = f"""
        select * from workstation.slots_abertos
        where id_slot = '{id_slot}'
        """
        tabela_workstation_id_slot = Banco_de_dados.consulta('bi', consultar_tabela_workstation_id_slot)
        if len(tabela_workstation_id_slot) > 0:
            subiu_slots_na_tabela = 'Sim'
        elif len(tabela_workstation_id_slot) == 0:
            subiu_slots_na_tabela = 'Não'
        return subiu_slots_na_tabela
class SalesForce():
    def retornar_pedido_reagendamento_salesforce(self, voucher):
        consultar_pedido_reagendamento_salesforce = f"""
        select subject from salesforce.case 
        where 1=1
        and subject like '%Alteração no Atendimento |%' 
        and subject like '% | WS%' 
        and subject like '%{voucher}%'
        """
        tabela_pedido_reagendamento_salesforce = Banco_de_dados.consulta('bi', consultar_pedido_reagendamento_salesforce)
        return tabela_pedido_reagendamento_salesforce

    def retornar_pedido_extra_salesforce(self, hub, bu, data, nome_tecnica):
        consultar_pedido_extra_salesforce = f"""
        select subject from salesforce.case 
        where 1=1
        and subject like '%Extra -%' 
        and subject like '% | WS%' 
        and subject like '%{hub}%'
        and subject like '%{bu}%'
        and subject like '%{data}%'
        and description like '%{nome_tecnica}%'
        """
        tabela_pedido_extra_salesforce = Banco_de_dados.consulta('bi', consultar_pedido_extra_salesforce)
        return tabela_pedido_extra_salesforce
class Agenda():
    def __init__(self):
        self.filtro_status_lancamento_escala_app = """((previsto in ('Trabalho','Afastamento INSS','Descanso','Férias','Folga','Folga extra','Folga hora','Licença maternidade','Licença médica','Vale folga') and (lancamento in ('Trabalho','Hora extra','Meia folga'))) 
        or (previsto = 'Trabalho' and lancamento is null) 
        or (previsto in ('Trabalho','Afastamento INSS','Férias','Folga','Folga extra','Folga hora','Licença maternidade','Licença médica','Vale folga') and (lancamento = 'Aleitamento materno')))
        and ((jeeo.data_inicio_previsto::time >= '06:00'
        and jeeo.data_inicio_previsto::time <= '14:00'
        and jeeo.data_fim_previsto::time >= '10:00'
        and jeeo.data_fim_previsto::time <= '20:00'
        and (jeeo.data_fim_previsto::time - jeeo.data_inicio_previsto::time) >= '02:00') 
        or (jeeo.data_inicio_lancamento::time >= '06:00'
        and jeeo.data_inicio_lancamento::time <= '14:00'
        and jeeo.data_fim_lancamento::time >= '10:00'
        and jeeo.data_fim_lancamento::time <= '20:00'
        and (jeeo.data_fim_lancamento::time - jeeo.data_inicio_lancamento::time) >= '02:00' ))
        """
        self.quantidade_tecnicas_para_uma_contingencia = 10
            
    def fechar_agendas(self, hub, bu, dias):
        dia_hoje = date.today()
        while dias > 0:
            data = dia_hoje + timedelta(dias)
            tabela_agendas_escala_app_alterada = Agenda().retornar_tabela_agendas_escala_app(data, hub, bu, 1)
            quantidade_agendas_escala_app_alterada = len(tabela_agendas_escala_app_alterada)
            token = Slots().retornar_token()
            if quantidade_agendas_escala_app_alterada == 0:
                print('Todas as agendas do dia ' + str(data) + ' estão funcionando corretamente.')
            while quantidade_agendas_escala_app_alterada > 0:
                permissao_substituir_tecnica = 0
                permissao_fechar_slots = 1
                id_tecnica = tabela_agendas_escala_app_alterada.iloc[quantidade_agendas_escala_app_alterada-1]['id_tecnica']
                nome_tecnica = tabela_agendas_escala_app_alterada.iloc[quantidade_agendas_escala_app_alterada-1]['tecnica']
                horario_min = tabela_agendas_escala_app_alterada.iloc[quantidade_agendas_escala_app_alterada-1]['min']
                horario_max = tabela_agendas_escala_app_alterada.iloc[quantidade_agendas_escala_app_alterada-1]['max']
                quantidade_slots_vendidos = tabela_agendas_escala_app_alterada.iloc[quantidade_agendas_escala_app_alterada-1]['slots']
                id_agenda = Agenda().retornar_id_agenda(data, hub, bu, id_tecnica)    
                print('Analisando a possibilidade de fechar slots da agenda ' + str(nome_tecnica) + '...')
                if id_agenda == 0:
                    print('O Id da Agenda não foi localizado na tabela.')
                    permissao_fechar_slots = 0
                if (quantidade_slots_vendidos > 0):
                    permissao_fechar_slots = 0
                    tabela_agendas_slots_abertos_horario_aproximado = Agenda().retornar_tecnica_horario_aproximado_sem_slots_vendidos(data, id_tecnica, hub, bu, horario_min, horario_max)
                    if len(tabela_agendas_slots_abertos_horario_aproximado) > 0:
                        permissao_fechar_slots = 1
                        permissao_substituir_tecnica = 1
                        id_tecnica_substituida = id_tecnica
                        nome_tecnica_substituida = nome_tecnica
                        id_tecnica = tabela_agendas_slots_abertos_horario_aproximado.iloc[0]['id_tecnica']
                        nome_tecnica = tabela_agendas_slots_abertos_horario_aproximado.iloc[0]['tecnica']
                        id_agenda = Agenda().retornar_id_agenda(data, hub, bu, id_tecnica)
                    if len(tabela_agendas_slots_abertos_horario_aproximado) == 0 and quantidade_slots_vendidos <= 7:
                        permissao_fechar_slots = 0
                        lista_vouchers = Agenda().retornar_lista_vouchers_agenda(data, hub, bu, id_tecnica)
                        quantidade_voucher = 0
                        tabela_pedido_reagendamento_salesforce = SalesForce().retornar_pedido_reagendamento_salesforce(str(lista_vouchers[quantidade_voucher]))
                        while quantidade_voucher < len(lista_vouchers):
                            if len(tabela_pedido_reagendamento_salesforce) > 0:
                                print('Reagendamento já solicitado')
                            if len(tabela_pedido_reagendamento_salesforce) == 0:
                                print('Solicitando reagendamento do voucher: ' + str(lista_vouchers[quantidade_voucher]))
                                mensagem = f"""
                                <p> A Técnica {nome_tecnica} do hub de {hub} e bu {bu} teve um imprevisto e por isso não poderá comparecer no dia {data}. </p>
                                <p>Favor reagendar o voucher: {lista_vouchers[quantidade_voucher]}.</p>
                                <p>essa mensagem foi enviada automaticamente pelo workstation, favor tratar o caso solicitado e não responder o email.</p>
                                """
                                Banco_de_dados.enviar_email(mensagem, f'Alteração no Atendimento | Voucher: {lista_vouchers[quantidade_voucher]} | {data} | WS') 
                                quantidade_voucher = quantidade_voucher + 1           
                    if len(tabela_agendas_slots_abertos_horario_aproximado) == 0 and quantidade_slots_vendidos >= 8:
                        permissao_fechar_slots = 0
                        tabela_pedido_extra_salesforce = SalesForce().retornar_pedido_extra_salesforce(hub,bu,data,nome_tecnica)
                        if len(tabela_pedido_extra_salesforce) > 0:
                            print('Extra já solicitado.')
                        if len(tabela_pedido_extra_salesforce) == 0:
                            print('Solicitando extra...')
                            mensagem = f"""
                            <p> A Técnica {nome_tecnica} do hub de {hub} e bu {bu} teve um imprevisto e por isso não poderá comparecer no dia {data}. </p>
                            <p>Tivemos {quantidade_slots_vendidos} slots já vendidos, favor solicitar uma técnica extra para substituí-la!.</p>
                            <p>essa mensagem foi enviada automaticamente pelo workstation, favor tratar o caso solicitado e não responder o email.</p>
                            """
                            Banco_de_dados.enviar_email(mensagem, f'Pedir equipe Extra - {hub} - {bu} - {data}')
                if tabela_agendas_escala_app_alterada.iloc[quantidade_agendas_escala_app_alterada-1]['slots'] == 0:
                    permissao_fechar_slots = 1
                if permissao_fechar_slots == 1:    
                    Slots().fechar_slots(id_agenda, token, id_tecnica, nome_tecnica, hub, data)
                if permissao_substituir_tecnica == 1:
                    Banco_de_dados.atualizar_dados_tecnica(id_tecnica, nome_tecnica, id_tecnica_substituida, nome_tecnica_substituida, data, hub)
                quantidade_agendas_escala_app_alterada = quantidade_agendas_escala_app_alterada - 1
            dias = dias - 1
        return(print('agendas do dia ' + str(data) +' analisadas.'))

    def retornar_lista_vouchers_agenda(self, data, hub, bu, id_tecnica):
        consultar_vouchers_agenda = f"""
        select sc.id as voucher from workstation.slots_abertos wsa
        left join sp_product_schedule_slots sp
        on wsa.id_slot::text = sp.id::text
        left join sp_calls sc 
        on sp.call_product_id::text = sc.service_id::text
        where wsa.produto = '{bu}'
        and wsa.hub = '{hub}'
        and wsa.data = '{data}'
        and wsa.id_tecnica = '{id_tecnica}'
        and sc.service_id is not null
        """
        tabela_vouchers_agenda = Banco_de_dados.consulta('bi', consultar_vouchers_agenda)
        lista_vouchers_agenda = list(tabela_vouchers_agenda['voucher'])
        return lista_vouchers_agenda

    def retornar_id_agenda(self, data, hub, bu, id_tecnica):
        id_agenda = 0
        consultar_id_agenda = f"""
        select sp.schedule_booking_id as id_agenda from workstation.slots_abertos wsa
        left join sp_product_schedule_slots sp
        on wsa.id_slot::text = sp.id::text
        where wsa.produto = '{bu}'
        and wsa.hub = '{hub}'
        and wsa.data = '{data}'
        and wsa.id_tecnica = '{id_tecnica}'
        group by sp.schedule_booking_id
        """
        tabela_id_agenda = Banco_de_dados.consulta('bi', consultar_id_agenda)
        if len(tabela_id_agenda) > 0:
            id_agenda = tabela_id_agenda.iloc[0]['id_agenda']
        return id_agenda

    def retornar_tabela_agendas_escala_app(self, data, hub, bu, alterado):
        consultar_agendas_workstation = f"""
        select wsa.tecnica, wsa.id_tecnica, wsa.hub, wsa.area, wsa.regime, min(wsa.horario::time), max(wsa.horario::time), wsa.data, count(call_product_id) as slots from workstation.slots_abertos wsa
        left join sp_product_schedule_slots sp
        on wsa.id_slot::text = sp.id::text
        where wsa.produto = '{bu}'
        and wsa.hub = '{hub}'
        and wsa.data = '{data}'
        group by wsa.tecnica, wsa.id_tecnica, wsa.hub, wsa.area, wsa.regime, wsa.data
        """
        bu = Parametros.retornar_bu(bu, 'escala app')
        consultar_agendas_escala_app = f"""
        select colaborador, id_colaborador, hub, data
        from jornadas_escala.escala_operacional jeeo
        where jeeo.escala like '%Técnica%' 
        and jeeo.escala like '{bu}'
        and jeeo.hub = '{hub}'
        and jeeo.data = '{data}'
        and {self.filtro_status_lancamento_escala_app}
        """
        tabela_agendas_workstation = Banco_de_dados.consulta('bi', consultar_agendas_workstation)
        if alterado == 1:
            tabela_agendas_escala_app = Banco_de_dados.consulta('bi', consultar_agendas_escala_app)
            lista_tecnicas_escala_app = list(tabela_agendas_escala_app['colaborador'])
            tabela_agendas_workstation = tabela_agendas_workstation[~tabela_agendas_workstation['tecnica'].isin(lista_tecnicas_escala_app)]
        return tabela_agendas_workstation
    
    def retornar_tecnica_horario_aproximado_sem_slots_vendidos(self, data, id_tecnica, hub, bu, horario_min, horario_max):
        tabela_agendas_slots_abertos = Agenda().retornar_tabela_agendas_escala_app(data, hub, bu, 0)
        tabela_agendas_slots_abertos_horario_aproximado = tabela_agendas_slots_abertos[(tabela_agendas_slots_abertos['min'] == horario_min) &( tabela_agendas_slots_abertos['max'] == horario_max)]
        tabela_agendas_slots_abertos_horario_aproximado = tabela_agendas_slots_abertos_horario_aproximado[tabela_agendas_slots_abertos_horario_aproximado['slots'] == 0]
        print('Encontrado ' + str(len(tabela_agendas_slots_abertos_horario_aproximado)) + ' técnicas com 0 slots.')
        return tabela_agendas_slots_abertos_horario_aproximado

    def retornar_tabela_agendas_regiao(self, data_min, regiao, bu):
        bu = Parametros.retornar_bu(bu, 'escala app')
        consultar_agendas_regiao = f"""
        select macro_região as região, jeeo.hub, jeeo.escala, jeeo.data, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica, 
        case when jeeo."lancamento" is null then jeeo.data_inicio_previsto::time else jeeo.data_inicio_lancamento::time end as hr_entrada, 
        case when jeeo."lancamento" is null then jeeo.data_fim_previsto::time else jeeo.data_fim_lancamento::time end as hr_saída, 
        wsa."area" as área,
        (case when jeeo.escala LIKE '%VAC%' then 'vaccines' when jeeo.escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu,
        (case when wsa.tecnica is not null then 'Ocupado' else 'Disponível' end) as status
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and {self.filtro_status_lancamento_escala_app}
        and jeeo.escala LIKE '{bu}'
        and macro_região = '{regiao}'
        and jeeo.data >= '{data_min}' and jeeo.data <= to_char(DATE '{data_min}', 'YYYY/MM/DD')::date + interval '9 days'
        and jeeo.data > current_date
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area", jeeo."lancamento", jeeo.data_inicio_lancamento, jeeo.data_fim_lancamento
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador
        """        
        tabela_agendas_regiao = Banco_de_dados.consulta('bi', consultar_agendas_regiao)
        return tabela_agendas_regiao

    def retornar_tabela_hibridas_hub(self, data, hub):
        consultar_hibridas_hub = f"""
        select macro_região as região, jeeo.hub, jeeo.escala, jeeo.data, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica, 
        case when jeeo."lancamento" is null then jeeo.data_inicio_previsto::time else jeeo.data_inicio_lancamento::time end as hr_entrada, 
        case when jeeo."lancamento" is null then jeeo.data_fim_previsto::time else jeeo.data_fim_lancamento::time end as hr_saída, 
        wsa."area" as área,
        (case when jeeo.escala LIKE '%VAC%' then 'vaccines' when jeeo.escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu,
        (case when wsa.tecnica is not null then 'Ocupado' else 'Disponível' end) as status
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and {self.filtro_status_lancamento_escala_app}
        and jeeo.hub = '{hub}'
        and jeeo.data = '{data}'
        and jeeo.id_cargo = '18394'
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area", jeeo."lancamento", jeeo.data_inicio_lancamento, jeeo.data_fim_lancamento
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador
        """        
        tabela_hibridas_hub = Banco_de_dados.consulta('bi', consultar_hibridas_hub)
        return tabela_hibridas_hub

    def retornar_tabela_agendas_hub(self, hub, bu, data_min, data_max):
        bu = Parametros.retornar_bu(bu, 'escala app')
        consultar_agenda_hub = f"""
        select macro_região as região, jeeo.hub, jeeo.escala, jeeo.data, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica, 
        case when jeeo."lancamento" is null then jeeo.data_inicio_previsto::time else jeeo.data_inicio_lancamento::time end as hr_entrada, 
        case when jeeo."lancamento" is null then jeeo.data_fim_previsto::time else jeeo.data_fim_lancamento::time end as hr_saída,
        wsa."area" as área,
        (case when jeeo.escala LIKE '%VAC%' then 'vaccines' when jeeo.escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu,
        (case when wsa.tecnica is not null then 'Ocupado' else 'Disponível' end) as status
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and jeeo.escala LIKE '{bu}'
        and jeeo.hub = '{hub}'
        and jeeo.data >= '{data_min}' and jeeo.data <= '{data_max}' 
        and {self.filtro_status_lancamento_escala_app}
        and jeeo.data > current_date
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area", jeeo."lancamento", jeeo.data_inicio_lancamento, jeeo.data_fim_lancamento
        order by status, jeeo.data::date, jeeo.hub, jeeo.colaborador
        """
        tabela_agendas_hub = Banco_de_dados.consulta('bi', consultar_agenda_hub)
        contingencia = Agenda().retornar_nome_agenda_contingencia(data_max, hub)
        lista_contingencia = list(contingencia['técnica'])
        tabela_agendas_hub_sem_contingencia = tabela_agendas_hub[~tabela_agendas_hub['técnica'].isin(lista_contingencia)]
        return tabela_agendas_hub_sem_contingencia

    def retornar_tabela_quantidade_agenda_disponivel_ocupado(self, data, região, bu):
        bu = Parametros.retornar_bu(bu, 'escala app')
        consulta_quantidade_agenda_disponivel_ocupado = f"""   
        select a.status, sum(case a.dataincompleta = data::date when true then 1 else 0 end) as quant, a.data::date from (
        select generate_series(DATE'{data}', to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days',INTERVAL'1 day') as data, macro_região as região, jeeo.data as dataincompleta, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica,
        (case when jeeo.escala LIKE '%VAC%' then 'vaccines' when jeeo.escala LIKE '%LAB%' then 'laboratories' else 'híbrida' end) as bu,
        (case when wsa.tecnica is not null then 'Ocupado' else 'Disponível' end) as status
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and macro_região = '{região}'
        and {self.filtro_status_lancamento_escala_app}
        and jeeo.data >= '{data}' and jeeo.data <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        and jeeo.escala LIKE '{bu}'
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area") a 
        group by a.status, a.data
        order by status, a.data
        """
        tabela_quantidade_agenda_disponivel_ocupado = Banco_de_dados.consulta('bi', consulta_quantidade_agenda_disponivel_ocupado)
        tabela_quantidade_agenda_disponivel_ocupado_pivotada = pd.pivot_table(tabela_quantidade_agenda_disponivel_ocupado, index=["status"], columns=["data"], values=["quant"])
        tabela_quantidade_agenda_disponivel_ocupado = tabela_quantidade_agenda_disponivel_ocupado_pivotada.set_axis(tabela_quantidade_agenda_disponivel_ocupado_pivotada.columns.tolist(), axis=1).reset_index()
        tabela_quantidade_agenda_disponivel_ocupado.columns = ['status', str(Parametros.retornar_data_somada(data, 0)), str(Parametros.retornar_data_somada(data, 1)), str(Parametros.retornar_data_somada(data, 2)), str(Parametros.retornar_data_somada(data, 3)), str(Parametros.retornar_data_somada(data, 4)), str(Parametros.retornar_data_somada(data, 5)), str(Parametros.retornar_data_somada(data, 6)), str(Parametros.retornar_data_somada(data, 7)), str(Parametros.retornar_data_somada(data, 8)), str(Parametros.retornar_data_somada(data, 9))] 
        return tabela_quantidade_agenda_disponivel_ocupado
    
    def retornar_quantidade_total_agendas_hub(self, data, hub):
        consulta_tabela_quantidade_total_agendas_hub = f"""   
        select sum(case a.dataincompleta = data::date when true then 1 else 0 end) as quant from (
        select to_char(DATE '{data}', 'YYYY/MM/DD')::date as data, macro_região as região, jeeo.data as dataincompleta, jeeo.id_colaborador as id_técnica, jeeo.colaborador as técnica
        from jornadas_escala.escala_operacional jeeo
        left join workstation.slots_abertos wsa
        on concat(wsa.data::text, wsa.id_tecnica::text) = concat(jeeo.data::text, jeeo.id_colaborador::text) 
        left join dim_parceiros
        on "HUB" = jeeo.hub
        where escala LIKE '%Técnica%'
        and jeeo.hub = '{hub}'
        and {self.filtro_status_lancamento_escala_app}
        and jeeo.data = '{data}'
        group by  macro_região, jeeo.hub, jeeo.escala, jeeo.data::date, jeeo.id_colaborador, jeeo.colaborador, jeeo.id_cargo, jeeo.data_inicio_previsto, jeeo.data_fim_previsto, wsa.tecnica, wsa."area") a 
        group by a.data
        """
        tabela_quantidade_agenda_disponivel_ocupado = Banco_de_dados.consulta('bi', consulta_tabela_quantidade_total_agendas_hub)
        if len(tabela_quantidade_agenda_disponivel_ocupado) == 0:
            quantidade_agenda_disponivel_ocupado = 0
        if len(tabela_quantidade_agenda_disponivel_ocupado) > 0:
            quantidade_agenda_disponivel_ocupado = tabela_quantidade_agenda_disponivel_ocupado.iloc[0]['quant']
        return quantidade_agenda_disponivel_ocupado

    def retornar_quantidade_contingencia(self, data, hub):
        quantidade_agendas_hub = Agenda().retornar_quantidade_total_agendas_hub(data, hub)     
        quantidade_contingencia = math.floor(float(quantidade_agendas_hub)/self.quantidade_tecnicas_para_uma_contingencia)
        return quantidade_contingencia

    def retornar_nome_agenda_contingencia(self, data, hub):
        quantidade_contingencia = Agenda().retornar_quantidade_contingencia(data, hub)
        tabela_hibridas_hub = Agenda().retornar_tabela_hibridas_hub(data, hub)
        nome_agenda_contingencia = tabela_hibridas_hub.head(quantidade_contingencia - 1)
        return nome_agenda_contingencia

    def registrar_agenda(self, data, produto, id_parceiro, area, hub, duracao, id_tecnica, tecnica, regime, inicio_regime, fim_regime):
        print('Você está abrindo slot no hub ' + hub + ' dentro da área: ' + area + '.')
        print(' O ID do parceiro da área é: ' + id_parceiro + '.')
        slot_atual_texto = str(inicio_regime)
        slot_atual_time = datetime.strptime(slot_atual_texto, "%H:%M:%S")
        fim_regime_texto = str(fim_regime)
        fim_regime_time = datetime.strptime(fim_regime_texto, "%H:%M:%S")
        trava_horario_minimo_abertura_slot = datetime.strptime('06:00:00', "%H:%M:%S")  
        trava_horario_maximo_abertura_slot = datetime.strptime('19:00:00', "%H:%M:%S")
        quantidade_slots = 0
        slots_agenda = []
        almoco = 0
        if (fim_regime_time > datetime.strptime("14:00:00", "%H:%M:%S")): 
            almoco = 1
        while(slot_atual_time < fim_regime_time - timedelta(hours=0, minutes=duracao+30, seconds=0)):
            slot_atual_time = slot_atual_time + timedelta(hours=0, minutes=duracao, seconds=0)
            if slot_atual_time >= datetime.strptime('11:30:00', "%H:%M:%S") and slot_atual_time < datetime.strptime('13:00:00', "%H:%M:%S") and almoco == 1:
                slot_atual_time = datetime.strptime('13:00:00', "%H:%M:%S")
            if slot_atual_time > trava_horario_minimo_abertura_slot and slot_atual_time < trava_horario_maximo_abertura_slot:
                slot_atual_texto = slot_atual_time.strftime('%H:%M:%S')
                print('Registrando slot ' + slot_atual_texto + ' dentro do array para abertura...')
                slots_agenda.append({"time": slot_atual_texto, "supplier_id": id_parceiro, "duration": duracao})
                quantidade_slots = quantidade_slots + 1
        print(tecnica)
        print('Consultando token...')
        token = Slots().retornar_token()
        print('Agenda registrada com sucesso.')
        Slots().abrirslots(f'{data}', f'{regime}', f'{produto}', id_parceiro, token, slots_agenda, f'{area}', f'{hub}', id_tecnica, tecnica)
 


class Area():
    def retornar_tabela_classificação_areas(self, hub, bu):
        bu_abreviado = Parametros.retornar_bu(bu, 'escala app')
        consultar_areas_classificadas = f"""
        select b.id_parceiro, b.hub, b.bu, b.área, b.classificação, b."nome_sinergia", b.id_sinergia from (
        select a.id_parceiro, a.hub, a.bu, a.área, case when a.área like '%Domingo%' then 7 when "nome_sinergia" like '%Domingo%' then 7 else a.classificação end as classificação, 
        case when ssa."nome_sinergia" is null then área when ssa."nome_sinergia" like '%desativado%' then área else ssa."nome_sinergia" end as "nome_sinergia" ,
        case when ssa.id_sinergia is null then 0 when ssa."nome_sinergia" like '%desativado%' then id_area else ssa.id_sinergia end as id_sinergia from (
        select id_sinergia::text as id_parceiro, "HUB" as hub, parceiro_tipo as bu, nome_sinergia as área, SUBSTRING("Categoria Sinergia" from 1 for 1)::int  as classificação from last_mile.analise_sinergias
        union
        select id_parceiro::text as id_parceiro, "HUB" as hub, parceiro_tipo as bu, parceiro_nome as área, SUBSTRING("Classificação" from 1 for 1)::int as classificação from last_mile.analise_areas ) a
        left join staging.sinergia_e_areas ssa
        on ssa.nome_area = a.área
        where a.hub = '{hub}' and a.bu = '{bu}' 
        and a.classificação >= 0 and a.classificação <= 6
        and a.área not like '%Global%' and a.área not like '%[%' and a.área not like '%]%' and a.área not like '%teste1%' and a.área not like '%[desativado]%' and a.área not like '%Beep%'
        group by a.id_parceiro, a.hub, a.bu, a.área, a.classificação, ssa.id_sinergia, "nome_sinergia", ssa.id_area
        order by a.área desc ) b
        where b."nome_sinergia" like '{bu_abreviado}' and b."nome_sinergia" not like '%Global%' and b."nome_sinergia" not like '%[%' and b."nome_sinergia" not like '%]%' and b."nome_sinergia" not like '%teste1%' and b."nome_sinergia" not like '%[desativado]%' and b."nome_sinergia" not like '%Beep%'
        group by nome_sinergia, id_parceiro, hub, bu, área, classificação, id_sinergia
        """
        tabela_areas_classificadas = Banco_de_dados.consulta('bi', consultar_areas_classificadas)
        return tabela_areas_classificadas

    def remover_areas_nao_utilizadas(self, tabela_areas_classificadas, classificacao_inicial, classificacao_final):
        quantidade_linhas_tabela_areas_classificadas = len(tabela_areas_classificadas)
        eixo_atual_tabela_areas_classificadas = 0
        tabela_areas_classificadas['status abertura'] = 0
        while quantidade_linhas_tabela_areas_classificadas > eixo_atual_tabela_areas_classificadas:
            celula_id_sinergia = str(tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 6])
            quantidade_areas_id_parceiro_igual_celula_id_sinergia = len(tabela_areas_classificadas[tabela_areas_classificadas['id_parceiro'] == celula_id_sinergia])
            if quantidade_areas_id_parceiro_igual_celula_id_sinergia >= 1:
                tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 7] = 0
            if (quantidade_areas_id_parceiro_igual_celula_id_sinergia == 0):
                tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 7] = 1
            if quantidade_areas_id_parceiro_igual_celula_id_sinergia <= 1 and str(tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 0]) == celula_id_sinergia:
                tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 7] = 1
            if tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 4] >= 5 and 'Sinergia' not in tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 3]:
                tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 7] = 1
            if tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 4] >= 5 and 'Sinergia' in tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 3]:
               tabela_areas_classificadas.iat[eixo_atual_tabela_areas_classificadas, 7] = 0
            eixo_atual_tabela_areas_classificadas = eixo_atual_tabela_areas_classificadas + 1
        remover_duplicado_tabela_areas_classificadas = tabela_areas_classificadas.drop_duplicates(subset='id_parceiro', keep='first')
        tabela_areas_classificadas = remover_duplicado_tabela_areas_classificadas[remover_duplicado_tabela_areas_classificadas['status abertura'] == 1]
        tabela_areas_classificadas = tabela_areas_classificadas[(tabela_areas_classificadas['classificação'] >= classificacao_inicial) &( tabela_areas_classificadas['classificação'] <= classificacao_final)]
        return tabela_areas_classificadas

    def retorna_tabela_prioridade_score_areas(self, data, região, bu):
        bu = Parametros.retornar_bu(bu, 'beep')
        consulta_areas_score = f"""
        select macro_região as região, lmsa."HUB" as hub,"Área" as área, max(SUBSTRING("Score"::text from 1 for 5)::numeric) as score, "Data da Agenda" as data
        from last_mile.sugestoes_alocacao lmsa
        left join dim_parceiros dp
        on dp."HUB" = lmsa."HUB"
        where "Turno da Agenda" = 'Manhã'
        and "BU da Agenda" = '{bu}'
        and "Data da Agenda" >= '{data}' and "Data da Agenda" <= to_char(DATE '{data}', 'YYYY/MM/DD')::date + interval '9 days'
        and macro_região = '{região}'
        group by macro_região, lmsa."HUB", "Área", "Data da Agenda"
        order by "Data da Agenda", lmsa."HUB", "Área"
        """
        tabela_prioridade_score_areas = Banco_de_dados.consulta('bi', consulta_areas_score)
        tabela_prioridade_score_areas_tratamento_colunas = pd.pivot_table(tabela_prioridade_score_areas, index=["região", "hub", "área"], columns=["data"], values=["score"])
        tabela_prioridade_score_areas = tabela_prioridade_score_areas_tratamento_colunas.set_axis(tabela_prioridade_score_areas_tratamento_colunas.columns.tolist(), axis=1).reset_index()
        tabela_prioridade_score_areas.columns = ['região', 'hub', 'área', str(Parametros.retornar_data_somada(data, 0)), str(Parametros.retornar_data_somada(data, 1)), str(Parametros.retornar_data_somada(data, 2)), str(Parametros.retornar_data_somada(data, 3)), str(Parametros.retornar_data_somada(data, 4)), str(Parametros.retornar_data_somada(data, 5)), str(Parametros.retornar_data_somada(data, 6)), str(Parametros.retornar_data_somada(data, 7)), str(Parametros.retornar_data_somada(data, 8)), str(Parametros.retornar_data_somada(data, 9))]
        return tabela_prioridade_score_areas

    def retorna_id_parceiro(self, parceiro_nome):
        consulta_tabela_parceiro = f"select parceiro_nome, id_parceiro from dim_parceiros"
        tabela_parceiro = Banco_de_dados.consulta('bi', consulta_tabela_parceiro)
        filtro_tabela_parceiro = tabela_parceiro[(tabela_parceiro['parceiro_nome'] == f'{parceiro_nome}')]
        id_parceiro = int(filtro_tabela_parceiro.iloc[0]['id_parceiro'])
        id_parceiro = json.dumps(id_parceiro)
        return id_parceiro

    def retorna_tabela_taxa_ocupacao_score_filtrada(self, hub, bu, data, taxa_ocupacao):
        bu = Parametros.retornar_bu(bu, 'escala app')
        consulta_tabela_taxa_ocupacao_score = f"""
        select a."HUB", a."slot_date", a."parceiro_nome", a."taxa_de_ocupacao", "Prioridade", "ID Área", "Taxa de Ocupação Simulada" from ( 
        select 
        dp."HUB"
        ,(spss.slot_date-interval '3 hours')::date as slot_date
        ,count(id) as total_slots
        ,sum(case when call_product_id is null then 0 else 1 end) as slots_ocupados
        ,sum(case when call_product_id is null then 0 else 1 end)/count(id)::numeric as taxa_de_ocupacao
        ,dp.parceiro_nome 
        ,dp.parceiro_ativo
        from sp_product_schedule_slots spss
        left join dim_parceiros dp 
        on dp.id_parceiro = spss.supplier_id 
        where slot_date::date = '{data}'
        and dp.parceiro_nome not like '%desativado%'
        and dp.parceiro_nome not like '%Domingo%'
        and dp.parceiro_nome not like '%Global%' and dp.parceiro_nome not like '%[%' and dp.parceiro_nome not like '%]%' and dp.parceiro_nome not like '%teste1%' and dp.parceiro_nome not like '%[desativado]%' and dp.parceiro_nome not like '%Beep%'
        and dp.parceiro_nome like '{bu}'
        and "HUB" = '{hub}'
        group by dp."HUB",(spss.slot_date-interval '3 hours')::date, dp.parceiro_tipo,dp.parceiro_nome,dp.parceiro_ativo) a
        left join last_mile.sugestoes_alocacao lmsa
        on concat(a."slot_date", a."parceiro_nome") = concat(lmsa."Data da Agenda",lmsa."Área")
        where lmsa."Turno da Agenda" = 'Manhã'
        order by "Prioridade" 
        """
        tabela_taxa_ocupacao_score = Banco_de_dados.consulta('bi', consulta_tabela_taxa_ocupacao_score)
        filtrar_taxa_ocupacao_tabela_score = tabela_taxa_ocupacao_score[tabela_taxa_ocupacao_score['taxa_de_ocupacao'] >= taxa_ocupacao]
        return filtrar_taxa_ocupacao_tabela_score

    def retorna_tabela_taxa_ocupacao_score_filtrada_sem_duplicada(self, hub, bu, data, taxa_ocupacao):
        bu = Parametros.retornar_bu(bu, 'escala app')
        consulta_tabela_taxa_ocupacao_score_sem_duplicada = f"""
        select "HUB", "Data da Agenda" as "slot_date", "Área" as "parceiro_nome", "Taxa de Ocupação Simulada" as "taxa_de_ocupacao", "Prioridade", "ID Área", "Taxa de Ocupação Simulada" from  last_mile.sugestoes_alocacao lmsa
        where "Data da Agenda" = '{data}' and "HUB" = '{hub}' and "Área" like '{bu}'
        and "Área" not like '%Domingo%' and lmsa."Turno da Agenda" = 'Manhã'
        and "Área" not like '%desativado%'
        and "Área" not like '%Domingo%'
        and "Área" not like '%Global%' and "Área" not like '%[%' and "Área" not like '%]%' and "Área" not like '%teste1%' and "Área" not like '%[desativado]%' and "Área" not like '%Beep%'
        order by "Prioridade"
        """
        tabela_taxa_ocupacao_score_sem_duplicada = Banco_de_dados.consulta('bi', consulta_tabela_taxa_ocupacao_score_sem_duplicada)
        tabela_taxa_ocupacao_score_sem_duplicada = tabela_taxa_ocupacao_score_sem_duplicada[tabela_taxa_ocupacao_score_sem_duplicada['taxa_de_ocupacao'] >= taxa_ocupacao]
        return tabela_taxa_ocupacao_score_sem_duplicada

class Dashboard():
    def retorna_lista_macro_regiao(self):
        consulta_macro_regiao = f"select macro_região from dim_parceiros where macro_região is not null group by macro_região"
        tabela_macro_regiao = Banco_de_dados.consulta('bi', consulta_macro_regiao)
        lista_macro_regiao = tabela_macro_regiao['macro_região'].values
        return lista_macro_regiao

    def retorna_lista_bu(self):
        consulta_bu = f"""
        select a.bu from ( select ( case 
        when parceiro_nome LIKE '%VAC%' then 'vaccines' 
        when parceiro_nome LIKE '%LAB%' then 'laboratories' end
        ) as bu 
        from dim_parceiros 
        group by bu ) a
        where a.bu is not null
        """
        tabela_bu = Banco_de_dados.consulta('bi', consulta_bu)
        lista_bu = tabela_bu['bu'].values
        return lista_bu

# area = Area()
# teste = area.retornar_tabela_classificação_areas('São Cristóvão', 'vaccines')
# print(teste)
# print(area.remover_areas_nao_utilizadas(teste, 3, 6))
# agenda = Agenda()
# print(agenda.retornar_tabela_agendas_alteradas_escala_app('2022-12-08', 'Tatuapé', 'vaccines'))
# print(agenda.retornar_tabela_agendas_escala_app('2022-12-11','Vila Olímpia','vaccines',1))
# print(agenda.retornar_tabela_agendas_escala_app('2022-12-19','São Cristóvão','vaccines',0))
# agenda.fechar_agendas('2022-12-11', 'Vila Olímpia', 'vaccines')