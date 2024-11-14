from fastapi import FastAPI
from datetime import time, datetime
from math import floor
import pymysql.cursors

app = FastAPI(docs_url="/api/py/docs", openapi_url="/api/py/openapi.json")

@app.get("/clientes")
async def get_costumer(stablishment_name: str = ""):
    connection = pymysql.connect(
        host="db1.rizer.com.br",
        password="oYIT9mHAg1",
        user="31938_msmbdpafjl",
        database="database_3193832354",
        cursorclass=pymysql.cursors.DictCursor
    )
    
    stablishments = (
        "IGUATEMI SÃO PAULO",
        "JK IGUATEMI",
        "ASSAI TIRADENTES",
        "CD ANHANGUERA",
        "CD ARUJA",
        "CD CAJAMAR",
        "CANTAREIRA NORTE",
        "GRAND PLAZA",
        "JARDIM SUL",
        "PARK SÃO CAETANO",
        "VILA OLIMPIA",
        "WEST PLAZA",
        "SHOPPING D",
        "SHOPPING TIETÊ",
        "CIDADE JARDIM",
        "CAMPO LIMPO",
        "SHOPS",
        "AREA PROTEGIDA",
        "SHOPPING SUZANO",
        "CIDADE SÃO PAULO",
        "EVENTO"
    )
    
    sql = """select 
    ficha_ambulatorial.id,

    empresas.nome_fantasia,

    ficha_ambulatorial.horario_de_entrada,
    ficha_ambulatorial.horario_de_saida,
    ficha_ambulatorial.data,
    ficha_ambulatorial.area_protegida,

    veiculos.prefixo,
    ficha_ambulatorial.medico,
    enfermeiros.nome_do_enfermeiro,

    ficha_ambulatorial.nome_do_paciente,
    ficha_ambulatorial.data_de_nascimento_do_paciente,
    ficha_ambulatorial.idade_do_paciente,
    ficha_ambulatorial.sexo_do_paciente,
    ficha_ambulatorial.lojista,

    cadastro_de_lojas_quiosques.razao_social_lq,

    ficha_ambulatorial.acompanhante,
    ficha_ambulatorial.hipotese_diagnostica,
    ficha_ambulatorial.classificacao_de_risco,
    ficha_ambulatorial.medicado,

    categoria_de_ocorrencia.nome_da_categoria,
    sub_categoria.nome_da_sub_categoria,

    ficha_ambulatorial.descricao_do_local,
    ficha_ambulatorial.remocao,
    ficha_ambulatorial.destino,
    ficha_ambulatorial.logradouro_do_destino,
    ficha_ambulatorial.km_origem_ida,
    ficha_ambulatorial.km_destino_no_destino,
    ficha_ambulatorial.km_origem_volta,

    ficha_ambulatorial.recusa_de_atendimento,
    ficha_ambulatorial.translado_por_meios_proprios

from ficha_ambulatorial
inner join empresas on ficha_ambulatorial.empresa_de_atendimento = empresas.id
inner join veiculos on ficha_ambulatorial.prefixo = veiculos.id
inner join categoria_de_ocorrencia on ficha_ambulatorial.nome_da_categoria = categoria_de_ocorrencia.id
inner join sub_categoria on ficha_ambulatorial.nome_da_sub_categoria = sub_categoria.id
inner join cadastro_de_lojas_quiosques on ficha_ambulatorial.loja_quiosque = cadastro_de_lojas_quiosques.id
inner join enfermeiros on ficha_ambulatorial.enfermagem = enfermeiros.id
where empresas.nome_fantasia = "{}";"""
    
    if not stablishment_name.upper() in stablishments:
        info = {}
        return info
    
    cursor = connection.cursor()
    
    cursor.execute(sql.format(stablishment_name.upper()))
    
    info = cursor.fetchall()
    records = []
    
    for record in info:
        # Área protegida
        if not record["area_protegida"]: record["area_protegida"] = ""
        
        # KM percorridos e Combustível em litros
        if not record["km_origem_ida"]:
            record["km_origem_ida"] = 0
            
        if not record["km_origem_volta"]:
            record["km_origem_volta"] = 0
            
        if not record["km_destino_no_destino"]:
            record["km_destino_no_destino"] = 0
        
        if record["km_origem_ida"] and record["km_origem_volta"]:
            record["km_percorridos"] = int(record["km_origem_volta"]) - int(record["km_origem_ida"])
            record["combustivel_em_litros"] = record["km_percorridos"] / 5

        else:
            record["km_percorridos"] = 0
            record["combustivel_em_litros"] = 0
        
        # Médico
        if not record["medico"]: record["medico"] = ""
        
        # Sexo do paciente
        if not record["sexo_do_paciente"]: record["sexo_do_paciente"] = ""
        
        else:
            match int(record["sexo_do_paciente"]):
                case 1:
                    record["sexo_do_paciente"] = "FEMININO"
                
                case 2: 
                    record["sexo_do_paciente"] = "MASCULINO"

        # Lojista
        if not record["lojista"]: record["lojista"] = ""
        
        else:
            match int(record["lojista"]):
                case 1:
                    record["lojista"] = "CLIENTE"
                
                case 2:
                    record["lojista"] = "LOJISTA"
        
        # Acompanhante
        if not record["acompanhante"]:
            record["acompanhante"] = "NÃO INFORMADO"
            
            
        # Classificação de risco
        
        if not record["classificacao_de_risco"]: record["classificacao_de_risco"] = ""
        
        else:
            match int(record["classificacao_de_risco"]):
                case 1:
                    record["classificacao_de_risco"] = "VERDE"
                
                case 2:
                    record["classificacao_de_risco"] = "AMARELO"
                
                case 3:
                    record["classificacao_de_risco"] = "VERMELHO"

        # Permanência
        if record["horario_de_entrada"] and record["horario_de_saida"]:
            start = datetime.combine(datetime.now(), time.fromisoformat(record["horario_de_entrada"]))
            end = datetime.combine(datetime.now(), time.fromisoformat(record["horario_de_saida"]))
            
            record["permanencia"] = '{:02d}:{:02d}'.format(*divmod(floor((end - start).total_seconds() // 60), 60))
        else:
            record["permanencia"] = 0

        # Medicado
        if not record["medicado"]: record["medicado"] = ""
        
        else:
            match int(record["medicado"]):
                case 1:
                    record["medicado"] = "MEDICADO"
                
                case 2:
                    record["medicado"] = "NÃO MEDICADO"
        
        # Descrição do local?
        if not record["descricao_do_local"]: record["descricao_do_local"] = ""
        
        # Remoção
        if not record["remocao"]: record["remocao"] = ""
        
        else:
            match int(record["remocao"]):
                case 1:
                    record["remocao"] = "REMOVIDO"
                
                case 2:
                    record["remocao"] = "NÃO REMOVIDO"
                    
        
        # Destino
        if not record["destino"]: record["destino"] = ""
        
        # Logradouro do destino
        if not record["logradouro_do_destino"]: record["logradouro_do_destino"] = ""
        
        # Recusa de atendimento
        if not record["recusa_de_atendimento"]: record["recusa_de_atendimento"] = ""
        
        else:
            match int(record["recusa_de_atendimento"]):
                case 1:
                    record["recusa_de_atendimento"] = "RECUSOU ATENDIMENTO"
                
                case 2:
                    record["recusa_de_atendimento"] = "NÃO RECUSOU ATENDIMENTO"
                    
        
        # Translado por meios próprios
        if not record["translado_por_meios_proprios"]: record["translado_por_meios_proprios"] = ""
        
        else:
            match int(record["translado_por_meios_proprios"]):
                case 1:
                    record["translado_por_meios_proprios"] = "ASSINOU TERMO DE TRANSLADO"
                
                case 2:
                    record["translado_por_meios_proprios"] = "NÃO ASSINOU TERMO DE TRANSLADO"
                    
        records.append( {
            "ID DA FICHA": record["id"],
            "AMBULATÓRIO": record["nome_fantasia"],
            "ENTRADA DO PACIENTE": record["horario_de_entrada"],
            "SAÍDA DO PACIENTE": record["horario_de_saida"],
            "PERMANENCIA DO PACIENTE": record["permanencia"],
            "DATA DO ATENDIMENTO": record["data"],
            "AREA PROTEGIDA": record["area_protegida"],
            "PREFIXO DA VTR": record["prefixo"],
            "ENFERMEIRO(A)": record["nome_do_enfermeiro"],
            "NOME DO PACIENTE": record["nome_do_paciente"],
            "DATA DE NASCIMENTO": record["data_de_nascimento_do_paciente"],
            "IDADE DO PACIENTE": record["idade_do_paciente"],
            "SEXO DO PACIENTE": record["sexo_do_paciente"],
            "CATEGORIA DO PACIENTE": record["lojista"],
            "LOJA/QUIOSQUE/TERCEIRIZADO": record["razao_social_lq"],
            "NOME DO ACOMPANHANTE": record["acompanhante"],
            "HIPÓTESE DIAGNÓSTICA": record["hipotese_diagnostica"],
            "CLASSIFICAÇÃO DE RISCO": record["classificacao_de_risco"],
            "PACIENTE MEDICADO?": record["medicado"],
            "CATEGORIA DO ATENDIMENTO": record["nome_da_categoria"],
            "SUB CATEGORIA DO ATENDIMENTO": record["nome_da_sub_categoria"],
            "DESCRIÇÃO DO LOCAL": record["descricao_do_local"],
            "REMOÇÃO?": record["remocao"],
            "DESTINO": record["destino"],
            "LOGRADOURO DE DESTINO": record["logradouro_do_destino"],
            "KM DE ORIGEM": record["km_origem_ida"],
            "KM NO DESTINO": record["km_destino_no_destino"],
            "KM DE VOLTA": record["km_origem_volta"],
            "KM PERCORRIDOS": record["km_percorridos"],
            "COMBUSTIVEL EM LITROS": record["combustivel_em_litros"],
            "TERMO DE RECUSA": record["recusa_de_atendimento"],
            "TERMO DE TRANSLADO": record["translado_por_meios_proprios"]
        })

    return records
