# -*- coding: utf-8 -*-
import sqlalchemy
from sqlalchemy.dialects import registry
import petl as etl
import configparser
import time

registry.register("postgresql.teiid", "sqlalchemy_teiid", "TeiidDialect")

# Carrega as credenciais
config = configparser.ConfigParser()
config.read(".ignore/config.ini")

# Conexão com o Quartzo
quartzo = sqlalchemy.create_engine(
    "postgresql+teiid://{}:{}@daas.serpro.gov.br:35432/NovoSCDP".format(
        config["Quartzo"]["user"], config["Quartzo"]["password"]
    ),
    executemany_mode="batch",
    server_side_cursors=True,
)

sqlserver = sqlalchemy.create_engine(
    "mssql+pymssql://{}:{}@10.209.42.30:1433/datalake_scdp?charset=utf8".format(
        config["Sqlserver"]["user"], config["Sqlserver"]["password"]
    )
)

postgres = sqlalchemy.create_engine(
    "postgresql+psycopg2://{}:{}@10.209.9.236:5432/pesq".format(
        config["Postgres"]["user"], config["Postgres"]["password"]
    ),
    executemany_mode="batch",
    server_side_cursors=True,
)


query = """SELECT id,
           classe_voo_vl,
           condicoes,
           data_chegada_destino,
           data_fim_permanencia,
           data_hora_inicio_trabalho,
           data_inicio_permanencia,
           data_ultima_atualizacao_registro,
           meio_transporte_vl,
           percentual_diarias_vl,
           posicao,
           quantidade_km,
           tem_adicional_embarque_desembarque,
           tem_passagens,
           tipo_etapa_roteiro_vl,
           valor_adicional_embarque_desembarque,
           id_cidade_destino,
           id_cidade_origem,
           id_planejamento,
           id_empenho,
           id_ultimo_empenho,
           valor_teto_reservado,
           hospedagem_disponibilizada_adm_publica,
           habilitado_cotacao,
           usa_pcdp_grupo,
           id_trajeto,
           id_bilhete,
           data_hora_inclusao_alteracao
           FROM NovoSCDP_VBL.trecho
           WHERE id BETWEEN 17 and 200"""


trecho_qtz = etl.fromdb(quartzo, query)

start_time = time.time()
etl.todb(trecho_qtz, sqlserver, "trecho")
print("--- {} seconds ---".format(time.time() - start_time))

# engine.connect()
# engine = sqlalchemy.create_engine(sqlserver)

