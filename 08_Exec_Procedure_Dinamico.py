
import os
from dotenv import load_dotenv
import pyodbc
import pandas as pd
import time
from datetime import datetime

# Carregar variáveis de ambiente a partir do arquivo .env
load_dotenv()

# Usar as variáveis de ambiente no seu código
DB_DRIVER = os.getenv("DB_DRIVER")
DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configurar a conexão com banco de dados
conn_str = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s' % (DB_SERVER, DB_DATABASE, DB_USERNAME, DB_PASSWORD)
conn = pyodbc.connect(conn_str) #Estabelecendo conexão
print ('conectado com sucesso')

# Crie um cursor para executar consultas SQL
crsr = conn.cursor()
print("Conector criado")

crsr.execute("select substring(rtrim(convert(varchar(255),convert(date,dt_campo))),1,4)+','+substring(rtrim(convert(varchar(255), convert(date,dt_campo))),6,2)+','+ substring(rtrim(convert(varchar(255),convert(date,dt_campo))),9,2) from tsv_server_status where cd_campo = 4")
resultado = crsr.fetchone()

# Exemplo de parâmetro do tipo data (substitua "SuaProcedure" pelo nome real da sua procedure)
data_base = resultado[0]

# Agora, data_convertida é um objeto datetime em Python
print("Data convertida:", data_base)

data_param = datetime.strptime(data_base, "%Y,%m,%d")
print("Data convertida:", data_param)

crsr.execute("{CALL spcl_carga_clientes (?)}",(data_param,))

# Faça commit para efetivar as alterações no banco de dados
conn.commit()
print("Procedure executada com sucesso")


# Feche o cursor e a conexão

conn.close()

