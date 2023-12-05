# Se você quiser criar uma variável que realiza a leitura de uma tabela do SQL Server e, em seguida, 
#utiliza os dados dessa tabela para montar o nome do arquivo a ser lido em Python, você pode seguir
#estes passos. Vou usar a biblioteca 'pandas' e 'pyodbc' para conectar-se ao SQL Server e realizar
#a leitura dos dados.

#Certifique-se de ter as bibliotecas instaladas antes de executar o código:

# Importando os pacotes que serão utilizados
import pandas as pd
import pymssql as sql
import time
import warnings
warnings.filterwarnings("ignore")

# Configuração da conexão com o SQL Server

server = 'server'
user = 'user'
password = 'password'
database = 'database'

# Estabelecendo a conexão
conn = sql.connect(server, user, password, database)

# Criando um objeto cursor para executar consultas
cursor = conn.cursor()

# Consulta SQL que você deseja executar
consulta_sql = 'select (select rtrim(convert(varchar(255),vl_campo)) from dbo.tsv_server_status_3 where cd_campo = 3) + (select  rtrim(convert(varchar(255),vl_campo)) from dbo.tsv_server_status_3 where cd_campo = 27) + (select substring(rtrim(convert(varchar(255),convert(date,dt_campo))),1,4) + substring(rtrim(convert(varchar(255),convert(date,dt_campo))),6,2) + substring(rtrim(convert(varchar(255),convert(date,dt_campo))),9,2) from tsv_server_status_3 where cd_campo = 4) + (select rtrim(convert(varchar(255), vl_campo)) as extension from dbo.tsv_server_status_3 where cd_campo = 24) as caminho_arquivo'

# Executando a consulta
cursor.execute(consulta_sql)

# Obtendo os resultados da consulta
dados_sql = pd.read_sql(consulta_sql, conn)

# Agora você tem os dados da consulta SQL no DataFrame 'dados_sql'

# Montando o nome do arquivo com base nos dados da consulta
# Vamos supor que você tenha uma coluna chamada 'nome_arquivo' na sua consulta

indice_linha = 0
nome_arquivo_a_ser_lido = dados_sql.loc[indice_linha, 'caminho_arquivo']

# Se desejar, forneça nomes de colunas personalizados usando names
nomes_colunas = ['cd_cliente_legado', 'nm_cliente', 'cd_cpf_cnpj', 'cd_passaporte', 'tp_pessoa',
                'id_pep', 'dt_cadastro']  # Substitua pelos nomes desejados

# Agora, você pode usar o nome do arquivo para realizar a leitura.
dados_lidos = pd.read_csv(nome_arquivo_a_ser_lido, sep=';', quotechar='"', encoding='utf-8', header=None, names=nomes_colunas)

inicio = time.time()
for index,row in dados_lidos.iterrows():
    sql = "INSERT INTO ttp_cliente_brazauk (cd_cliente_legado, nm_cliente, cd_cpf_cnpj, tp_pessoa, id_pep, dt_cadastro) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (row['cd_cliente_legado'], row['nm_cliente'], row['cd_cpf_cnpj'], row['tp_pessoa'], row['id_pep'], row['dt_cadastro'])    
    cursor.execute(sql, val)
    conn.commit()
    
final = time.time()    
#conexao.close()
print("Dados inseridos com sucesso no SQL")    
print('Tempo de Processamento:', int(final - inicio),'segundos')   

# Fechando o cursor e a conexão
cursor.close()
conn.close()
