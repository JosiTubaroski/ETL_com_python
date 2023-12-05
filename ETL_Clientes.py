# Importando as bibliotecas que serão utilizados
import pandas as pd
import pymssql as sql
import time
import warnings
warnings.filterwarnings("ignore")

#Cria a conexão com o SQL Server passando os parametros (Servidor, Usuário, Senha, Database)
conexao = sql.connect('Servidor', 'usuario', 'senha', 'database')

# Substitua 'seu_arquivo.csv' pelo caminho do seu arquivo CSV
caminho_arquivo = 'C:\Arquivos\cliente_20230313.csv'

# Incluir nomes de colunas personalizados usando names
nomes_colunas = ['cd_cliente_legado', 'nm_cliente', 'cd_cpf_cnpj', 'cd_passaporte', 'tp_pessoa',
                'id_pep', 'dt_cadastro']  # Substitua pelos nomes desejados

df_produtos = pd.read_csv(caminho_arquivo, sep=';', quotechar='"', encoding='utf-8', header=None, names=nomes_colunas)

# Criando um cursor e executando um LOOP no DataFrame para fazer o INSERT no banco SQL Server

cursor = conexao.cursor()

inicio = time.time()
for index,row in df_produtos.iterrows():
    sql = "INSERT INTO ttp_cliente_brazauk (cd_cliente_legado, nm_cliente, cd_cpf_cnpj, tp_pessoa, id_pep, dt_cadastro) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (row['cd_cliente_legado'], row['nm_cliente'], row['cd_cpf_cnpj'], row['tp_pessoa'], row['id_pep'], row['dt_cadastro'])    
    cursor.execute(sql, val)
    conexao.commit()
    
final = time.time()    
#conexao.close()
print("Dados inseridos com sucesso no SQL")    
print('Tempo de Processamento:', int(final - inicio),'segundos')   

#Fecha conexão com banco de dados
conexao.close()
