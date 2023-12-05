
import pymssql

# Configuração da conexão com o SQL Server
server = 'VILA-SRV2.fatoti.com.br\SQL2014'
user = 'sa'
password = '08NTVoCWUzT4Cbq5VYEp'
database = 'SircoiMSBank'

# Estabelecendo a conexão
conn = pymssql.connect(server, user, password, database)

# Criando um objeto cursor para executar comandos SQL
cursor = conn.cursor()

# Consulta SQL para deletar dados
delete_sql = 'DELETE FROM dbo.ttp_cliente_brazauk'

# Executando o comando SQL
cursor.execute(delete_sql)

# Commit das alterações
conn.commit()

# Fechando o cursor e a conexão
cursor.close()
conn.close()