import psycopg2
import pandas as pd
import uuid

# Conectar ao banco de dados PostgreSQL
# Configura a conexão com o banco de dados PostgreSQL usando as credenciais fornecidas
conn = psycopg2.connect(
    dbname="seu_banco",
    user="seu_usuario",
    password="sua_senha",
    host="localhost",
    port="5432"
)
cursor = conn.cursor()

# Dicionários globais para armazenar IDs
# Esses dicionários são usados para mapear nomes ou códigos para IDs gerados no banco de dados
regioes_dict = {}
ufs_dict = {}
municipios_dict = {}
escolas_dict = {}

# Função para criar o esquema
# Cria as tabelas no banco de dados conforme o esquema definido
def criar_esquema():
    schema_sql = '''
    -- Define as tabelas do banco de dados, incluindo chaves primárias, estrangeiras e restrições
    -- 1. Tabela Regiao
    CREATE TABLE "Regiao" (
        "ID_REGIAO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_REGIAO" VARCHAR(50) NOT NULL,
        "POPULACAO_TOTAL" INT,
        "POPULACAO_INDIGENA" INT
    );

    -- 2. Tabela Unidade_Federativa
    CREATE TABLE "Unidade_Federativa" (
        "ID_UF" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_UF" VARCHAR(50) NOT NULL,
        "SIGLA_UF" CHAR(2) NOT NULL,
        "ID_REGIAO" INT NOT NULL,
        "POPULACAO_TOTAL" INT,
        "POPULACAO_INDIGENA" INT,
        FOREIGN KEY ("ID_REGIAO") REFERENCES "Regiao"("ID_REGIAO")
    );

    -- 3. Tabela Municipio
    CREATE TABLE "Municipio" (
        "ID_MUNICIPIO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_MUNICIPIO" VARCHAR(100) NOT NULL,
        "ID_UF" INT NOT NULL,
        "POPULACAO_TOTAL" INT,
        "POPULACAO_INDIGENA" INT,
        FOREIGN KEY ("ID_UF") REFERENCES "Unidade_Federativa"("ID_UF")
    );

    -- 4. Tabela Escola
    CREATE TABLE "Escola" (
        "ID_ESCOLA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_ESCOLA" VARCHAR(100) NOT NULL,
        "ID_MUNICIPIO" INT NOT NULL,
        "TIPO_DEPENDENCIA" VARCHAR(20) NOT NULL,
        "TIPO_LOCALIZACAO" VARCHAR(20) NOT NULL,
        "SITUACAO_FUNCIONAMENTO" VARCHAR(20) NOT NULL DEFAULT 'Ativa',
        "INDIGENA" BOOLEAN NOT NULL DEFAULT FALSE,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO")
    );

    -- 5. Tabela Turma
    CREATE TABLE "Turma" (
        "ID_TURMA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_ESCOLA" INT NOT NULL,
        "NIVEL_ENSINO" VARCHAR(20) NOT NULL,
        "QT_TURMAS" INT NOT NULL DEFAULT 0,
        "QT_TURMAS_INDIGENAS" INT DEFAULT 0,
        FOREIGN KEY ("ID_ESCOLA") REFERENCES "Escola"("ID_ESCOLA")
    );

    -- 6. Tabela Matricula
    CREATE TABLE "Matricula" (
        "ID_MATRICULA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_ESCOLA" INT NOT NULL,
        "NIVEL_ENSINO" VARCHAR(20) NOT NULL,
        "QT_MATRICULAS_TOTAL" INT NOT NULL DEFAULT 0,
        "QT_MATRICULAS_INDIGENAS" INT DEFAULT 0,
        "ANO_REFERENCIA" INT NOT NULL,
        FOREIGN KEY ("ID_ESCOLA") REFERENCES "Escola"("ID_ESCOLA")
    );

    -- 7. Tabela Frequencia_Escolar
    CREATE TABLE "Frequencia_Escolar" (
        "ID_FREQUENCIA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_MUNICIPIO" INT NOT NULL,
        "FAIXA_ETARIA" VARCHAR(20) NOT NULL,
        "TAXA_FREQUENCIA" DECIMAL(5,2) NOT NULL,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO"),
        CONSTRAINT "check_taxa_frequencia" CHECK ("TAXA_FREQUENCIA" BETWEEN 0 AND 100)
    );

    -- 8. Tabela Nivel_Instrucao
    CREATE TABLE "Nivel_Instrucao" (
        "ID_NIVEL_INSTRUCAO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_MUNICIPIO" INT NOT NULL,
        "FAIXA_ETARIA" VARCHAR(20) NOT NULL,
        "NIVEL" VARCHAR(30) NOT NULL,
        "QT_PESSOAS" INT NOT NULL DEFAULT 0,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO")
    );

    -- 9. Tabela Anos_Estudo
    CREATE TABLE "Anos_Estudo" (
        "ID_ANOS_ESTUDO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_MUNICIPIO" INT NOT NULL,
        "FAIXA_ETARIA" VARCHAR(20) NOT NULL,
        "MEDIA_ANOS_ESTUDO" DECIMAL(3,1) NOT NULL,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO"),
        CONSTRAINT "check_media_anos" CHECK ("MEDIA_ANOS_ESTUDO" BETWEEN 0 AND 20)
    );

    -- 10. Tabela Territorio_Indigena
    CREATE TABLE "Territorio_Indigena" (
        "ID_TERRITORIO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_UF" INT NOT NULL,
        "NOME_TERRITORIO" VARCHAR(100) NOT NULL,
        "ETNIA_DOMINANTE" VARCHAR(50),
        "AREA" DECIMAL(12,2),
        "POP_TOTAL" INT,
        FOREIGN KEY ("ID_UF") REFERENCES "Unidade_Federativa"("ID_UF"),
        CONSTRAINT "check_area" CHECK ("AREA" >= 0),
        CONSTRAINT "check_pop_total" CHECK ("POP_TOTAL" >= 0)
    );
    '''
    try:
        # Remove o esquema existente e cria um novo
        cursor.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        cursor.execute(schema_sql)
        conn.commit()
        print("Esquema criado com sucesso.")
    except Exception as e:
        # Trata erros e desfaz alterações em caso de falha
        print(f"Erro ao criar esquema: {e}")
        conn.rollback()

# Função para carregar e processar o CSV do Censo Escolar
# Lê os dados do arquivo CSV e insere nas tabelas do banco de dados
def carregar_csv_censo():
    try:
        # Lê o arquivo CSV com os dados do censo escolar
        df = pd.read_csv('microdados_censo_escolar_2023.csv', low_memory=False)

        # Tratar valores nulos e tipos de dados
        # Substitui valores nulos por padrões e ajusta os tipos de dados
        df = df.fillna({
            'QT_MAT_BAS': 0,
            'QT_MAT_BAS_INDIGENA': 0,
            'IN_EDUCACAO_INDIGENA': 0,
            'TP_DEPENDENCIA': '4',
            'TP_LOCALIZACAO': '1',
            'TP_SITUACAO_FUNCIONAMENTO': '1',
            'QT_TUR_INF': 0,
            'QT_TUR_FUND': 0,
            'QT_TUR_MED': 0,
            'QT_TUR_EJA': 0,
            'IN_INF': 0,
            'IN_FUND_AI': 0,
            'IN_FUND_AF': 0,
            'IN_MED': 0,
            'IN_EJA': 0
        })
        df['IN_EDUCACAO_INDIGENA'] = df['IN_EDUCACAO_INDIGENA'].astype(bool)

        # Mapear valores
        # Converte códigos numéricos para valores descritivos
        df['TP_DEPENDENCIA'] = df['TP_DEPENDENCIA'].map({
            1: 'Federal', 2: 'Estadual', 3: 'Municipal', 4: 'Privada'
        })
        df['TP_LOCALIZACAO'] = df['TP_LOCALIZACAO'].map({
            1: 'Urbana', 2: 'Rural'
        })
        df['TP_SITUACAO_FUNCIONAMENTO'] = df['TP_SITUACAO_FUNCIONAMENTO'].map({
            1: 'Ativa', 2: 'Inativa'
        })

        # 1. Regiao
        # Agrupa os dados por região e insere na tabela "Regiao"
        regioes = df.groupby('NO_REGIAO').agg({
            'QT_MAT_BAS': 'sum',
            'QT_MAT_BAS_INDIGENA': 'sum'
        }).reset_index()
        for _, row in regioes.iterrows():
            cursor.execute(
                'INSERT INTO "Regiao" ("NOME_REGIAO", "POPULACAO_TOTAL", "POPULACAO_INDIGENA") VALUES (%s, %s, %s) RETURNING "ID_REGIAO"',
                (row['NO_REGIAO'], int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']))
            )
            regioes_dict[row['NO_REGIAO']] = cursor.fetchone()[0]

        # 2. Unidade_Federativa
        # Agrupa os dados por unidade federativa e insere na tabela "Unidade_Federativa"
        ufs = df.groupby(['SG_UF', 'NO_UF', 'NO_REGIAO']).agg({
            'QT_MAT_BAS': 'sum',
            'QT_MAT_BAS_INDIGENA': 'sum'
        }).reset_index()
        for _, row in ufs.iterrows():
            id_regiao = regioes_dict[row['NO_REGIAO']]
            cursor.execute(
                'INSERT INTO "Unidade_Federativa" ("NOME_UF", "SIGLA_UF", "ID_REGIAO", "POPULACAO_TOTAL", "POPULACAO_INDIGENA") VALUES (%s, %s, %s, %s, %s) RETURNING "ID_UF"',
                (row['NO_UF'], row['SG_UF'], id_regiao, int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']))
            )
            ufs_dict[row['SG_UF']] = cursor.fetchone()[0]

        # 3. Municipio
        # Agrupa os dados por município e insere na tabela "Municipio"
        municipios = df.groupby(['CO_MUNICIPIO', 'NO_MUNICIPIO', 'SG_UF']).agg({
            'QT_MAT_BAS': 'sum',
            'QT_MAT_BAS_INDIGENA': 'sum'
        }).reset_index()
        for _, row in municipios.iterrows():
            id_uf = ufs_dict[row['SG_UF']]
            cursor.execute(
                'INSERT INTO "Municipio" ("NOME_MUNICIPIO", "ID_UF", "POPULACAO_TOTAL", "POPULACAO_INDIGENA") VALUES (%s, %s, %s, %s) RETURNING "ID_MUNICIPIO"',
                (row['NO_MUNICIPIO'], id_uf, int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']))
            )
            municipios_dict[row['CO_MUNICIPIO']] = cursor.fetchone()[0]

        # 4. Escola
        # Processa os dados das escolas e insere na tabela "Escola"
        escolas = df[['CO_ENTIDADE', 'NO_ENTIDADE', 'CO_MUNICIPIO', 'TP_DEPENDENCIA', 'TP_LOCALIZACAO', 'TP_SITUACAO_FUNCIONAMENTO', 'IN_EDUCACAO_INDIGENA']].drop_duplicates()
        for _, row in escolas.iterrows():
            id_municipio = municipios_dict[row['CO_MUNICIPIO']]
            cursor.execute(
                'INSERT INTO "Escola" ("NOME_ESCOLA", "ID_MUNICIPIO", "TIPO_DEPENDENCIA", "TIPO_LOCALIZACAO", "SITUACAO_FUNCIONAMENTO", "INDIGENA") VALUES (%s, %s, %s, %s, %s, %s) RETURNING "ID_ESCOLA"',
                (row['NO_ENTIDADE'], id_municipio, row['TP_DEPENDENCIA'], row['TP_LOCALIZACAO'], row['TP_SITUACAO_FUNCIONAMENTO'], row['IN_EDUCACAO_INDIGENA'])
            )
            escolas_dict[row['CO_ENTIDADE']] = cursor.fetchone()[0]

        # 5. Turma
        # Processa os dados das turmas por nível de ensino e insere na tabela "Turma"
        niveis_ensino = ['Infantil', 'Fundamental', 'Médio', 'EJA']
        for co_entidade in escolas_dict:
            escola_data = df[df['CO_ENTIDADE'] == co_entidade]
            if escola_data.empty:
                continue
            for nivel in niveis_ensino:
                qt_turmas = 0
                qt_turmas_indigenas = 0
                if nivel == 'Infantil' and escola_data['QT_TUR_INF'].iloc[0] > 0:
                    qt_turmas = escola_data['QT_TUR_INF'].iloc[0]
                    qt_turmas_indigenas = qt_turmas if escola_data['IN_EDUCACAO_INDIGENA'].iloc[0] else 0
                elif nivel == 'Fundamental' and escola_data['QT_TUR_FUND'].iloc[0] > 0:
                    qt_turmas = escola_data['QT_TUR_FUND'].iloc[0]
                    qt_turmas_indigenas = qt_turmas if escola_data['IN_EDUCACAO_INDIGENA'].iloc[0] else 0
                elif nivel == 'Médio' and escola_data['QT_TUR_MED'].iloc[0] > 0:
                    qt_turmas = escola_data['QT_TUR_MED'].iloc[0]
                    qt_turmas_indigenas = qt_turmas if escola_data['IN_EDUCACAO_INDIGENA'].iloc[0] else 0
                elif nivel == 'EJA' and escola_data['QT_TUR_EJA'].iloc[0] > 0:
                    qt_turmas = escola_data['QT_TUR_EJA'].iloc[0]
                    qt_turmas_indigenas = qt_turmas if escola_data['IN_EDUCACAO_INDIGENA'].iloc[0] else 0
                if qt_turmas > 0:
                    cursor.execute(
                        'INSERT INTO "Turma" ("ID_ESCOLA", "NIVEL_ENSINO", "QT_TURMAS", "QT_TURMAS_INDIGENAS") VALUES (%s, %s, %s, %s)',
                        (escolas_dict[co_entidade], nivel, int(qt_turmas), int(qt_turmas_indigenas))
                    )

        # 6. Matricula
        # Processa os dados de matrículas e insere na tabela "Matricula"
        matriculas = df[['CO_ENTIDADE', 'QT_MAT_BAS', 'QT_MAT_BAS_INDIGENA', 'NU_ANO_CENSO', 'IN_INF', 'IN_FUND_AI', 'IN_FUND_AF', 'IN_MED', 'IN_EJA']].drop_duplicates()
        for _, row in matriculas.iterrows():
            id_escola = escolas_dict[row['CO_ENTIDADE']]
            niveis = []
            if row['IN_INF'] == 1:
                niveis.append('Infantil')
            if row['IN_FUND_AI'] == 1 or row['IN_FUND_AF'] == 1:
                niveis.append('Fundamental')
            if row['IN_MED'] == 1:
                niveis.append('Médio')
            if row['IN_EJA'] == 1:
                niveis.append('EJA')
            for nivel in niveis:
                cursor.execute(
                    'INSERT INTO "Matricula" ("ID_ESCOLA", "NIVEL_ENSINO", "QT_MATRICULAS_TOTAL", "QT_MATRICULAS_INDIGENAS", "ANO_REFERENCIA") VALUES (%s, %s, %s, %s, %s)',
                    (id_escola, nivel, int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']), row['NU_ANO_CENSO'])
                )

        # 7. Territorio_Indigena (parcial)
        # Processa os dados de territórios indígenas e insere na tabela "Territorio_Indigena"
        territorios = df[df['TP_LOCALIZACAO_DIFERENCIADA'] == 1][['SG_UF', 'NO_MUNICIPIO']].drop_duplicates()
        for _, row in territorios.iterrows():
            id_uf = ufs_dict[row['SG_UF']]
            nome_territorio = f"Território Indígena {row['NO_MUNICIPIO']}"
            cursor.execute(
                'INSERT INTO "Territorio_Indigena" ("ID_UF", "NOME_TERRITORIO", "ETNIA_DOMINANTE", "AREA", "POP_TOTAL") VALUES (%s, %s, %s, %s, %s)',
                (id_uf, nome_territorio, None, None, None)
            )

        conn.commit()
        print("CSV do Censo Escolar carregado com sucesso.")
    except Exception as e:
        # Trata erros e desfaz alterações em caso de falha
        print(f"Erro ao carregar CSV: {e}")
        conn.rollback()

# Função para carregar e processar múltiplos arquivos XLSX
# Lê os dados de arquivos XLSX e insere nas tabelas do banco de dados
def carregar_xlsx(arquivos_xlsx):
    try:
        for arquivo in arquivos_xlsx:
            print(f"Processando arquivo: {arquivo}")
            # Lê o arquivo XLSX
            df = pd.read_excel(arquivo)

            # Tratar valores nulos e inválidos
            # Substitui valores ausentes ou inválidos por padrões
            df = df.fillna(0)  # Substitui NaN por 0 para valores numéricos
            df = df.replace('-', 0)  # Substitui '-' por 0 (valores ausentes no XLSX)

            if 'frequencia_escolar.xlsx' in arquivo:
                # Processar frequencia_escolar.xlsx
                # Renomeia colunas e filtra faixas etárias principais
                df = df.rename(columns={
                    'Brasil e Unidade da Federação': 'UF',
                    'Total': 'TAXA_FREQUENCIA'
                })

                # Filtrar apenas as faixas etárias principais
                faixas_etarias = [
                    '0 a 3 anos', '4 a 5 anos', '6 a 14 anos',
                    '15 a 17 anos', '18 a 24 anos', '25 anos ou mais'
                ]

                # Mapear nomes de UFs para siglas (para compatibilidade com ufs_dict)
                uf_to_sigla = {
                    'Rondônia': 'RO', 'Acre': 'AC', 'Amazonas': 'AM', 'Roraima': 'RR',
                    'Pará': 'PA', 'Amapá': 'AP', 'Tocantins': 'TO', 'Maranhão': 'MA',
                    'Piauí': 'PI', 'Ceará': 'CE', 'Rio Grande do Norte': 'RN',
                    'Paraíba': 'PB', 'Pernambuco': 'PE', 'Alagoas': 'AL', 'Sergipe': 'SE',
                    'Bahia': 'BA', 'Minas Gerais': 'MG', 'Espírito Santo': 'ES',
                    'Rio de Janeiro': 'RJ', 'São Paulo': 'SP', 'Paraná': 'PR',
                    'Santa Catarina': 'SC', 'Rio Grande do Sul': 'RS',
                    'Mato Grosso do Sul': 'MS', 'Mato Grosso': 'MT', 'Goiás': 'GO',
                    'Distrito Federal': 'DF'
                }

                # Processar apenas linhas com UFs válidas e faixas etárias principais
                for _, row in df.iterrows():
                    uf_nome = row['UF']
                    if uf_nome == 'Brasil':
                        continue  # Ignorar o total nacional
                    if uf_nome not in uf_to_sigla:
                        continue  # Ignorar UFs não mapeadas
                    sigla_uf = uf_to_sigla[uf_nome]
                    if sigla_uf not in ufs_dict:
                        continue  # Ignorar se a UF não está no banco

                    # Obter todos os municípios da UF
                    cursor.execute(
                        'SELECT "ID_MUNICIPIO" FROM "Municipio" WHERE "ID_UF" = %s',
                        (ufs_dict[sigla_uf],)
                    )
                    municipios_uf = [r[0] for r in cursor.fetchall()]

                    # Inserir a taxa de frequência para cada município da UF
                    for faixa in faixas_etarias:
                        if faixa in row and row[faixa] != 0:  # Verificar se a faixa existe e não é zero
                            taxa = float(row[faixa])
                            if 0 <= taxa <= 100:  # Validar a taxa
                                for id_municipio in municipios_uf:
                                    cursor.execute(
                                        'INSERT INTO "Frequencia_Escolar" ("ID_MUNICIPIO", "FAIXA_ETARIA", "TAXA_FREQUENCIA") VALUES (%s, %s, %s)',
                                        (id_municipio, faixa, taxa)
                                    )

            else:
                # Processar outros arquivos XLSX (lógica genérica)
                # Insere dados em tabelas específicas com base nas colunas disponíveis
                # Tratar valores nulos
                df = df.fillna({
                    'TAXA_FREQUENCIA': 0,
                    'QT_PESSOAS': 0,
                    'MEDIA_ANOS_ESTUDO': 0,
                    'AREA': None,
                    'POP_TOTAL': None,
                    'ETNIA_DOMINANTE': None,
                    'FAIXA_ETARIA': 'Desconhecida',
                    'NIVEL_INSTRUCAO': 'Desconhecido',
                    'NOME_TERRITORIO': None,
                    'CO_MUNICIPIO': None,
                    'CO_UF': None
                })

                # Carregar Frequencia_Escolar
                if all(col in df.columns for col in ['CO_MUNICIPIO', 'FAIXA_ETARIA', 'TAXA_FREQUENCIA']):
                    for _, row in df.iterrows():
                        if row['CO_MUNICIPIO'] and row['CO_MUNICIPIO'] in municipios_dict:
                            cursor.execute(
                                'INSERT INTO "Frequencia_Escolar" ("ID_MUNICIPIO", "FAIXA_ETARIA", "TAXA_FREQUENCIA") VALUES (%s, %s, %s)',
                                (municipios_dict[row['CO_MUNICIPIO']], row['FAIXA_ETARIA'], row['TAXA_FREQUENCIA'])
                            )

                # Carregar Nivel_Instrucao
                if all(col in df.columns for col in ['CO_MUNICIPIO', 'FAIXA_ETARIA', 'NIVEL_INSTRUCAO', 'QT_PESSOAS']):
                    for _, row in df.iterrows():
                        if row['CO_MUNICIPIO'] and row['CO_MUNICIPIO'] in municipios_dict:
                            cursor.execute(
                                'INSERT INTO "Nivel_Instrucao" ("ID_MUNICIPIO", "FAIXA_ETARIA", "NIVEL", "QT_PESSOAS") VALUES (%s, %s, %s, %s)',
                                (municipios_dict[row['CO_MUNICIPIO']], row['FAIXA_ETARIA'], row['NIVEL_INSTRUCAO'], row['QT_PESSOAS'])
                            )

                # Carregar Anos_Estudo
                if all(col in df.columns for col in ['CO_MUNICIPIO', 'FAIXA_ETARIA', 'MEDIA_ANOS_ESTUDO']):
                    for _, row in df.iterrows():
                        if row['CO_MUNICIPIO'] and row['CO_MUNICIPIO'] in municipios_dict:
                            cursor.execute(
                                'INSERT INTO "Anos_Estudo" ("ID_MUNICIPIO", "FAIXA_ETARIA", "MEDIA_ANOS_ESTUDO") VALUES (%s, %s, %s)',
                                (municipios_dict[row['CO_MUNICIPIO']], row['FAIXA_ETARIA'], row['MEDIA_ANOS_ESTUDO'])
                            )

                # Carregar Territorio_Indigena
                if all(col in df.columns for col in ['CO_UF', 'NOME_TERRITORIO']):
                    for _, row in df.iterrows():
                        if row['CO_UF'] and row['CO_UF'] in ufs_dict and row['NOME_TERRITORIO']:
                            cursor.execute(
                                'INSERT INTO "Territorio_Indigena" ("ID_UF", "NOME_TERRITORIO", "ETNIA_DOMINANTE", "AREA", "POP_TOTAL") VALUES (%s, %s, %s, %s, %s)',
                                (
                                    ufs_dict[row['CO_UF']],
                                    row['NOME_TERRITORIO'],
                                    row.get('ETNIA_DOMINANTE', None),
                                    row.get('AREA', None),
                                    row.get('POP_TOTAL', None)
                                )
                            )

        conn.commit()
        print("Todos os arquivos XLSX foram carregados com sucesso.")
    except Exception as e:
        # Trata erros e desfaz alterações em caso de falha
        print(f"Erro ao carregar XLSX: {e}")
        conn.rollback()

# Executar as funções
if __name__ == "__main__":
    # Lista de arquivos XLSX a serem processados
    arquivos_xlsx = [
        'frequencia_escolar.xlsx',
        # Adicione outros arquivos XLSX aqui
        # Exemplo: 'nivel_instrucao.xlsx', 'territorios_indigenas.xlsx'
    ]

    try:
        # Cria o esquema do banco de dados
        criar_esquema()
        # Carrega os dados do CSV do censo escolar
        carregar_csv_censo()
        # Carrega os dados dos arquivos XLSX
        carregar_xlsx(arquivos_xlsx)
    finally:
        # Fecha a conexão com o banco de dados
        cursor.close()
        conn.close()
        print("Conexão fechada.")