import psycopg2
from psycopg2 import extras
import pandas as pd
import uuid

# Conectar ao banco de dados PostgreSQL
# Configura a conexão com o banco de dados PostgreSQL usando as credenciais fornecidas
conn = psycopg2.connect(
    dbname="db_250502",
    user="ra250502",
    password="oSaiph1pie",
    host="bidu.lab.ic.unicamp.br",
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
    CREATE TABLE IF NOT EXISTS "Regiao" (
        "ID_REGIAO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_REGIAO" VARCHAR(50) NOT NULL,
        "POPULACAO_TOTAL" INT,
        "POPULACAO_INDIGENA" INT
    );

    -- 2. Tabela Unidade_Federativa
    CREATE TABLE IF NOT EXISTS "Unidade_Federativa" (
        "ID_UF" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_UF" VARCHAR(50) NOT NULL,
        "SIGLA_UF" CHAR(2) NOT NULL,
        "ID_REGIAO" INT NOT NULL,
        "POPULACAO_TOTAL" INT,
        "POPULACAO_INDIGENA" INT,
        FOREIGN KEY ("ID_REGIAO") REFERENCES "Regiao"("ID_REGIAO")
    );

    -- 3. Tabela Municipio
    CREATE TABLE IF NOT EXISTS "Municipio" (
        "ID_MUNICIPIO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "NOME_MUNICIPIO" VARCHAR(100) NOT NULL,
        "ID_UF" INT NOT NULL,
        "POPULACAO_TOTAL" INT,
        "POPULACAO_INDIGENA" INT,
        FOREIGN KEY ("ID_UF") REFERENCES "Unidade_Federativa"("ID_UF")
    );

    -- 4. Tabela Escola
    CREATE TABLE IF NOT EXISTS "Escola" (
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
    CREATE TABLE IF NOT EXISTS "Turma" (
        "ID_TURMA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_ESCOLA" INT NOT NULL,
        "NIVEL_ENSINO" VARCHAR(20) NOT NULL,
        "QT_TURMAS" INT NOT NULL DEFAULT 0,
        "QT_TURMAS_INDIGENAS" INT DEFAULT 0,
        FOREIGN KEY ("ID_ESCOLA") REFERENCES "Escola"("ID_ESCOLA")
    );

    -- 6. Tabela Matricula
    CREATE TABLE IF NOT EXISTS "Matricula" (
        "ID_MATRICULA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_ESCOLA" INT NOT NULL,
        "NIVEL_ENSINO" VARCHAR(20) NOT NULL,
        "QT_MATRICULAS_TOTAL" INT NOT NULL DEFAULT 0,
        "QT_MATRICULAS_INDIGENAS" INT DEFAULT 0,
        "ANO_REFERENCIA" INT NOT NULL,
        FOREIGN KEY ("ID_ESCOLA") REFERENCES "Escola"("ID_ESCOLA")
    );

    -- 7. Tabela Frequencia_Escolar
    CREATE TABLE IF NOT EXISTS "Frequencia_Escolar" (
        "ID_FREQUENCIA" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_MUNICIPIO" INT NOT NULL,
        "FAIXA_ETARIA" VARCHAR(20) NOT NULL,
        "TAXA_FREQUENCIA" DECIMAL(5,2) NOT NULL,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO"),
        CONSTRAINT "check_taxa_frequencia" CHECK ("TAXA_FREQUENCIA" BETWEEN 0 AND 100)
    );

    -- 8. Tabela Nivel_Instrucao
    CREATE TABLE IF NOT EXISTS "Nivel_Instrucao" (
        "ID_NIVEL_INSTRUCAO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_MUNICIPIO" INT NOT NULL,
        "FAIXA_ETARIA" VARCHAR(20) NOT NULL,
        "NIVEL" VARCHAR(30) NOT NULL,
        "QT_PESSOAS" INT NOT NULL DEFAULT 0,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO")
    );

    -- 9. Tabela Anos_Estudo
    CREATE TABLE IF NOT EXISTS "Anos_Estudo" (
        "ID_ANOS_ESTUDO" INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        "ID_MUNICIPIO" INT NOT NULL,
        "FAIXA_ETARIA" VARCHAR(20) NOT NULL,
        "MEDIA_ANOS_ESTUDO" DECIMAL(3,1) NOT NULL,
        FOREIGN KEY ("ID_MUNICIPIO") REFERENCES "Municipio"("ID_MUNICIPIO"),
        CONSTRAINT "check_media_anos" CHECK ("MEDIA_ANOS_ESTUDO" BETWEEN 0 AND 20)
    );

    -- 10. Tabela Territorio_Indigena
    CREATE TABLE IF NOT EXISTS "Territorio_Indigena" (
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
        cursor.execute(schema_sql)
        conn.commit()
        print("Esquema criado com sucesso.")
    except Exception as inner_exception:
        # Trata erros e desfaz alterações em caso de falha
        print(f"Erro ao criar esquema: {e}")
        conn.rollback()

# Função para carregar e processar o CSV do Censo Escolar
# Lê os dados do arquivo CSV e insere nas tabelas do banco de dados
def carregar_csv_censo():
    regioes_dict = {}
    ufs_dict = {}
    municipios_dict = {}
    escolas_dict = {}

    print("Carregando CSV do Censo Escolar...")
    try:
        # Lê o arquivo CSV com os dados do censo escolar
        df = pd.read_csv('microdados_ed_basica_2023.csv', sep=';', low_memory=False, encoding='latin1')
        if df.empty:
            raise ValueError("CSV file is empty or invalid.")

        # Tratar valores nulos e ajustar tipos de dados
        df.fillna({
            'QT_MAT_BAS': 0, 'QT_MAT_BAS_INDIGENA': 0, 'IN_EDUCACAO_INDIGENA': 0,
            'TP_DEPENDENCIA': '4', 'TP_LOCALIZACAO': '1', 'TP_SITUACAO_FUNCIONAMENTO': '1',
            'QT_TUR_INF': 0, 'QT_TUR_FUND': 0, 'QT_TUR_MED': 0, 'QT_TUR_EJA': 0,
            'IN_INF': 0, 'IN_FUND_AI': 0, 'IN_FUND_AF': 0, 'IN_MED': 0, 'IN_EJA': 0
        }, inplace=True)
        df['IN_EDUCACAO_INDIGENA'] = df['IN_EDUCACAO_INDIGENA'].astype(bool)

        # Mapear valores
        df['TP_DEPENDENCIA'] = df['TP_DEPENDENCIA'].map({1: 'Federal', 2: 'Estadual', 3: 'Municipal', 4: 'Privada'})
        df['TP_LOCALIZACAO'] = df['TP_LOCALIZACAO'].map({1: 'Urbana', 2: 'Rural'})
        df['TP_SITUACAO_FUNCIONAMENTO'] = df['TP_SITUACAO_FUNCIONAMENTO'].map({1: 'Ativa', 2: 'Inativa'})

        # Inserir dados em lote
        def insert_batch(query, data):
            psycopg2.extras.execute_batch(cursor, query, data)

        # 1. Regiao
        regioes = df.groupby('NO_REGIAO').agg({'QT_MAT_BAS': 'sum', 'QT_MAT_BAS_INDIGENA': 'sum'}).reset_index()
        regioes_data = [
            (row['NO_REGIAO'], int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']))
            for _, row in regioes.iterrows()
        ]
        insert_batch(
            'INSERT INTO "Regiao" ("NOME_REGIAO", "POPULACAO_TOTAL", "POPULACAO_INDIGENA") VALUES (%s, %s, %s) RETURNING "ID_REGIAO"',
            regioes_data
        )

        cursor.execute('SELECT "NOME_REGIAO", "ID_REGIAO" FROM "Regiao"')
        regioes_dict = {nome: id_regiao for nome, id_regiao in cursor.fetchall()}

        # 2. Unidade_Federativa
        ufs = df.groupby(['SG_UF', 'NO_UF', 'NO_REGIAO']).agg({'QT_MAT_BAS': 'sum', 'QT_MAT_BAS_INDIGENA': 'sum'}).reset_index()
        ufs_data = [
            (row['NO_UF'], row['SG_UF'], regioes_dict.get(row['NO_REGIAO'], None), int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']))
            for _, row in ufs.iterrows() if row['NO_REGIAO'] in regioes_dict
        ]
        insert_batch(
            'INSERT INTO "Unidade_Federativa" ("NOME_UF", "SIGLA_UF", "ID_REGIAO", "POPULACAO_TOTAL", "POPULACAO_INDIGENA") VALUES (%s, %s, %s, %s, %s) RETURNING "ID_UF"',
            ufs_data
        )
        cursor.execute('SELECT "SIGLA_UF", "ID_UF" FROM "Unidade_Federativa"')
        ufs_dict = {sigla: id_uf for sigla, id_uf in cursor.fetchall()}

        # 3. Municipio
        municipios = df.groupby(['CO_MUNICIPIO', 'NO_MUNICIPIO', 'SG_UF']).agg({'QT_MAT_BAS': 'sum', 'QT_MAT_BAS_INDIGENA': 'sum'}).reset_index()
        municipios_data = [
            (row['NO_MUNICIPIO'], ufs_dict.get(row['SG_UF'], None), int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']))
            for _, row in municipios.iterrows() if row['SG_UF'] in ufs_dict
        ]
        insert_batch(
            'INSERT INTO "Municipio" ("NOME_MUNICIPIO", "ID_UF", "POPULACAO_TOTAL", "POPULACAO_INDIGENA") VALUES (%s, %s, %s, %s) RETURNING "ID_MUNICIPIO"',
            municipios_data
        )

        cursor.execute('SELECT "NOME_MUNICIPIO", "ID_MUNICIPIO" FROM "Municipio"')
        municipios_dict = {nome: id_municipio for nome, id_municipio in cursor.fetchall()}

        # 4. Escola
        escolas = df[['CO_ENTIDADE', 'NO_ENTIDADE', 'CO_MUNICIPIO', 'TP_DEPENDENCIA', 
                    'TP_LOCALIZACAO', 'TP_SITUACAO_FUNCIONAMENTO', 'IN_EDUCACAO_INDIGENA']].drop_duplicates()

        # Verificar se há dados para inserir
        print(f"Total de escolas encontradas no CSV: {len(escolas)}")
        print(f"Exemplo de CO_MUNICIPIO: {escolas['CO_MUNICIPIO'].iloc[0]}")

        # Verificar mapeamento de municípios
        cursor.execute('SELECT COUNT(*) FROM "Municipio"')
        print(f"Total de municípios no banco: {cursor.fetchone()[0]}")

        escolas_data = []
        for _, row in escolas.iterrows():
            id_municipio = municipios_dict.get(str(row['CO_MUNICIPIO']))
            if id_municipio is None:
                print(f"Município não encontrado para CO_MUNICIPIO: {row['CO_MUNICIPIO']}")
                continue
                
            escolas_data.append((
                row['NO_ENTIDADE'], 
                id_municipio,
                row['TP_DEPENDENCIA'],
                row['TP_LOCALIZACAO'],
                row['TP_SITUACAO_FUNCIONAMENTO'],
                row['IN_EDUCACAO_INDIGENA']
            ))

        print(f"Total de escolas a inserir: {len(escolas_data)}")

        if escolas_data:
            psycopg2.extras.execute_batch(
                cursor,
                'INSERT INTO "Escola" ("NOME_ESCOLA", "ID_MUNICIPIO", "TIPO_DEPENDENCIA", "TIPO_LOCALIZACAO", "SITUACAO_FUNCIONAMENTO", "INDIGENA") VALUES (%s, %s, %s, %s, %s, %s) RETURNING "ID_ESCOLA"',
                escolas_data
            )
            returned_ids = [row[0] for row in cursor.fetchall()]
            escolas_dict.update({row['CO_ENTIDADE']: id_escola for row, id_escola in zip(escolas.itertuples(), returned_ids)})
            print(f"Total de escolas inseridas: {len(returned_ids)}")
        else:
            print("Nenhuma escola para inserir - verifique os logs acima")    

        # 5. Turma
        niveis_ensino = ['Infantil', 'Fundamental', 'Médio', 'EJA']
        turmas_columns = {
            'Infantil': 'QT_TUR_INF',
            'Fundamental': 'QT_TUR_FUND',
            'Médio': 'QT_TUR_MED',
            'EJA': 'QT_TUR_EJA'
        }
        turmas_data = []
        # Adicione verificação antes de inserir turmas:
        print(f"Total de escolas no dicionário: {len(escolas_dict)}")
        if not escolas_dict:
            print("AVISO: Dicionário de escolas vazio - não é possível inserir turmas")
        else:
            for co_entidade, id_escola in escolas_dict.items():
                escola_data = df[df['CO_ENTIDADE'] == co_entidade]
                if escola_data.empty:
                    continue
                for nivel in niveis_ensino:
                    col_name = turmas_columns.get(nivel)
                    if col_name not in df.columns:
                        print(f"Column {col_name} not found in CSV. Skipping {nivel} for escola {co_entidade}")
                        continue
                    qt_turmas = escola_data[col_name].iloc[0]
                    qt_turmas_indigenas = qt_turmas if escola_data['IN_EDUCACAO_INDIGENA'].iloc[0] else 0
                    if qt_turmas > 0:
                        turmas_data.append((id_escola, nivel, int(qt_turmas), int(qt_turmas_indigenas)))
            insert_batch(
                'INSERT INTO "Turma" ("ID_ESCOLA", "NIVEL_ENSINO", "QT_TURMAS", "QT_TURMAS_INDIGENAS") VALUES (%s, %s, %s, %s)',
                turmas_data
            )

        # 6. Matricula
        matriculas = df[['CO_ENTIDADE', 'QT_MAT_BAS', 'QT_MAT_BAS_INDIGENA', 'NU_ANO_CENSO', 'IN_INF', 'IN_FUND_AI', 'IN_FUND_AF', 'IN_MED', 'IN_EJA']].drop_duplicates()
        matriculas_data = []
        print(f"Total de escolas no dicionário: {len(escolas_dict)}")
        if not escolas_dict:
            print("AVISO: Dicionário de escolas vazio - não é possível inserir matrículas")
        else:
            for _, row in matriculas.iterrows():
                id_escola = escolas_dict.get(row['CO_ENTIDADE'], None)
                if id_escola is None:
                    continue
                niveis = [
                    nivel for nivel, flag in zip(
                        ['Infantil', 'Fundamental', 'Médio', 'EJA'],
                        [row['IN_INF'], row['IN_FUND_AI'] or row['IN_FUND_AF'], row['IN_MED'], row['IN_EJA']]
                    ) if flag == 1
                ]
                matriculas_data.extend(
                    (id_escola, nivel, int(row['QT_MAT_BAS']), int(row['QT_MAT_BAS_INDIGENA']), row['NU_ANO_CENSO'])
                    for nivel in niveis
                )
            insert_batch(
                'INSERT INTO "Matricula" ("ID_ESCOLA", "NIVEL_ENSINO", "QT_MATRICULAS_TOTAL", "QT_MATRICULAS_INDIGENAS", "ANO_REFERENCIA") VALUES (%s, %s, %s, %s, %s)',
                matriculas_data
            )

        # 7. Territorio Indígena
        territorios = df[df['TP_LOCALIZACAO_DIFERENCIADA'] == 1][['SG_UF', 'NO_MUNICIPIO']].drop_duplicates()
        for _, row in territorios.iterrows():
            if row['SG_UF'] in ufs_dict:
                id_uf = ufs_dict[row['SG_UF']]
                nome_territorio = f"Território Indígena {row['NO_MUNICIPIO']}"
                cursor.execute(
                    'INSERT INTO "Territorio_Indigena" ("ID_UF", "NOME_TERRITORIO", "ETNIA_DOMINANTE", "AREA", "POP_TOTAL") VALUES (%s, %s, %s, %s, %s)',
                    (id_uf, nome_territorio, None, None, None)
                )

        conn.commit()
        print("CSV do Censo Escolar carregado com sucesso.")
    except Exception as e:
        print(f"Erro ao carregar CSV: {e}")
        conn.rollback()   

# Função para carregar e processar múltiplos arquivos XLSX
# Lê os dados de arquivos XLSX e insere nas tabelas do banco de dados
def carregar_xlsx(arquivos_xlsx):
    try:
        # Recarregar os dicionários com os dados atualizados do banco
        cursor.execute('SELECT "NOME_REGIAO", "ID_REGIAO" FROM "Regiao"')
        regioes_dict = {nome: id_regiao for nome, id_regiao in cursor.fetchall()}

        cursor.execute('SELECT "SIGLA_UF", "ID_UF" FROM "Unidade_Federativa"')
        ufs_dict = {sigla: id_uf for sigla, id_uf in cursor.fetchall()}

        cursor.execute('SELECT "NOME_MUNICIPIO", "ID_MUNICIPIO" FROM "Municipio"')
        municipios_dict = {nome: id_municipio for nome, id_municipio in cursor.fetchall()}

        cursor.execute('SELECT "NOME_ESCOLA", "ID_ESCOLA" FROM "Escola"')
        escolas_dict = {nome: id_escola for nome, id_escola in cursor.fetchall()}


        for arquivo in arquivos_xlsx:
            print(f"Processando arquivo: {arquivo}")
            
            # Lista de UFs válidas
            ufs_brasil = [
                'Brasil', 'Rondônia', 'Acre', 'Amazonas', 'Roraima', 'Pará', 'Amapá', 'Tocantins',
                'Maranhão', 'Piauí', 'Ceará', 'Rio Grande do Norte', 'Paraíba', 'Pernambuco',
                'Alagoas', 'Sergipe', 'Bahia', 'Minas Gerais', 'Espírito Santo', 'Rio de Janeiro',
                'São Paulo', 'Paraná', 'Santa Catarina', 'Rio Grande do Sul', 'Mato Grosso do Sul',
                'Mato Grosso', 'Goiás', 'Distrito Federal'
            ]
            
            # Mapeamento de nomes de UFs para siglas
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
            
            # Faixas etárias comuns
            faixas_etarias = {
                '0 a 3 anos': 1,    # col_1
                '4 a 5 anos': 2,    # col_2
                '6 a 14 anos': 3,   # col_3
                '15 a 17 anos': 4,  # col_4
                '18 a 24 anos': 5,  # col_5
                '25 anos ou mais': 6 # col_6
            }
            
            if 'frequencia_escolar.xlsx' in arquivo:
                # Processar frequencia_escolar.xlsx
                df = pd.read_excel(arquivo, header=None, skiprows=5)
                
                if df.empty:
                    print("Arquivo está vazio após pular linhas iniciais.")
                    continue
                
                colunas = ['UF'] + [f'col_{i}' for i in range(1, len(df.columns))]
                df.columns = colunas
                df = df[df['UF'].isin(ufs_brasil)].copy()
                
                total_insercoes = 0
                
                for _, row in df.iterrows():
                    uf_nome = row['UF']
                    if uf_nome == 'Brasil':
                        continue
                        
                    if uf_nome not in uf_to_sigla:
                        print(f"UF não mapeada: {uf_nome}")
                        continue
                        
                    sigla_uf = uf_to_sigla[uf_nome]
                    if sigla_uf not in ufs_dict:
                        print(f"UF não encontrada no banco: {sigla_uf}")
                        continue
                        
                    cursor.execute(
                        'SELECT "ID_MUNICIPIO" FROM "Municipio" WHERE "ID_UF" = %s',
                        (ufs_dict[sigla_uf],)
                    )
                    municipios_uf = [r[0] for r in cursor.fetchall()]
                    
                    if not municipios_uf:
                        print(f"Nenhum município encontrado para UF: {sigla_uf}")
                        continue
                    
                    for faixa, col_idx in faixas_etarias.items():
                        col_name = f'col_{col_idx}'
                        if col_name not in row:
                            print(f"Coluna {col_name} não encontrada para UF {uf_nome}")
                            continue
                            
                        try:
                            taxa_str = str(row[col_name]).replace(',', '.').strip()
                            if taxa_str in ['-', '', 'X', '..', '...']:
                                continue
                                
                            taxa = float(taxa_str)
                            if not (0 <= taxa <= 100):
                                print(f"Taxa inválida para {uf_nome}, faixa {faixa}: {taxa}")
                                continue
                                
                            for id_municipio in municipios_uf:
                                cursor.execute(
                                    'INSERT INTO "Frequencia_Escolar" ("ID_MUNICIPIO", "FAIXA_ETARIA", "TAXA_FREQUENCIA") VALUES (%s, %s, %s)',
                                    (id_municipio, faixa, taxa)
                                )
                                total_insercoes += 1
                                
                        except (ValueError, TypeError) as e:
                            print(f"Erro ao processar taxa para {uf_nome}, faixa {faixa}: {e}")
                            continue
                
                print(f"Total de inserções realizadas em Frequencia_Escolar: {total_insercoes}")

            elif 'media_anos.xlsx' in arquivo:
                # Processar media_anos.xlsx
                df = pd.read_excel(arquivo, header=None, skiprows=5)
                
                if df.empty:
                    print("Arquivo está vazio após pular linhas iniciais.")
                    continue
                
                colunas = ['UF'] + [f'col_{i}' for i in range(1, len(df.columns))]
                df.columns = colunas
                df = df[df['UF'].isin(ufs_brasil)].copy()
                
                total_insercoes = 0
                
                for _, row in df.iterrows():
                    uf_nome = row['UF']
                    if uf_nome == 'Brasil':
                        continue
                        
                    if uf_nome not in uf_to_sigla:
                        print(f"UF não mapeada: {uf_nome}")
                        continue
                        
                    sigla_uf = uf_to_sigla[uf_nome]
                    if sigla_uf not in ufs_dict:
                        print(f"UF não encontrada no banco: {sigla_uf}")
                        continue
                        
                    cursor.execute(
                        'SELECT "ID_MUNICIPIO" FROM "Municipio" WHERE "ID_UF" = %s',
                        (ufs_dict[sigla_uf],)
                    )
                    municipios_uf = [r[0] for r in cursor.fetchall()]
                    
                    if not municipios_uf:
                        print(f"Nenhum município encontrado para UF: {sigla_uf}")
                        continue
                    
                    for faixa, col_idx in faixas_etarias.items():
                        col_name = f'col_{col_idx}'
                        if col_name not in row:
                            print(f"Coluna {col_name} não encontrada para UF {uf_nome}")
                            continue
                            
                        try:
                            media_str = str(row[col_name]).replace(',', '.').strip()
                            if media_str in ['-', '', 'X', '..', '...']:
                                continue
                                
                            media = float(media_str)
                            if not (0 <= media <= 20):  # Suposição: média de anos de estudo entre 0 e 20
                                print(f"Média inválida para {uf_nome}, faixa {faixa}: {media}")
                                continue
                                
                            for id_municipio in municipios_uf:
                                cursor.execute(
                                    'INSERT INTO "Anos_Estudo" ("ID_MUNICIPIO", "FAIXA_ETARIA", "MEDIA_ANOS_ESTUDO") VALUES (%s, %s, %s)',
                                    (id_municipio, faixa, media)
                                )
                                total_insercoes += 1
                                
                        except (ValueError, TypeError) as e:
                            print(f"Erro ao processar média para {uf_nome}, faixa {faixa}: {e}")
                            continue
                
                print(f"Total de inserções realizadas em Anos_Estudo: {total_insercoes}")

            elif 'nivel_instrucao.xlsx' in arquivo:
                try:
                    # Ler o arquivo pulando metadados (5 primeiras linhas são cabeçalhos)
                    df = pd.read_excel(arquivo, header=None, skiprows=5)
                    
                    if df.empty:
                        print("Arquivo está vazio após pular linhas iniciais.")
                        continue
                    
                    # Níveis de instrução (baseado nos cabeçalhos)
                    niveis_instrucao = [
                        'Sem instrução e fundamental incompleto',
                        'Fundamental completo e médio incompleto',
                        'Médio completo e superior incompleto',
                        'Superior completo'
                    ]
                    
                    # Faixas etárias (baseado nos cabeçalhos)
                    faixas_etarias = [
                        'Total',
                        '18 a 24 anos',
                        '18 a 19 anos',
                        '20 a 24 anos',
                        '25 anos ou mais',
                        '25 a 64 anos',
                        '25 a 29 anos',
                        '30 a 34 anos',
                        '35 a 39 anos',
                        '40 a 44 anos',
                        '45 a 49 anos',
                        '50 a 54 anos',
                        '55 a 59 anos',
                        '60 a 64 anos',
                        '65 anos ou mais',
                        '65 a 69 anos',
                        '70 a 74 anos',
                        '75 a 79 anos',
                        '80 anos ou mais'
                    ]
                    
                    # Primeiro, vamos criar um mapeamento de município para ID_MUNICIPIO
                    cursor.execute('SELECT "NOME_MUNICIPIO", "ID_MUNICIPIO" FROM "Municipio"')
                    municipios_dict = {row[0].upper(): row[1] for row in cursor.fetchall()}
                    
                    total_insercoes = 0
                    
                    # Processar cada linha (cada município/UF)
                    for _, row in df.iterrows():
                        # Verificar se a primeira célula contém texto (nome do município/UF)
                        if pd.isna(row[0]) or not isinstance(row[0], str):
                            continue
                            
                        nome_local = row[0].strip()
                        
                        if nome_local == 'Brasil':
                            continue  # Ignorar o total nacional
                            
                        # Verificar se é um município (está no nosso dicionário)
                        nome_municipio = nome_local.upper()
                        if nome_municipio not in municipios_dict:
                            continue  # Pular UFs e outros que não são municípios específicos
                            
                        id_municipio = municipios_dict[nome_municipio]
                        
                        # Para cada nível de instrução (as colunas estão agrupadas por nível)
                        for nivel_idx, nivel in enumerate(niveis_instrucao):
                            # Calcular o deslocamento das colunas para este nível
                            # Assumindo que cada nível tem um bloco de colunas para cada faixa etária
                            col_offset = 1 + (len(faixas_etarias) * nivel_idx)
                            
                            # Para cada faixa etária
                            for faixa_idx, faixa in enumerate(faixas_etarias):
                                col_num = col_offset + faixa_idx
                                if col_num >= len(row):
                                    continue  # Evitar índices fora do range
                                    
                                # Obter valor da célula
                                valor = row[col_num]
                                
                                # Verificar se o valor é numérico
                                if pd.isna(valor):
                                    continue
                                    
                                try:
                                    qt_pessoas = int(float(valor))

                                    nivel_abreviado = {
                                    'Sem instrução e fundamental incompleto': 'Sem instrução',
                                    'Fundamental completo e médio incompleto': 'Fundamental completo',
                                    'Médio completo e superior incompleto': 'Médio completo',
                                    'Superior completo': 'Superior completo'  # Já cabe em 30 chars
                                    }
                                        
                                    nivel_para_inserir = nivel_abreviado[nivel]
                                    
                                    # Inserir no banco
                                    cursor.execute(
                                        'INSERT INTO "Nivel_Instrucao" ("ID_MUNICIPIO", "FAIXA_ETARIA", "NIVEL", "QT_PESSOAS") VALUES (%s, %s, %s, %s)',
                                        (id_municipio, faixa, nivel_para_inserir, qt_pessoas)
                                    )
                                    total_insercoes += 1
                                    
                                except (ValueError, TypeError) as e:
                                    print(f"Erro ao processar valor para {nome_local}, nível {nivel}, faixa {faixa}: {e}")
                                    continue
                    
                    conn.commit()
                    print(f"Total de inserções em Nivel_Instrucao: {total_insercoes}")
                    
                except Exception as e:
                    conn.rollback()
                    print(f"Erro ao processar nível de instrução: {e}")
                    
            else:
                # Processar outros arquivos XLSX (lógica genérica)
                df = pd.read_excel(arquivo)
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

                if all(col in df.columns for col in ['CO_MUNICIPIO', 'FAIXA_ETARIA', 'TAXA_FREQUENCIA']):
                    for _, row in df.iterrows():
                        if row['CO_MUNICIPIO'] and row['CO_MUNICIPIO'] in municipios_dict:
                            cursor.execute(
                                'INSERT INTO "Frequencia_Escolar" ("ID_MUNICIPIO", "FAIXA_ETARIA", "TAXA_FREQUENCIA") VALUES (%s, %s, %s)',
                                (municipios_dict[row['CO_MUNICIPIO']], row['FAIXA_ETARIA'], row['TAXA_FREQUENCIA'])
                            )

                if all(col in df.columns for col in ['CO_MUNICIPIO', 'FAIXA_ETARIA', 'NIVEL_INSTRUCAO', 'QT_PESSOAS']):
                    for _, row in df.iterrows():
                        if row['CO_MUNICIPIO'] and row['CO_MUNICIPIO'] in municipios_dict:
                            cursor.execute(
                                'INSERT INTO "Nivel_Instrucao" ("ID_MUNICIPIO", "FAIXA_ETARIA", "NIVEL", "QT_PESSOAS") VALUES (%s, %s, %s, %s)',
                                (municipios_dict[row['CO_MUNICIPIO']], row['FAIXA_ETARIA'], row['NIVEL_INSTRUCAO'], row['QT_PESSOAS'])
                            )

                if all(col in df.columns for col in ['CO_MUNICIPIO', 'FAIXA_ETARIA', 'MEDIA_ANOS_ESTUDO']):
                    for _, row in df.iterrows():
                        if row['CO_MUNICIPIO'] and row['CO_MUNICIPIO'] in municipios_dict:
                            cursor.execute(
                                'INSERT INTO "Anos_Estudo" ("ID_MUNICIPIO", "FAIXA_ETARIA", "MEDIA_ANOS_ESTUDO") VALUES (%s, %s, %s)',
                                (municipios_dict[row['CO_MUNICIPIO']], row['FAIXA_ETARIA'], row['MEDIA_ANOS_ESTUDO'])
                            )

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
        print(f"Erro ao carregar XLSX: {e}")
        conn.rollback()
        
        # Populate ufs_dict with data from the database
        cursor.execute('SELECT "SIGLA_UF", "ID_UF" FROM "Unidade_Federativa"')
        for sigla_uf, id_uf in cursor.fetchall():
            ufs_dict[sigla_uf] = id_uf

        for arquivo in arquivos_xlsx:
            print(f"Processando arquivo: {arquivo}")
            
            if 'frequencia_escolar.xlsx' in arquivo:
                # Processar frequencia_escolar.xlsx com estrutura específica
                # Ler o arquivo pulando as primeiras linhas que não contêm dados
                df = pd.read_excel(arquivo, header=None, skiprows=5)
                
                # Verificar se o DataFrame não está vazio
                if df.empty:
                    print("Arquivo está vazio após pular linhas iniciais.")
                    continue
                
                # Definir manualmente os nomes das colunas baseado na estrutura
                colunas = ['UF'] + [f'col_{i}' for i in range(1, len(df.columns))]
                df.columns = colunas
                
                # Filtrar apenas linhas com UFs válidas
                ufs_brasil = [
                    'Brasil', 'Rondônia', 'Acre', 'Amazonas', 'Roraima', 'Pará', 'Amapá', 'Tocantins',
                    'Maranhão', 'Piauí', 'Ceará', 'Rio Grande do Norte', 'Paraíba', 'Pernambuco',
                    'Alagoas', 'Sergipe', 'Bahia', 'Minas Gerais', 'Espírito Santo', 'Rio de Janeiro',
                    'São Paulo', 'Paraná', 'Santa Catarina', 'Rio Grande do Sul', 'Mato Grosso do Sul',
                    'Mato Grosso', 'Goiás', 'Distrito Federal'
                ]
                
                df = df[df['UF'].isin(ufs_brasil)].copy()
                
                # Mapear nomes de UFs para siglas
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
                
                # Definir manualmente as faixas etárias e suas colunas correspondentes
                faixas_etarias = {
                    '0 a 3 anos': 1,    # col_1
                    '4 a 5 anos': 2,    # col_2
                    '6 a 14 anos': 3,  # col_3
                    '15 a 17 anos': 4,  # col_4
                    '18 a 24 anos': 5,  # col_5
                    '25 anos ou mais': 6 # col_6
                }
                
                # Contador para acompanhar o progresso
                total_insercoes = 0
                
                for _, row in df.iterrows():
                    uf_nome = row['UF']
                    if uf_nome == 'Brasil':
                        continue  # Ignorar o total nacional
                        
                    if uf_nome not in uf_to_sigla:
                        print(f"UF não mapeada: {uf_nome}")
                        continue
                        
                    sigla_uf = uf_to_sigla[uf_nome]
                    if sigla_uf not in ufs_dict:
                        print(f"UF não encontrada no banco: {sigla_uf}")
                        continue
                        
                    # Obter todos os municípios da UF
                    cursor.execute(
                        'SELECT "ID_MUNICIPIO" FROM "Municipio" WHERE "ID_UF" = %s',
                        (ufs_dict[sigla_uf],)
                    )
                    municipios_uf = [r[0] for r in cursor.fetchall()]
                    
                    if not municipios_uf:
                        print(f"Nenhum município encontrado para UF: {sigla_uf}")
                        continue
                    
                    # Inserir dados para cada faixa etária e município
                    for faixa, col_idx in faixas_etarias.items():
                        col_name = f'col_{col_idx}'
                        if col_name not in row:
                            print(f"Coluna {col_name} não encontrada para UF {uf_nome}")
                            continue
                            
                        try:
                            # Converter para float, tratando possíveis strings como 'X' ou '-'
                            taxa_str = str(row[col_name]).replace(',', '.').strip()
                            if taxa_str in ['-', '', 'X', '..', '...']:
                                continue
                                
                            taxa = float(taxa_str)
                            if not (0 <= taxa <= 100):
                                print(f"Taxa inválida para {uf_nome}, faixa {faixa}: {taxa}")
                                continue
                                
                            for id_municipio in municipios_uf:
                                cursor.execute(
                                    'INSERT INTO "Frequencia_Escolar" ("ID_MUNICIPIO", "FAIXA_ETARIA", "TAXA_FREQUENCIA") VALUES (%s, %s, %s)',
                                    (id_municipio, faixa, taxa)
                                )
                                total_insercoes += 1
                                
                        except (ValueError, TypeError) as e:
                            print(f"Erro ao processar taxa para {uf_nome}, faixa {faixa}: {e}")
                            continue
                
                print(f"Total de inserções realizadas: {total_insercoes}")

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
    except Exception as inner_exception:
        # Trata erros e desfaz alterações em caso de falha
        print(f"Erro ao carregar XLSX: {inner_exception}")
        conn.rollback()

# Função para executar consultas analíticas
def executar_consultas_analiticas():
    try:
        print("\nExecutando consultas analíticas...")

        # Consulta 1: Taxa de Frequência Escolar por Faixa Etária e Região
        query1 = '''
        SELECT r."NOME_REGIAO", f."FAIXA_ETARIA", AVG(f."TAXA_FREQUENCIA") as media_taxa_frequencia
        FROM "Frequencia_Escolar" f
        JOIN "Municipio" m ON f."ID_MUNICIPIO" = m."ID_MUNICIPIO"
        JOIN "Unidade_Federativa" uf ON m."ID_UF" = uf."ID_UF"
        JOIN "Regiao" r ON uf."ID_REGIAO" = r."ID_REGIAO"
        GROUP BY r."NOME_REGIAO", f."FAIXA_ETARIA"
        ORDER BY r."NOME_REGIAO", f."FAIXA_ETARIA";
        '''
        cursor.execute(query1)
        print("\nConsulta 1: Taxa de Frequência Escolar por Faixa Etária e Região")
        for row in cursor.fetchall():
            print(f"Região: {row[0]}, Faixa Etária: {row[1]}, Média Taxa Frequência: {row[2]:.2f}%")

        # Consulta 2: Proporção de Matrículas Indígenas por UF
        query2 = '''
        SELECT uf."NOME_UF", uf."SIGLA_UF",
               SUM(m."QT_MATRICULAS_INDIGENAS") * 100.0 / NULLIF(SUM(m."QT_MATRICULAS_TOTAL"), 0) as proporcao_indigena
        FROM "Matricula" m
        JOIN "Escola" e ON m."ID_ESCOLA" = e."ID_ESCOLA"
        JOIN "Municipio" mun ON e."ID_MUNICIPIO" = mun."ID_MUNICIPIO"
        JOIN "Unidade_Federativa" uf ON mun."ID_UF" = uf."ID_UF"
        WHERE m."ANO_REFERENCIA" = 2023
        GROUP BY uf."NOME_UF", uf."SIGLA_UF"
        HAVING SUM(m."QT_MATRICULAS_TOTAL") > 0
        ORDER BY proporcao_indigena DESC;
        '''
        cursor.execute(query2)
        print("\nConsulta 2: Proporção de Matrículas Indígenas por UF (2023)")
        for row in cursor.fetchall():
            print(f"UF: {row[0]} ({row[1]}), Proporção Indígena: {row[2]:.2f}%")

        # Consulta 3: Municípios com Alta Média de Anos de Estudo em Faixa Etária Específica
        query3 = '''
        SELECT m."NOME_MUNICIPIO", uf."SIGLA_UF", a."MEDIA_ANOS_ESTUDO"
        FROM "Anos_Estudo" a
        JOIN "Municipio" m ON a."ID_MUNICIPIO" = m."ID_MUNICIPIO"
        JOIN "Unidade_Federativa" uf ON m."ID_UF" = uf."ID_UF"
        WHERE a."FAIXA_ETARIA" = '25 anos ou mais' AND a."MEDIA_ANOS_ESTUDO" > 10
        ORDER BY a."MEDIA_ANOS_ESTUDO" DESC
        LIMIT 10;
        '''
        cursor.execute(query3)
        print("\nConsulta 3: Top 10 Municípios com Alta Média de Anos de Estudo (25 anos ou mais)")
        for row in cursor.fetchall():
            print(f"Município: {row[0]} ({row[1]}), Média Anos Estudo: {row[2]}")

        # Consulta 4: Escolas Indígenas por Tipo de Dependência e Localização
        query4 = '''
        SELECT e."TIPO_DEPENDENCIA", e."TIPO_LOCALIZACAO", COUNT(*) as total_escolas
        FROM "Escola" e
        WHERE e."INDIGENA" = TRUE AND e."SITUACAO_FUNCIONAMENTO" = 'Ativa'
        GROUP BY e."TIPO_DEPENDENCIA", e."TIPO_LOCALIZACAO"
        ORDER BY total_escolas DESC;
        '''
        cursor.execute(query4)
        print("\nConsulta 4: Escolas Indígenas por Tipo de Dependência e Localização")
        for row in cursor.fetchall():
            print(f"Dependência: {row[0]}, Localização: {row[1]}, Total Escolas: {row[2]}")

        # Consulta 5: Territórios Indígenas com Alta População e Baixa Frequência Escolar
        query5 = '''
        SELECT t."NOME_TERRITORIO", uf."SIGLA_UF", t."POP_TOTAL", AVG(f."TAXA_FREQUENCIA") as media_frequencia
        FROM "Territorio_Indigena" t
        JOIN "Unidade_Federativa" uf ON t."ID_UF" = uf."ID_UF"
        JOIN "Municipio" m ON uf."ID_UF" = m."ID_UF"
        JOIN "Frequencia_Escolar" f ON m."ID_MUNICIPIO" = f."ID_MUNICIPIO"
        WHERE t."POP_TOTAL" > 1000 AND f."FAIXA_ETARIA" = '6 a 14 anos'
        GROUP BY t."NOME_TERRITORIO", uf."SIGLA_UF", t."POP_TOTAL"
        HAVING AVG(f."TAXA_FREQUENCIA") < 50
        ORDER BY media_frequencia ASC
        LIMIT 5;
        '''
        cursor.execute(query5)
        print("\nConsulta 5: Territórios Indígenas com Alta População e Baixa Frequência Escolar (6 a 14 anos)")
        for row in cursor.fetchall():
            print(f"Território: {row[0]} ({row[1]}), População: {row[2]}, Média Frequência: {row[3]:.2f}%")

        conn.commit()
        print("Consultas analíticas executadas com sucesso.")
    except Exception as e:
        print(f"Erro ao executar consultas analíticas: {e}")
        conn.rollback()


# Executar as funções
if __name__ == "__main__":
    arquivos_xlsx = [
        'frequencia_escolar.xlsx',
        'nivel_instrucao.xlsx',
        'media_anos.xlsx'
    ]

    try:
        criar_esquema()
        carregar_csv_censo()
        carregar_xlsx(arquivos_xlsx)
        executar_consultas_analiticas()
    finally:
        cursor.close()
        conn.close()
        print("Conexão fechada.")