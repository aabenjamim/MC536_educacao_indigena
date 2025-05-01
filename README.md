Projeto da disciplina de Banco de Dados(MC563) da Unicamp
Alunos: Johatan dos Reis Lima 250502,  Alane Benjamim dos Santos
ODS 4: Educação
Tema Educação Indígena

TUTORIAL: Para usar o arquivo, primeiro descomprima o arquivo dos microdados do Censo de 2023(Como o arquivo é muito grande para colocar no github, tivemos que comprimir-lo). Depois mude os parâmetros do arquivo educacao_indigena.py:
conn = psycopg2.connect(
    dbname="",
    user="",
    password="",
    host="",
    port=""
)
para seus dados. Assim, já é possível rodar o arquivo educacao_indigena.py
