# Scoring Modelinho Olist

## Pré-requisitos
Esse projeto foi criado usando Python 3.8.5\
Ele lê os modelos de diretório models\
Ele lê os dados em um arquivo data/olist_dsa.db \
As bibliotecas necessárias e suas versões estão listadas no arquivo requirements.txt

## Run
Para executa-lo basta rodar o script main.py\
Ele vai listar os modelos do diretório de modelos e importar o último modelo em ordem alfabética, é espera que tenha a data no nome do arquivo para mostrar o mais recente. Depois o script vai ler as tabelas da base de dados e criar a tabela análitica (ABT). Por fim a ABT é passada para o método predict do modelo gerando a predição final