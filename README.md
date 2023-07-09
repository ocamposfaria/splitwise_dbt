# Transformação de dados do Splitwise com dbt

O Splitwise é um popular aplicativo para compartilhamento de gastos financeiros entre pessoas. Este projeto contém modelos de dados e transformações implementados usando o dbt (Data Build Tool), para análise de dados capturados com a API do Splitwise, carregados num PostgreSQL.

## Visão Geral

O objetivo deste projeto é fornecer um pipeline de dados confiável e escalável para análise de gastos. Ele utiliza o dbt para transformar dados brutos em um formato estruturado, pronto para análise. Abaixo, uma preview da lineage do pipeline:

![alt text for screen readers](/dbt-dag.png "Text to show on mouseover")

## Pré-requisitos

Para executar este projeto, verifique se você possui os seguintes itens instalados:

- dbt (Data Build Tool): [Guia de instalação](https://docs.getdbt.com/dbt-cli/installation/)
- Conexão com o banco de dados: Configure as informações de conexão com o banco de dados no arquivo `profiles.yml`.

## Estrutura do Projeto
A estrutura do projeto segue as convenções padrão do dbt:

- /logs: Contém os logs gerados nas execuções do modelo.
- /splitwise/models: Contém os modelos dbt implementados em arquivos SQL. Cada modelo representa uma transformação ou agregação de dados específica.
- /splitwise/macros: Inclui macros personalizados usados no projeto. Macros são trechos de código reutilizáveis que melhoram as capacidades do dbt (vazio para este projeto).
- /splitwise/data: Opcionalmente, você pode incluir arquivos de dados de exemplo ou scripts SQL usados para testes ou desenvolvimento (vazio para este projeto).
- /splitwise/tests: Contém os testes escritos para validar a corretude dos modelos dbt.
- /splitwise/analysis: Opcionalmente, você pode incluir notebooks Jupyter ou outros arquivos de análise usados para explorar os dados transformados.

## Contribuindo
Se encontrar algum problema ou tiver sugestões de melhorias, abra uma issue.