**Desafio Técnico para Analista de Dados e Importação para o Solr**

---

**Parte 1: Desenvolvimento de Dashboard no Power BI**

Bem-vindo(a) ao teste prático para a posição de Analista de Dados. Neste teste, estaremos avaliando suas habilidades na criação de dashboards utilizando o Power BI. O foco será integrar e visualizar dados relacionados a alunos, matérias e notas.

**Objetivo do Teste:**
Construir um dashboard informativo que forneça insights sobre o desempenho acadêmico dos alunos, destacando características demográficas, médias de notas por semestre e outras informações relevantes.

**Conjunto de Dados:**
1. **Tabela de Alunos:**
   - ID
   - MATRICULA
   - Nome
   - Idade
   - Gênero
   - Bairro
   - Endereço
   - Data de Criação

2. **Tabela de Matérias:**
   - ID
   - Nome
   - Descrição
   - Professor
   - Data de Criação

3. **Tabela de Notas:**
   - ID
   - ALUNOID (Relacionado à tabela de Alunos)
   - MATERIAID (Relacionado à tabela de Matérias)
   - Nota
   - Data de Criação

**Requisitos do Dashboard:**

1. **Demografia dos Alunos:**
   - Gráfico de pizza ou barras que destaque a distribuição de alunos por gênero.
   - Gráfico de barras que exiba a distribuição de alunos por bairro.
   - Gráfico de dispersão ou barras empilhadas que mostre a distribuição de idade dos alunos.

2. **Desempenho Acadêmico:**
   - Gráfico de linhas ou barras que represente a média das notas dos alunos por semestre.
   - Tabela dinâmica que liste os alunos, suas médias de notas e outras informações relevantes.

3. **Detalhes sobre Matérias e Professores:**
   - Tabela ou gráfico que apresente as informações sobre as matérias, incluindo o professor responsável.

4. **Interatividade:**
   - Filtros interativos por semestre, gênero, bairro, entre outros, para permitir uma análise mais personalizada.

**Instruções Adicionais:**
- Utilize as funcionalidades avançadas do Power BI, como medidas calculadas, relações entre tabelas e hierarquias para aprimorar a interatividade.
- Certifique-se de que o design do dashboard seja limpo, intuitivo e capaz de contar uma história visual sobre os dados apresentados.
- Execute o arquivo docker-compose.yml para criar as bases de dados já com os dados necessários para a criação do dashboard.
- Pedimos que você conclua o teste dentro de 8 dias após a data de envio. Se precisar de alguma informação adicional ou esclarecimento, sinta-se à vontade para entrar em contato.

**Parte 2: Importação de Dados para o Solr**

**Descrição:**

Seu objetivo é criar um script em Python que realiza as seguintes tarefas:

1. **Formatar o CSV:**
   - O script deve ler um arquivo CSV fornecido e realizar a formatação dos dados para garantir consistência e correção.
   - Considere que o arquivo CSV pode conter campos mal formatados, dados ausentes ou outros problemas típicos em conjuntos de dados do mundo real.

2. **Inserir no Solr:**
   - Após a formatação, o script deve inserir os dados no Apache Solr. 
   - Certifique-se de mapear corretamente os campos do CSV para os campos correspondentes no esquema do Solr.

**Requisitos Técnicos:**

- Use a biblioteca `pandas` para manipulação de dados CSV em Python.
- Utilize a biblioteca `pysolr` para interagir com o Solr a partir do script Python.
- Hospede seu projeto no GitHub, fornecendo instruções claras sobre como configurar e executar o script.

**Configuração do Solr em Docker:**

Execute o seguinte comando para criar uma instância do Solr no Docker:

```bash
docker run -d -p 8983:8983 --name solr_instance -t solr
```

Acesse o Solr Admin Interface em http://localhost:8983/solr.

**Arquivo CSV de Exemplo (Alunos de uma Escola Primária):**

Utilize o arquivo [alunos.csv](https://raw.githubusercontent.com/joaomarceloubc/desafio-engenheiro-dados2/main/aluno.csv) com os dados fictícios representando alunos de uma escola primária. 

**Pontos Extras:**

- Lidar com situações de erro durante a formatação e inserção no Solr.
- Implementar logs adequados para rastrear o progresso e eventuais problemas.
- Garantir que o script seja eficiente, mesmo para grandes conjuntos de dados.

**Instruções:**

1. Baixe o arquivo CSV de exemplo contendo dados fictícios de alunos de uma escola primária.
2. Desenvolva o script em Python.
3. Documente claramente como o script deve ser executado, incluindo dependências e configurações.
4. Hospede o projeto no GitHub e forneça o link do repositório.

**Avaliação:**

Você será avaliado com base na eficácia do script, na capacidade de lidar com situações não ideais nos dados e na integração bem-sucedida com o Solr. A utilização do GitHub para hospedar o projeto será considerada na avaliação global.

**Get Started:**

1. Dashboard.pbix é o arquivo relativo ao dashboard solicitado.
3. Instale a biblioteca pysolr
4. Para rodar o script, antes é necessário subir o container docker relativo ao solr.

```bash
docker run -d -p 8983:8983 --name solr_instance -t solr
```

5. Entre no terminal bash do solr e crie um 'core'. Ele será necessário para exportar os dados para o solr.

```bash
docker exec -it solr_instance bash
```

```bash
cd server/solr
```

```bash
solr create_core -c alunos
```
6. Rode o script 

```bash
python script.py
```

