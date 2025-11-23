### **Dicionário de Dados 📊**

Este dicionário de dados descreve o modelo multi-dimensional do Data Warehouse, baseado na base de dados transacional **AdventureWorks** com o seguinte diagrama.

![Imagem do diagrama do Data Warehouse feito no dbdiagram.io](/Assets/AdventureWorksDW-diagram.png "Diagrama do Data Warehouse")

#### **Tabela de Fatos: `factvendas` 💰**

> Esta tabela armazena todas as tuas transações de venda e as métricas principais. É o coração do teu DW.

| Coluna | Tipo de Dados | Descrição |
| :--- | :--- | :--- |
| `factvendas_id` | `INT` | 🔑 **Chave Primária**. ID único de cada transação de venda. |
| `tempo_id` | `INT` | 🔗 **Chave Estrangeira**. Liga à tabela `dimtempo`. |
| `cliente_id` | `INT` | 🔗 **Chave Estrangeira**. Liga à tabela `dimcliente`. |
| `produto_id` | `INT` | 🔗 **Chave Estrangeira**. Liga à tabela `dimproduto`. |
| `vendedor_id` | `INT` | 🔗 **Chave Estrangeira**. Liga à tabela `dimvendedor`. |
| `faturalocalizacao_id` | `INT` | 🔗 **Chave Estrangeira**. Liga a `dimlocalizacao` para a morada de fatura. |
| `entregalocalizacao_id` | `INT` | 🔗 **Chave Estrangeira**. Liga a `dimlocalizacao` para a morada de entrega. |
| `quantidadevendida` | `INT` | A quantidade de itens vendidos por transação. |
| `precounitario` | `NUMERIC(10, 2)` | Preço de um único item. |
| `valortotalvenda` | `NUMERIC(10, 2)` | O valor total da venda. |
| `desconto` | `NUMERIC(10, 2)` | O desconto aplicado, em percentagem. |
| `custototalproduto` | `NUMERIC(10, 2)` | O custo total do produto (calculado). |

---

#### **Tabelas de Dimensão 🧩**

> As tabelas de dimensão fornecem contexto para as métricas da tabela de fatos, respondendo a perguntas como *quem*, *o quê*, *onde* e *quando*.

##### **`dimtempo` 🗓️**

| Coluna | Tipo de Dados | Descrição |
| :--- | :--- | :--- |
| `tempo_id` | `INT` | 🔑 **Chave Primária**. ID da data no formato `AAAAMMDD`. |
| `data` | `DATE` | A data da transação. |
| `ano` | `INT` | O ano da transação. |
| `trimestre` | `INT` | O trimestre do ano. |
| `mes` | `INT` | O número do mês (1-12). |
| `nomemes` | `VARCHAR(20)` | Nome do mês (ex: 'Janeiro'). |
| `dia` | `INT` | O dia do mês. |
| `nomediasemana` | `VARCHAR(20)` | Nome do dia da semana (ex: 'Domingo'). |
| `fimsemana` | `BOOLEAN` | `TRUE` se for fim de semana. |

##### **`dimcliente` 🧑**

| Coluna | Tipo de Dados | Descrição |
| :--- | :--- | :--- |
| `cliente_id` | `INT` | 🔑 **Chave Primária**. ID único do cliente. |
| `nomecompleto` | `VARCHAR(255)` | Nome e apelido do cliente. |
| `email` | `VARCHAR(100)` | Endereço de email. |
| `telefone` | `VARCHAR(50)` | Número de telefone. |
| `tipopessoa` | `VARCHAR(50)` | Tipo de cliente ('IN' para Individual, 'SC' para Loja). |

##### **`dimproduto` 📦**

| Coluna | Tipo de Dados | Descrição |
| :--- | :--- | :--- |
| `produto_id` | `INT` | 🔑 **Chave Primária**. ID único do produto. |
| `nome` | `VARCHAR(255)` | O nome do produto. |
| `numero` | `VARCHAR(25)` | Número de referência. |
| `cor` | `VARCHAR(25)` | A cor do produto. |
| `categoria` | `VARCHAR(50)` | Categoria do produto (ex: 'Bikes'). |
| `subcategoria` | `VARCHAR(50)` | Subcategoria (ex: 'Road Bikes'). |

##### **`dimlocalizacao` 📍**

| Coluna | Tipo de Dados | Descrição |
| :--- | :--- | :--- |
| `localizacao_id` | `INT` | 🔑 **Chave Primária**. ID da localização. |
| `codigopaisregiao` | `VARCHAR(50)` | O código do país. |
| `provinciaestado` | `VARCHAR(50)` | O nome do estado/província. |
| `cidade` | `VARCHAR(50)` | O nome da cidade. |
| `codigopostal` | `VARCHAR(15)` | O código postal. |

##### **`dimvendedor` 🤝**

| Coluna | Tipo de Dados | Descrição |
| :--- | :--- | :--- |
| `vendedor_id` | `INT` | 🔑 **Chave Primária**. ID único do vendedor. |
| `nomecompleto` | `VARCHAR(255)` | Nome e apelido do vendedor. |
| `cargo` | `VARCHAR(100)` | Cargo do vendedor. |
| `genero` | `VARCHAR(10)` | Género do vendedor. |

---