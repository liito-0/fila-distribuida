# Fila Distribuída de Tarefas

Projeto acadêmico desenvolvido para a disciplina de **Sistemas Distribuídos**, com o objetivo de implementar um sistema de gerenciamento e processamento distribuído de tarefas por meio de uma fila.

O grupo é composto por:
* David Melo
* Evelin Dionísio
* Heitor Carvalho
* Miguel Monteiro
* Pedro Henrique

## 1. Visão geral

O sistema separa a submissão de tarefas de sua execução. O Producer envia tarefas, o Broker distribui e acompanha seu estado, e um ou mais Workers processam as tarefas e enviam o resultado.

Os componentes comunicam-se por TCP, com mensagens JSON delimitadas por linha. O protocolo compartilhado está definido em `common/protocol.py`.

**Diagramas existentes:** [componentes](docs/diagrama_componentes.png) · [comunicação](docs/diagrama_comunicacao.png).

## 2. Organização do repositório

```text
fila-distribuida/
├── broker/       # Recebe mensagens, distribui tarefas e gerencia estados
├── common/       # Constantes e serialização do protocolo compartilhado
├── docs/         # Documentação e diagramas
├── producer/     # Cliente e CLI para envio e consulta de tarefas
├── scripts/      # Demonstração e testes manuais
├── tests/        # Testes automatizados
├── worker/       # Consumo e processamento de tarefas
├── requirements.txt
└── README.md
```

## 3. Protocolo de mensagens

| Tipo | Direção | Campos principais / finalidade |
|---|---|---|
| `SUBMIT` | Producer → Broker | `task` com `payload` e opcionalmente `id`; submete tarefa |
| `REQUEST_TASK` | Worker → Broker | `worker_id`; solicita tarefa |
| `ACK` | Worker → Broker | `task_id`, `result`; confirma conclusão |
| `FAIL` | Worker → Broker | `task_id`, `error`; informa falha de execução |
| `STATUS` | Producer → Broker | Consulta contagem de tarefas por estado |
| `RESULT` | Producer → Broker | `task_id`; consulta estado e resultado/erro quando disponível |
| `OK` | Broker → Producer/Worker | Confirma uma mensagem ou submissão (a submissão também inclui `task_id`) |
| `TASK` | Broker → Worker | Inclui a tarefa entregue |
| `NO_TASK` | Broker → Worker | Não há tarefa pendente no momento |
| `ERROR` | Broker → Producer/Worker | Inclui a descrição do erro |
| `STATUS` | Broker → Producer | `pending`, `in_progress`, `done` e `failed` com suas quantidades |
| `RESULT` | Broker → Producer | `task_id`, estado (`pending`, `in_progress`, `done`, `failed` ou `unknown`) e `result` quando concluída ou falha |

As respostas para `STATUS` e `RESULT` usam seus próprios nomes como `type`. Tarefas fora de `in_progress` que recebem ACK/FAIL são ignoradas.

## 4. Estados e tolerância a falhas

O Broker mantém os estados `pending`, `in_progress`, `done` e `failed`, além do número de entregas em `attempts`. O timeout de execução é de **30 segundos**; uma tarefa expirada volta ao início da fila. Uma falha reportada por `FAIL` volta ao fim da fila. Após **3 entregas** sem sucesso, a tarefa não é reenfileirada e passa a `failed`.

Se um Worker desconectar com uma tarefa ainda em execução, o Broker a devolve ao início da fila, respeitando o limite de tentativas. Os Workers tentam reconectar automaticamente ao Broker após falhas de comunicação.

O estado é mantido somente em memória pelo processo Broker: ao reiniciar o Broker, fila, resultados e contadores são perdidos. Não há persistência em disco.

## 5. Execução

### Pré-requisitos

- Python 3.10 ou superior.
- Pip.

### Instalação

```bash
git clone https://github.com/liito-0/fila-distribuida.git
cd fila-distribuida
python -m venv .venv
```

Ative o ambiente virtual no Windows:

```powershell
.\.venv\Scripts\Activate.ps1
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

### Inicialização do Broker

```bash
python -m broker.broker
```

### Inicialização dos Workers

Em outros terminais:

```bash
python -m worker.worker --id worker-1
python -m worker.worker --id worker-2
```

Os Workers conectam-se ao Broker e reconectam automaticamente se a conexão cair.

### Envio e consulta com o Producer

Envie um payload simples:

```bash
python -m producer.producer --id producer-1 --payload "ola"
```

Envie três tarefas de texto em intervalos de um segundo:

```bash
python -m producer.producer --action uppercase --text "ola mundo" --count 3 --interval 1
```

Some uma lista de inteiros:

```bash
python -m producer.producer --action sum --numbers 1 2 3
```

Use `--wait` para aguardar cada tarefa enviada e imprimir seu resultado:

```bash
python -m producer.producer --action reverse --text "worker" --count 2 --wait
```

O Producer também fornece as consultas `status()` e `get_result(task_id)` pela API Python; `wait_result(task_id, timeout=30, poll=0.5)` devolve o resultado concluído e lança `ProducerError` em caso de falha definitiva ou timeout.

### Demonstração completa

Com a porta padrão do Broker (5000) disponível, execute:

```bash
python -m scripts.demo
```

O script inicia um Broker e dois Workers como subprocessos usando o Python ativo, envia cinco tarefas de exemplo, imprime os resultados e encerra os processos ao terminar ou ao receber Ctrl+C.

### Testes manuais e automatizados

```bash
python -m scripts.teste_manual
python -m pytest tests/ -q
```
