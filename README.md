# Fila Distribuída de Tarefas

Sistema acadêmico desenvolvido para a disciplina de **Sistemas Distribuídos**. Ele separa a submissão, a distribuição e a execução de tarefas usando uma arquitetura assíncrona baseada em TCP.

O grupo é formado por David Melo, Evelin Dionísio, Heitor Carvalho, Miguel Monteiro e Pedro Henrique. Veja as responsabilidades em [AUTORES.md](AUTORES.md).

## Objetivo

Permitir que Producers submetam tarefas a um Broker, que as organiza em uma fila e as entrega a Workers disponíveis. Os Workers executam o payload, confirmam o sucesso com `ACK` ou reportam uma falha com `FAIL`; o Producer pode consultar o estado e aguardar o resultado.

A implementação funciona em uma ou mais máquinas da rede local. Neste repositório, os componentes usam `127.0.0.1:5000` por padrão.

## Arquitetura

```text
Producer ──SUBMIT/STATUS/RESULT──> Broker <──REQUEST_TASK/ACK/FAIL── Worker
                                      |
                                      └── fila e estados em memória
```

- **Broker**: recebe conexões TCP assíncronas, mantém a fila e controla o ciclo de vida das tarefas.
- **Producer**: envia tarefas e consulta estado ou resultado. Também pode aguardar até que uma tarefa termine ou falhe.
- **Worker**: solicita tarefas periodicamente, executa o payload e devolve `ACK` ou `FAIL`.
- **Protocolo comum**: usa JSON em UTF-8, com uma mensagem por linha (`\n`), em `common/protocol.py`.

**Diagramas existentes:** [componentes](docs/diagrama_componentes.png) · [comunicação](docs/diagrama_comunicacao.png).

## Estrutura das camadas

```text
fila-distribuida/
├── common/       # Protocolo, host, porta e codificação JSON
├── broker/       # Fila, distribuição, estados, timeout e reentrega
├── worker/       # Execução das tarefas e reconexão ao Broker
├── producer/     # Cliente assíncrono e interface de linha de comando
├── scripts/      # Demonstração completa e teste manual
├── tests/        # Testes unitários e de integração com pytest
├── docs/         # Diagramas da arquitetura
├── requirements.txt
└── README.md
```

## Estados de falha

- `pending`: aguardando atribuição.
- `in_progress`: reservada para um Worker.
- `done`: concluída; contém o resultado enviado pelo `ACK`.
- `failed`: entrou em falha definitiva após esgotar as tentativas.
- `unknown`: ID inexistente ou não conhecido pelo Broker.

### Recuperação e reentrega

- **ACK válido**: remove a tarefa de `in_progress` e salva o resultado em `done`.
- **FAIL válido**: nas primeiras entregas, devolve a tarefa ao fim da fila. Ao atingir três entregas sem sucesso, marca `failed`.
- **Timeout**: a tarefa sem `ACK`/`FAIL` por mais de 30 segundos volta ao início da fila. O monitor verifica expirados a cada 5 segundos.
- **Desconexão do Worker**: tarefas entregues pela conexão são devolvidas ao início da fila.
- **Máximo de tentativas**: `MAX_ATTEMPTS = 3`.
- **ACK/FAIL atrasado**: sem `worker_id`, o ACK/FAIL antigo é ignorado; com `worker_id`, o Broker rejeita a resposta com `ERROR` se o Worker não for o responsável.
- **Reconexão**: Workers tentam reconectar ao Broker automaticamente após falhas de comunicação.

## Instalação

### Pré-requisitos

- Python 3.10 ou superior.
- Pip.
- Acesso à porta TCP configurada (5000 por padrão).

### Windows

```powershell
git clone https://github.com/liito-0/fila-distribuida.git
cd fila-distribuida
py -m venv .venv
.\.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
```

### Linux

```bash
git clone https://github.com/liito-0/fila-distribuida.git
cd fila-distribuida
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## Execução

Execute os componentes em terminais separados. Deixe o Broker ativo antes dos Producers e Workers.

### Broker

Windows:

```powershell
py -m broker.broker
```

Linux:

```bash
python -m broker.broker
```

O Broker usa `127.0.0.1:5000` por padrão.

### Worker

```bash
python -m worker.worker --id worker-1
python -m worker.worker --id worker-2
```

No Windows, substitua `python` por `py` quando necessário. Opções: `--host`, `--port`, `--poll-interval`, `--max-tasks` e `--debug`.

### Producer

```bash
python -m producer.producer --id producer-1 --payload "ola"
python -m producer.producer --action uppercase --text "ola mundo" --count 3 --interval 1
python -m producer.producer --action reverse --text "worker" --count 2 --wait
python -m producer.producer --action sum --numbers 1 2 3
```

Opções disponíveis: `--host`, `--port`, `--payload`, `--action`, `--text`, `--numbers`, `--duration`, `--count`, `--interval`, `--wait` e `--debug`.

Pela API Python, `submit()`, `submit_many()`, `status()`, `get_result()` e `wait_result()` estão disponíveis em `Producer`. `wait_result(task_id, timeout=30, poll=0.5)` lança `ProducerError` em falha definitiva ou timeout.

### Demonstração completa

Com a porta 5000 disponível:

```bash
python -m scripts.demo
```

No Windows, use `py -m scripts.demo` se necessário. O script inicia um Broker e dois Workers, envia cinco tarefas, aguarda os resultados e encerra os subprocessos.

## Comandos principais

| Finalidade | Comando |
|---|---|
| Criar ambiente virtual, Windows | `py -m venv .venv` |
| Criar ambiente virtual, Linux | `python3 -m venv .venv` |
| Instalar dependências | `python -m pip install -r requirements.txt` |
| Iniciar Broker | `python -m broker.broker` |
| Iniciar Worker | `python -m worker.worker --id worker-1` |
| Enviar tarefa | `python -m producer.producer --payload "ola" --wait` |
| Executar demonstração | `python -m scripts.demo` |
| Executar teste manual | `python -m scripts.teste_manual` |
| Executar testes pytest | `python -m pytest tests/ -v` |
## Testes

Os testes estão escritos com `pytest` e cobrem o Broker, o Producer, o Worker e fluxos de integração. A execução com `py -m pytest tests/ -q` resultou em **42 testes aprovados**.

Para executar a suíte real:

```bash
python -m pytest tests/ -v
```

## Limitações conhecidas

- Não há persistência ou replicação do estado do Broker.
- Reiniciar o Broker apaga fila, tarefas em execução, resultados e contadores.
- Não há autenticação, criptografia/TLS, autorização ou isolamento por usuário.
- Não há balanceamento além da distribuição simples por solicitação do Worker.
- O protocolo não possui controle de versão e não faz checksum das mensagens.
- Não há confirmação persistente de entrega; ACK/FAIL depende da conexão ativa.
- `pending` é uma `deque` FIFO, mas timeout traz uma tarefa ao início e FAIL a traz ao fim.
- O Broker não expõe opção de porta pela CLI; neste código `PORT = 5000` é constante.
- O limite de tamanho do payload e o dimensionamento da fila não são configurados.
- Não há supervisão externa dos processos no modo manual.
- Os dados são mantidos em memória local; não há compartilhamento de estado entre vários processos Broker.
