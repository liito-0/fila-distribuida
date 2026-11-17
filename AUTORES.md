# Autores

Projeto **Fila Distribuída**: fila de tarefas com broker, workers e producers que se comunicam por sockets.

| Integrante | Parte | Responsabilidades |
|---|---|---|
| Miguel Monteiro | 1. Broker | `broker/`: recebe tarefas, mantém a fila, controla `in_progress`, timeout, reentrega e `MAX_ATTEMPTS` |
| Pedro Henrique | 2. Worker | `worker/`: pega a tarefa, executa e confirma (ACK/FAIL) |
| Evelin Paula | 3. Producer + protocolo | `producer/` e `common/protocol.py`: envio de tarefas e formato das mensagens |
| David Melo | 4. Arquitetura e docs | `docs/`, diagramas, descrição dos componentes e `README.md` |
| Heitor Santana | 5. Testes e integração | `tests/` e `scripts/`: testes unitários e de integração, script que sobe tudo e prova a comunicação |
