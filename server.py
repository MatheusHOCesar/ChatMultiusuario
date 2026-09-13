import socket
import threading
import queue
import time
from datetime import datetime
import sys

HOST = '127.0.0.1'
PORT = 5000

clientes_ativos = []

# Estrutura de dados em memória compartilhada (thread-safe nativo)
fila_mensagens = queue.Queue()

# Lista global que vai guardar as conexões ativas para podermos fazer o broadcast e contar as vagas depois
clientes_ativos = []

def thread_recepcao(conn):
    """
    Thread 1: Lê do socket e salva na memória compartilhada (loop infinito).
    """
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                print("\n[!] Cliente desconectou.")
                break
            
            # Decodifica e armazena na memória compartilhada
            msg = data.decode('utf-8')
            fila_mensagens.put(msg)

            #Se for o comando de saída, interrompe a leitura do socket antes que o cliente feche a conexão do outro lado e cause erro
            if msg.strip() == ':quit':
                print("\n[!] Cliente solicitou desconexão.")
                break
            
            
        except ConnectionResetError:
            print("\n[!] Conexão perdida com o cliente.")
            break

def thread_processamento(conn, addr):
    """
    Thread 2: Varre a memória compartilhada, processa comandos e envia o output ao cliente.
    """
    # Define o nome padrão conforme especificação (IP:porta)
    nome_usuario = f"{addr[0]}:{addr[1]}"
    
    ultimo_envio_tempo = time.time()
    
    while True:
        agora_ts = time.time()
        
        # Envio automático do horário a cada 1 minuto
        if agora_ts - ultimo_envio_tempo >= 60:
            data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            msg_tempo = f"\n[SERVER TIME] {data_hora}"
            try:
                conn.sendall(msg_tempo.encode('utf-8'))
            except OSError:
                break
            ultimo_envio_tempo = agora_ts
        
        try:
            msg = fila_mensagens.get(timeout=1) 
            
            # Lógica de comandos vs mensagens
            if msg.startswith(':'):
                # Processamento do comando :nome
                if msg.startswith(':nome '):
                    # Quebra a string no primeiro espaço e pega a segunda parte
                    nome_usuario = msg.split(' ', 1)[1].strip()
                    
                    #Envia a confirmação de volta para o cliente
                    confirmacao = f"\n[SERVER] Nome atualizado para: {nome_usuario}"
                    conn.sendall(confirmacao.encode('utf-8'))
                    print(f"[!] Cliente {addr} atualizou o nome para: {nome_usuario}")

                #Intercepta o :quit e finaliza a conexão    
                elif msg.strip() == ':quit':
                    despedida = "\n[SERVER] Encerrando conexão."
                    try:
                        conn.sendall(despedida.encode('utf-8'))
                    except OSError:
                        pass
                    
                    # O break mata a Thread 2 de forma limpa
                    break    
            else:
                # É uma mensagem normal. 
                # Pela especificação da Fase 1, o remetente recebe o eco.
                eco = f"Voce digitou: {msg}"
                conn.sendall(eco.encode('utf-8'))
                
            
            fila_mensagens.task_done()
            
        except queue.Empty:
            continue
        except OSError:
            break

def working_thread(conn, addr, limite_clientes):
    """
    Thread de trabalho instanciada pelo accept() para cada nova conexão.
    """
    # 1. Trava de lotação
    if len(clientes_ativos) >= limite_clientes:
        msg_erro = "\n[!] Limite de usuários excedido. Tente novamente mais tarde."
        try:
            conn.sendall(msg_erro.encode('utf-8'))
        except OSError:
            pass
        
        conn.close()
        print(f"[!] Conexão de {addr} recusada (lotação atingida).")
        return

    # 2. Alocação do cliente
    clientes_ativos.append(conn)
    print(f"[+] Cliente {addr} conectado. Lotação: {len(clientes_ativos)}/{limite_clientes}")

    # 3. Envio da confirmação inicial
    agora = datetime.now().strftime("%H:%M")
    try:
        conn.sendall(f"<{agora}>: CONECTADO!!".encode('utf-8'))
    except OSError:
        clientes_ativos.remove(conn)
        conn.close()
        return

    # 4. Fila exclusiva para evitar corrida de threads
    fila_cliente = queue.Queue()

    # 5. Inicia as sub-threads deste cliente
    t1 = threading.Thread(target=thread_recepcao, args=(conn, fila_cliente), daemon=True)
    t2 = threading.Thread(target=thread_processamento, args=(conn, addr, fila_cliente), daemon=True)
    
    t1.start()
    t2.start()

    # Segura a thread viva até o cliente dar o :quit
    t1.join()

    # 6. Desalocação
    if conn in clientes_ativos:
        clientes_ativos.remove(conn)
    conn.close()
    
    print(f"[-] Cliente {addr} desconectou. Vaga liberada. Lotação: {len(clientes_ativos)}/{limite_clientes}")

def start_server():
    # Validação do argumento de linha de comando para o limite de clientes
    if len(sys.argv) < 2:
        print("[!] Uso correto: python server.py <limite_de_clientes>")
        sys.exit(1)

    try:
        limite_clientes = int(sys.argv[1])
    except ValueError:
        print("[!] O limite de clientes deve ser um número inteiro.")
        sys.exit(1)

        
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen()

    print(f"Servidor escutando em {HOST}:{PORT} | Lotação máxima: {limite_clientes} usuários...")

    # Loop do acept()
    while True:
        try:
            conn, addr = server_socket.accept()
            print(f"[!] Tenativa de conexão de um novo cliente de {addr}")

            t_work = threading.Thread(target=working_thread, args=(conn, addr, limite_clientes), daemon=True)
            t_work.start()

        except KeyboardInterrupt:
            print("\n[!] Servidor encerrando...")
            break

    
    server_socket.close()

if __name__ == "__main__":
    start_server()

