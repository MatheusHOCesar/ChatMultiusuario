import socket
import threading
import queue
from datetime import datetime

HOST = '127.0.0.1'
PORT = 5000

# Estrutura de dados em memória compartilhada (thread-safe nativo)
fila_mensagens = queue.Queue()

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
            
        except ConnectionResetError:
            print("\n[!] Conexão perdida com o cliente.")
            break

def thread_processamento(conn):
    """
    Thread 2: Varre a memória compartilhada e envia o output ao cliente (loop infinito).
    """
    while True:
        # O método get() bloqueia a thread até ter um item na fila, otimizando o uso de CPU
        msg = fila_mensagens.get() 
        
        # Para testar a via de volta, vamos enviar um echo simples (Regra da Fase 1)
        resposta = f"Você digitou: {msg}"
        
        try:
            conn.sendall(resposta.encode('utf-8'))
        except OSError:
            break
        
        # Sinaliza que a tarefa da fila foi concluída
        fila_mensagens.task_done()

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(1)
    
    print(f"Servidor escutando em {HOST}:{PORT}...")

    conn, addr = server_socket.accept()
    print(f"Conexão recebida de {addr}")

    # Envia a mensagem de confirmação (MSG1)
    agora = datetime.now().strftime("%H:%M")
    conn.sendall(f"<{agora}>: CONECTADO!!".encode('utf-8'))

    # Inicializa as threads passando o handler da conexão
    t1 = threading.Thread(target=thread_recepcao, args=(conn,), daemon=True)
    t2 = threading.Thread(target=thread_processamento, args=(conn,), daemon=True)
    
    t1.start()
    t2.start()

    # Mantém a thread principal do servidor rodando enquanto a Thread 1 estiver ativa
    t1.join()
    
    conn.close()
    server_socket.close()

if __name__ == "__main__":
    start_server()