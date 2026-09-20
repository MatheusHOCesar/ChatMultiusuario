import socket
import threading
import queue
import time
from datetime import datetime
import sys

HOST = '127.0.0.1'
PORT = 5000

# Lista global única para gerenciar os sockets dos usuários conectados
clientes_ativos = []

def thread_recepcao(conn, fila_cliente):
    """
    Lê do socket e salva na fila. Blindado contra quedas abruptas do cliente.
    """
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                print("\n[!] Cliente desconectou graciosamente.")
                break
            
            msg = data.decode('utf-8')
            fila_cliente.put(msg)
            
            if msg.strip() == ':quit':
                print("\n[!] Comando :quit recebido. Encerrando recepção.")
                break
                
        except (ConnectionResetError, ConnectionAbortedError, OSError):
            print("\n[!] Conexão perdida inesperadamente com um cliente.")
            break

def thread_processamento(conn, addr, fila_cliente):
    """
    Processa a fila e faz o broadcast blindado contra sockets mortos no array.
    """
    nome_usuario = f"{addr[0]}:{addr[1]}"
    ultimo_envio_tempo = time.time()
    
    while True:
        agora_ts = time.time()
        
        if agora_ts - ultimo_envio_tempo >= 60:
            data_hora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            msg_tempo = f"\n[SERVER TIME] {data_hora}"
            try:
                conn.sendall(msg_tempo.encode('utf-8'))
            except OSError:
                break 
            ultimo_envio_tempo = agora_ts
        
        try:
            msg = fila_cliente.get(timeout=1) 
            
            if msg.startswith(':'):
                if msg.startswith(':nome '):
                    nome_usuario = msg.split(' ', 1)[1].strip()
                    confirmacao = f"\n[SERVER] Nome atualizado para: {nome_usuario}"
                    try:
                        conn.sendall(confirmacao.encode('utf-8'))
                    except OSError:
                        break
                    
                elif msg.strip() == ':quit':
                    despedida = "\n[SERVER] Encerrando conexão. Falou!"
                    try: conn.sendall(despedida.encode('utf-8'))
                    except OSError: pass
                    break 
            else:
                hora_atual = datetime.now().strftime("%H:%M")
                msg_publica = f"{nome_usuario} ({hora_atual}): {msg}"
                eco = f"Voce digitou: {msg}"
                
                try: 
                    conn.sendall(eco.encode('utf-8'))
                except OSError: 
                    break 
                
                for cliente_socket in clientes_ativos:
                    if cliente_socket != conn:
                        try: 
                            cliente_socket.sendall(f"\n{msg_publica}".encode('utf-8'))
                        except (OSError, BrokenPipeError): 
                            pass 
            
            fila_cliente.task_done()
            
        except queue.Empty:
            continue
        except OSError:
            break

def working_thread(conn, addr, limite_clientes):
    """
    Thread instanciada pelo accept(). Valida o limite e gerencia o ciclo de vida.
    """
    if len(clientes_ativos) >= limite_clientes:
        msg_erro = "\n[!] Limite de usuários excedido. Tente novamente mais tarde."
        try:
            conn.sendall(msg_erro.encode('utf-8'))
        except OSError:
            pass
        conn.close()
        print(f"[!] Conexão de {addr} recusada (lotação atingida).")
        return

    clientes_ativos.append(conn)
    print(f"[+] Cliente {addr} conectado. Lotação: {len(clientes_ativos)}/{limite_clientes}")

    agora = datetime.now().strftime("%H:%M")
    try:
        conn.sendall(f"<{agora}>: CONECTADO!!".encode('utf-8'))
    except OSError:
        clientes_ativos.remove(conn)
        conn.close()
        return

    # Fila isolada garantindo que os clientes não roubem mensagens uns dos outros
    fila_cliente = queue.Queue()
    
    t1 = threading.Thread(target=thread_recepcao, args=(conn, fila_cliente), daemon=True)
    t2 = threading.Thread(target=thread_processamento, args=(conn, addr, fila_cliente), daemon=True)
    
    t1.start()
    t2.start()

    t1.join()

    if conn in clientes_ativos:
        clientes_ativos.remove(conn)
    conn.close()
    print(f"[-] Cliente {addr} desconectou. Vaga liberada. Lotação: {len(clientes_ativos)}/{limite_clientes}")

def start_server():
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

    while True:
        try:
            conn, addr = server_socket.accept()
            print(f"[!] Tentativa de conexão de um novo cliente de {addr}")

            t_work = threading.Thread(target=working_thread, args=(conn, addr, limite_clientes), daemon=True)
            t_work.start()

        except KeyboardInterrupt:
            print("\n[!] Servidor encerrando...")
            break
            
    server_socket.close()

if __name__ == "__main__":
    start_server()