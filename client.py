import socket
import threading
import sys
import time
import os

HOST = '127.0.0.1'
PORT = 5000

def thread_escuta(client_socket):
    """
    Thread 2: Lê do socket e imprime na tela em loop infinito.
    """
    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                # Se data for vazio, o servidor caiu ou encerrou a conexão
                print("\n[!] Conexão encerrada pelo servidor.")

                # Derruba o processo inteiro na hora, matando o input() travado
                os._exit(0)  
                
            
            # Imprime a mensagem recebida formatada
            print(f"\n{data.decode('utf-8')}")
        except OSError:
            # Erro de IO padrão ao fechar o socket forçadamente
            os._exit(0)

def thread_envio(client_socket):
    """
    Thread 1: Lê do teclado e envia pela rede em loop infinito.
    """
    while True:
        try:
            # Aguarda o input do usuário
            msg = input()
            
            # Envia a mensagem codificada em bytes
            client_socket.sendall(msg.encode('utf-8'))
            
            # Regra de negócio: se o comando for :quit, quebra o loop e inicia o encerramento
            if msg.strip() == ':quit':
                print("[!] Desconectando...")
                #Dá um tempo de 500ms para a Thread 2 printar o adeus do server
                time.sleep(0.5)
                break
        except EOFError:
            break

def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        client_socket.connect((HOST, PORT))
        
        # Recebe a mensagem MSG1 inicial de conexão do servidor
        msg_inicial = client_socket.recv(1024).decode('utf-8')
        print(msg_inicial)
        
        # Instancia e inicia a Thread 2 (Escuta)
        # O argumento daemon=True faz com que essa thread morra se o programa principal fechar
        t2 = threading.Thread(target=thread_escuta, args=(client_socket,), daemon=True)
        t2.start()
        
        # A Thread 1 (Envio) pode rodar no fluxo principal de execução, 
        # bloqueando o fim do programa até que o usuário digite :quit
        thread_envio(client_socket)
        
    except ConnectionRefusedError:
        print("[!] Servidor offline. Inicie o server.py primeiro.")
    finally:
        # Fechamento gracioso do socket
        client_socket.close()
        sys.exit(0)

if __name__ == "__main__":
    start_client()

