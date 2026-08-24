import socket
from datetime import datetime

HOST = '127.0.0.1' # Endereco IP do Servidor para teste
PORT = 5000 # Porta que o Servidor esta

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #Cria o socket IPv4 e TCP

    server_socket.bind((HOST, PORT))
    #Associa o socket ao endereço e porta

    server_socket.listen(1)
    print(f"Servidor iniciado em {HOST}:{PORT}. Aguardando conexões...")
    #Escuta conexões. O parâmetro 1 indica o número máximo de conexões pendentes

    conn, addr = server_socket.accept()
    print(f"Conexão estabelecida com {addr}")
    #Bloqueia e execução até que o cliente se conecte.

    
    msg1 = f"Olá, cliente! A conexão foi estabelecida com sucesso."
    conn.sendall(msg1.encode('utf-8'))
    #Coleta a hora atual, formata a mensagem e envia para o cliente

    conn.close()
    server_socket.close()
    #Fecha a conexão e o socket apenas para teste 

if __name__ == "__main__":
    start_server()